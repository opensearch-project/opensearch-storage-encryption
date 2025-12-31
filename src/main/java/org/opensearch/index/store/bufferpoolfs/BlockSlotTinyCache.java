/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.file.Path;
import java.util.concurrent.locks.LockSupport;

import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;

/**
 * Tiny L1 cache in front of the main Caffeine L2 cache.
 *
 * Correctness under segment recycling:
 *  - Tier-2 slot uses a single volatile "stamp" to publish {blockIdx, val, generation} consistently.
 *  - Tier-3 uses pin-then-validate generation to close the TOCTOU window:
 *
 *    expectedGen = v.getGeneration()
 *    if (v.tryPin()) {
 *      if (v.getGeneration() == expectedGen) return v;
 *      v.unpin(); // pinned a recycled object; treat as miss
 *    }
 *
 * Volatile stamp gate (release/acquire) avoids reading half-updated slot fields:
 *
 *   Writer (fill slot):
 *     slot.blockIdx = X;
 *     slot.val      = V;
 *     slot.generation = G;
 *     stamp = pack(hash(X), G)   // RELEASE store, written last
 *
 *   Reader (check slot):
 *     s = stamp                 // ACQUIRE load, read first
 *     if (matches) then read slot.blockIdx/val safely
 */
public class BlockSlotTinyCache {

    public static final class CacheHitHolder {
        private boolean wasCacheHit;

        public void reset() {
            wasCacheHit = false;
        }

        public boolean wasCacheHit() {
            return wasCacheHit;
        }

        void setWasCacheHit(boolean hit) {
            this.wasCacheHit = hit;
        }
    }

    private static final int SLOT_COUNT = 32;
    private static final int SLOT_MASK = SLOT_COUNT - 1;

    private static final class Slot {
        @SuppressWarnings("unused")
        long p1, p2, p3, p4, p5, p6, p7;

        // Published under the stamp gate
        long blockIdx = -1;
        BlockCacheValue<RefCountedMemorySegment> val;
        int generation;

        /**
         * Single publication gate:
         *   upper 32 bits: generation
         *   lower 32 bits: blockIdx hash
         *
         * Written LAST with release semantics. Read FIRST with acquire semantics.
         * A stamp==0 means "empty".
         */
        volatile long stamp;

        @SuppressWarnings("unused")
        long q1, q2, q3, q4, q5, q6, q7;
    }

    // VarHandle for release/acquire stamp operations
    private static final VarHandle STAMP;
    static {
        try {
            STAMP = MethodHandles.lookup().findVarHandle(Slot.class, "stamp", long.class);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    // Thread-local MRU (keep for now; can be removed later if you want)
    private static final class LastAccessed {
        long blockIdx = -1;
        BlockCacheValue<RefCountedMemorySegment> val;
        int generation;
    }

    private final BlockCache<RefCountedMemorySegment> cache;
    private final Path path;

    private final Slot[] slots;
    private final FileBlockCacheKey[] slotKeys;

    private final ThreadLocal<LastAccessed> lastAccessed = ThreadLocal.withInitial(LastAccessed::new);

    public BlockSlotTinyCache(BlockCache<RefCountedMemorySegment> cache, Path path, long fileLength) {
        this.cache = cache;
        this.path = path;

        this.slots = new Slot[SLOT_COUNT];
        for (int i = 0; i < SLOT_COUNT; i++) {
            slots[i] = new Slot();
        }
        this.slotKeys = new FileBlockCacheKey[SLOT_COUNT];
    }

    public BlockCacheValue<RefCountedMemorySegment> acquireRefCountedValue(long blockOff) throws IOException {
        return acquireRefCountedValue(blockOff, null);
    }

    public BlockCacheValue<RefCountedMemorySegment> acquireRefCountedValue(long blockOff, CacheHitHolder hitHolder) throws IOException {
        final long blockIdx = blockOff >>> CACHE_BLOCK_SIZE_POWER;

        // -------------------------
        // TIER 1: Thread-local MRU
        // -------------------------
        LastAccessed last = lastAccessed.get();
        if (last.blockIdx == blockIdx) {
            BlockCacheValue<RefCountedMemorySegment> v = last.val;
            if (v != null) {
                final int expectedGen = last.generation;
                // pin-then-validate is cheap; we already have expectedGen
                if (v.tryPin()) {
                    if (v.value().getGeneration() == expectedGen) {
                        if (hitHolder != null)
                            hitHolder.setWasCacheHit(true);
                        return v;
                    }
                    v.unpin();
                }
            }
        }

        // -------------------------
        // TIER 2: Shared slots
        // -------------------------
        final int slotIdx = (int) ((blockIdx ^ (blockIdx >>> 17)) & SLOT_MASK);
        final Slot slot = slots[slotIdx];

        // Acquire-load stamp FIRST (publication gate)
        final long stamp = (long) STAMP.getAcquire(slot);
        if (stamp != 0) {
            final int wantHash = hashBlockIdx(blockIdx);
            final int gotHash = (int) stamp;
            if (gotHash == wantHash) {
                // Safe to read published fields after matching stamp
                if (slot.blockIdx == blockIdx) {
                    final BlockCacheValue<RefCountedMemorySegment> v = slot.val;
                    if (v != null) {
                        final int expectedGen = (int) (stamp >>> 32);
                        if (v.tryPin()) {
                            if (v.value().getGeneration() == expectedGen) {
                                // Promote to MRU
                                last.blockIdx = blockIdx;
                                last.val = v;
                                last.generation = expectedGen;

                                if (hitHolder != null)
                                    hitHolder.setWasCacheHit(true);
                                return v;
                            }
                            v.unpin();
                        }
                    }
                }
            }
        }

        // -------------------------
        // TIER 3: Main cache (race-safe pin)
        // -------------------------
        final int maxAttempts = 10;

        FileBlockCacheKey key = slotKeys[slotIdx];
        if (key == null || key.fileOffset() != blockOff) {
            key = new FileBlockCacheKey(path, blockOff);
            slotKeys[slotIdx] = key;
        }

        for (int attempts = 0; attempts < maxAttempts; attempts++) {
            // 1) Prefer hit
            BlockCacheValue<RefCountedMemorySegment> v = cache.get(key);
            if (v != null) {
                final int expectedGen = v.value().getGeneration();
                if (v.tryPin()) {
                    if (v.value().getGeneration() == expectedGen) {
                        publishToL1(slot, blockIdx, v, expectedGen);
                        last.blockIdx = blockIdx;
                        last.val = v;
                        last.generation = expectedGen;

                        if (hitHolder != null)
                            hitHolder.setWasCacheHit(true);
                        return v;
                    }
                    v.unpin(); // pinned recycled object; treat as miss
                }
            }

            // 2) Load path (deduped by caffeine get())
            BlockCacheValue<RefCountedMemorySegment> loaded = cache.getOrLoad(key);

            // For a freshly loaded segment, tryPin should *almost always* succeed,
            // but keep correctness: pin-then-validate.
            if (loaded != null) {
                final int expectedGen = loaded.value().getGeneration();
                if (loaded.tryPin()) {
                    if (loaded.value().getGeneration() == expectedGen) {
                        publishToL1(slot, blockIdx, loaded, expectedGen);
                        last.blockIdx = blockIdx;
                        last.val = loaded;
                        last.generation = expectedGen;

                        if (hitHolder != null)
                            hitHolder.setWasCacheHit(false);
                        return loaded;
                    }
                    loaded.unpin();
                }
            }

            if (attempts < maxAttempts - 1) {
                LockSupport.parkNanos(50_000L << attempts);
            }
        }

        throw new IOException("Unable to pin memory segment for block offset " + blockOff + " after " + maxAttempts + " attempts");
    }

    private static void publishToL1(Slot slot, long blockIdx, BlockCacheValue<RefCountedMemorySegment> v, int gen) {
        // Write fields first (plain)
        slot.blockIdx = blockIdx;
        slot.val = v;
        slot.generation = gen;

        // Publish stamp LAST with release semantics
        final long stamp = packStamp(hashBlockIdx(blockIdx), gen);
        STAMP.setRelease(slot, stamp);
    }

    private static long packStamp(int hash, int gen) {
        // upper 32 = gen, lower 32 = hash
        return ((gen & 0xFFFF_FFFFL) << 32) | (hash & 0xFFFF_FFFFL);
    }

    private static int hashBlockIdx(long blockIdx) {
        // small, fast mix; collisions OK because we still compare slot.blockIdx
        return (int) (blockIdx ^ (blockIdx >>> 32));
    }

    public void clear() {
        lastAccessed.remove();
        for (int i = 0; i < SLOT_COUNT; i++) {
            Slot s = slots[i];
            s.blockIdx = -1;
            s.val = null;
            s.generation = 0;
            // Clear stamp LAST (release not required here; clear is best-effort)
            s.stamp = 0;
            slotKeys[i] = null;
        }
    }
}

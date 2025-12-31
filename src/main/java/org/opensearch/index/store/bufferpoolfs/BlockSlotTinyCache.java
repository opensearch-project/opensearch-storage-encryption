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
 *  - Tier-2 slot uses a single stamp (release/acquire) to publish {blockIdx, val} consistently.
 *  - Tier-3 uses pin-then-validate generation to close TOCTOU:
 *
 *    expectedGen = v.value().getGeneration()
 *    if (v.tryPin()) {
 *      if (v.value().getGeneration() == expectedGen) return v;
 *      v.unpin();
 *    }
 *
 * Stamp gate (release/acquire) avoids reading half-updated slot fields:
 *
 *   Writer (fill slot):
 *     slotBlockIdx[i] = X;
 *     slotVal[i]      = V;
 *     stamp[i] = pack(hash(X), G)   // RELEASE store, written last
 *
 *   Reader (check slot):
 *     s = stamp[i]                 // ACQUIRE load, read first
 *     if (matches) then read slotBlockIdx/slotVal safely
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

    // VarHandle for acquire/release element access on long[]
    private static final VarHandle STAMP_ARR = MethodHandles.arrayElementVarHandle(long[].class);

    private final BlockCache<RefCountedMemorySegment> cache;
    private final Path path;

    // Parallel arrays for Tier-2 L1 slots
    private final long[] slotBlockIdx; // published under stamp gate
    private final BlockCacheValue<RefCountedMemorySegment>[] slotVal; // published under stamp gate

    /**
     * Stamp array acts as a memory barrier gate using acquire/release ordering:
     *
     * WRITER (publishToL1):
     *   1. Plain stores to slotBlockIdx[i] and slotVal[i]
     *   2. VarHandle.setRelease(slotStamp, i, stamp)  ← RELEASE barrier
     *      - Ensures all preceding plain stores become visible before stamp update
     *      - Forms a "happens-before" edge with any acquire load of this stamp
     *
     * READER (acquireRefCountedValue):
     *   1. s = VarHandle.getAcquire(slotStamp, i)     // ACQUIRE barrier
     *      - Ensures stamp is read before any dependent reads
     *      - Synchronizes-with the release store from writer
     *   2. If stamp matches, safe to read slotBlockIdx[i] and slotVal[i]
     *      - Acquire load guarantees we see the values that writer published
     *
     * Result: stamp != 0 && hash matches = {slotBlockIdx, slotVal} are consistent
     *         No torn reads, no stale data, no locks needed.
     */
    private final long[] slotStamp; // 0 => empty; otherwise pack(gen, hash(blockIdx))

    // Key reuse per slot
    private final FileBlockCacheKey[] slotKeys;

    public BlockSlotTinyCache(BlockCache<RefCountedMemorySegment> cache, Path path, long fileLength) {
        this.cache = cache;
        this.path = path;

        this.slotBlockIdx = new long[SLOT_COUNT];
        this.slotStamp = new long[SLOT_COUNT];

        @SuppressWarnings("unchecked")
        final BlockCacheValue<RefCountedMemorySegment>[] tmp = (BlockCacheValue<RefCountedMemorySegment>[]) new BlockCacheValue[SLOT_COUNT];
        this.slotVal = tmp;

        this.slotKeys = new FileBlockCacheKey[SLOT_COUNT];

        for (int i = 0; i < SLOT_COUNT; i++) {
            slotBlockIdx[i] = -1;
            slotVal[i] = null;
            slotStamp[i] = 0L;
            slotKeys[i] = null;
        }
    }

    public BlockCacheValue<RefCountedMemorySegment> acquireRefCountedValue(long blockOff) throws IOException {
        return acquireRefCountedValue(blockOff, null);
    }

    public BlockCacheValue<RefCountedMemorySegment> acquireRefCountedValue(long blockOff, CacheHitHolder hitHolder) throws IOException {

        final long blockIdx = blockOff >>> CACHE_BLOCK_SIZE_POWER;

        // -------------------------
        // TIER 2: Shared slots (stamp-gated)
        // -------------------------
        final int slotIdx = (int) ((blockIdx ^ (blockIdx >>> 17)) & SLOT_MASK);

        // Acquire-load stamp FIRST (publication gate)
        final long stamp = (long) STAMP_ARR.getAcquire(slotStamp, slotIdx);
        if (stamp != 0L) {
            final int wantHash = hashBlockIdx(blockIdx);
            final int gotHash = (int) stamp;
            if (gotHash == wantHash) {
                // Safe to read published fields after matching stamp
                if (slotBlockIdx[slotIdx] == blockIdx) {
                    final BlockCacheValue<RefCountedMemorySegment> v = slotVal[slotIdx];
                    if (v != null) {
                        final int expectedGen = (int) (stamp >>> 32);
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
                        publishToL1(slotIdx, blockIdx, v, expectedGen);
                        if (hitHolder != null)
                            hitHolder.setWasCacheHit(true);
                        return v;
                    }
                    v.unpin(); // pinned recycled object; treat as miss
                }
            }

            // 2) Load path (deduped by caffeine get())
            BlockCacheValue<RefCountedMemorySegment> loaded = cache.getOrLoad(key);
            if (loaded != null) {
                final int expectedGen = loaded.value().getGeneration();
                if (loaded.tryPin()) {
                    if (loaded.value().getGeneration() == expectedGen) {
                        publishToL1(slotIdx, blockIdx, loaded, expectedGen);
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

    private void publishToL1(int slotIdx, long blockIdx, BlockCacheValue<RefCountedMemorySegment> v, int gen) {
        // Write fields first (plain)
        slotBlockIdx[slotIdx] = blockIdx;
        slotVal[slotIdx] = v;

        // Publish stamp LAST with release semantics
        final long stamp = packStamp(hashBlockIdx(blockIdx), gen);
        STAMP_ARR.setRelease(slotStamp, slotIdx, stamp);
    }

    private static long packStamp(int hash, int gen) {
        // upper 32 = gen, lower 32 = hash
        return ((gen & 0xFFFF_FFFFL) << 32) | (hash & 0xFFFF_FFFFL);
    }

    private static int hashBlockIdx(long blockIdx) {
        // small, fast mix; collisions OK because we still compare full blockIdx
        return (int) (blockIdx ^ (blockIdx >>> 32));
    }

    public void clear() {
        for (int i = 0; i < SLOT_COUNT; i++) {
            slotBlockIdx[i] = -1;
            slotVal[i] = null;
            // Clear stamp LAST (best-effort)
            slotStamp[i] = 0L;
            slotKeys[i] = null;
        }
    }
}

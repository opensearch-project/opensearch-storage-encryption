/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;

/**
 * Tiny L1 cache in front of the main Caffeine L2 cache.
 *
 * Uses an immutable record per slot published via AtomicReferenceArray to eliminate
 * torn reads. A single atomic reference read gives the reader a consistent snapshot
 * of (blockIdx, value, generation) — no possibility of observing fields from
 * different writers.
 *
 * Thread safety:
 * - Writers create a new immutable SlotEntry and publish via plain store to the
 *   AtomicReferenceArray (reference writes are atomic per JLS §17.7).
 * - Readers load the reference atomically; all fields are final and guaranteed
 *   fully constructed (JLS §17.5 final field semantics).
 * - A stale read sees either the old entry or the new entry, never a mix.
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

    private record SlotEntry(long blockIdx, BlockCacheValue<RefCountedMemorySegment> val, int gen) {
    }

    private static final int SLOT_COUNT = 32;
    private static final int SLOT_MASK = SLOT_COUNT - 1;

    private final BlockCache<RefCountedMemorySegment> cache;
    private final Path path;

    private final AtomicReferenceArray<SlotEntry> slots = new AtomicReferenceArray<>(SLOT_COUNT);

    // Key reuse per slot
    private final FileBlockCacheKey[] slotKeys = new FileBlockCacheKey[SLOT_COUNT];

    public BlockSlotTinyCache(BlockCache<RefCountedMemorySegment> cache, Path path, long fileLength) {
        this.cache = cache;
        this.path = path;
    }

    public BlockCacheValue<RefCountedMemorySegment> acquireRefCountedValue(long blockOff) throws IOException {
        return acquireRefCountedValue(blockOff, null);
    }

    public BlockCacheValue<RefCountedMemorySegment> acquireRefCountedValue(long blockOff, CacheHitHolder hitHolder) throws IOException {

        final long blockIdx = blockOff >>> CACHE_BLOCK_SIZE_POWER;
        final int slotIdx = (int) ((blockIdx ^ (blockIdx >>> 17)) & SLOT_MASK);

        // L1 lookup: single atomic reference read gives consistent snapshot
        SlotEntry entry = slots.get(slotIdx);
        if (entry != null && entry.blockIdx == blockIdx) {
            BlockCacheValue<RefCountedMemorySegment> v = entry.val;
            if (v.tryPin()) {
                if (v.value().getGeneration() == entry.gen) {
                    if (hitHolder != null)
                        hitHolder.setWasCacheHit(true);
                    return v;
                }
                v.unpin();
            }
        }

        final int maxAttempts = 10;

        FileBlockCacheKey key = slotKeys[slotIdx];
        if (key == null || key.fileOffset() != blockOff) {
            key = new FileBlockCacheKey(path, blockOff);
            slotKeys[slotIdx] = key;
        }

        for (int attempts = 0; attempts < maxAttempts; attempts++) {
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
                    v.unpin();
                }
            }

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
        slots.set(slotIdx, new SlotEntry(blockIdx, v, gen));
    }

    public void clear() {
        for (int i = 0; i < SLOT_COUNT; i++) {
            slots.set(i, null);
            slotKeys[i] = null;
        }
    }
}

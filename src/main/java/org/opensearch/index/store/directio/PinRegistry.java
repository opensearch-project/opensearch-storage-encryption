/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;

import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;

@SuppressWarnings("preview")
public final class PinRegistry {

    private static final int SLOT_COUNT = 32;
    private static final int SLOT_MASK = SLOT_COUNT - 1;

    private record Slot(long blockIdx, BlockCacheValue<RefCountedMemorySegment> val) {
    }

    private final BlockCache<RefCountedMemorySegment> cache;
    private final Path path;
    private final Slot[] slots;

    // Pre-allocated keys for hot slots to reduce allocation pressure
    private final DirectIOBlockCacheKey[] slotKeys;

    private long lastBlockIdx = -1;
    private BlockCacheValue<RefCountedMemorySegment> lastVal;

    PinRegistry(BlockCache<RefCountedMemorySegment> cache, Path path, long fileLength) {
        this.cache = cache;
        this.path = path;
        this.slots = new Slot[SLOT_COUNT];
        this.slotKeys = new DirectIOBlockCacheKey[SLOT_COUNT];
    }

    public MemorySegment acquire(long blockOff) throws IOException {
        final long blockIdx = blockOff >>> CACHE_BLOCK_SIZE_POWER;

        // Fast path: last accessed (avoid slot calculation if possible)
        if (blockIdx == lastBlockIdx) {
            BlockCacheValue<RefCountedMemorySegment> val = lastVal;
            if (val != null && !val.isRetired()) {
                return val.value().segment();
            }
        }

        final int slotIdx = (int) (blockIdx & SLOT_MASK);

        // Slot lookup - single memory access, better cache locality
        Slot slot = slots[slotIdx];
        if (slot != null && slot.blockIdx == blockIdx) {
            BlockCacheValue<RefCountedMemorySegment> val = slot.val;
            if (val != null && !val.isRetired()) {
                // Cache both slot and last reference atomically
                lastBlockIdx = blockIdx;
                lastVal = val;
                return val.value().segment();
            }
        }

        // Cache miss path - reuse pre-allocated key if possible
        DirectIOBlockCacheKey key = slotKeys[slotIdx];
        if (key == null || key.fileOffset() != blockOff) {
            key = new DirectIOBlockCacheKey(path, blockOff);
            slotKeys[slotIdx] = key; // Cache for future use
        }
        BlockCacheValue<RefCountedMemorySegment> val = cache.get(key);

        if (val == null) {
            val = cache.getOrLoad(key);
        }

        // Single update point - create new slot record
        slots[slotIdx] = new Slot(blockIdx, val);
        lastBlockIdx = blockIdx;
        lastVal = val;

        return val.value().segment();
    }

    public void clear() {
        lastBlockIdx = -1;
        lastVal = null;
        for (int i = 0; i < SLOT_COUNT; i++) {
            slots[i] = null;
            slotKeys[i] = null; // Clear cached keys
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Optional;

import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;

@SuppressWarnings("preview")
public final class PinRegistry {

    // Optimized for CPU cache efficiency with 8KB blocks
    // 64 slots = 64 * 16 bytes = 1KB metadata (fits well in L1 cache)
    // Can cache up to 64 * 8KB = 512KB of blocks (good for L2 cache)
    private static final int SLOT_COUNT = 64;
    private static final int SLOT_MASK = SLOT_COUNT - 1;

    private final BlockCache<RefCountedMemorySegment> cache;
    private final Path path;
    private final MemorySegment[] slots;
    private final long[] blockIndices;

    // Cache the last accessed block to avoid repeated lookups
    private long lastBlockIdx = -1;
    private MemorySegment lastSegment;

    PinRegistry(BlockCache<RefCountedMemorySegment> cache, Path path, long fileLength) {
        this.cache = cache;
        this.path = path;
        this.slots = new MemorySegment[SLOT_COUNT];
        this.blockIndices = new long[SLOT_COUNT];
        // Initialize with invalid block indices
        for (int i = 0; i < SLOT_COUNT; i++) {
            blockIndices[i] = -1;
        }
    }

    public MemorySegment acquire(long blockOff) throws IOException {
        final long blockIdx = blockOff >>> CACHE_BLOCK_SIZE_POWER;

        // Fast path: check if same as last accessed block
        if (blockIdx == lastBlockIdx && lastSegment != null) {
            return lastSegment;
        }

        final int slotIdx = (int) (blockIdx & SLOT_MASK);

        // Check if this slot contains the block we want
        if (blockIndices[slotIdx] == blockIdx) {
            MemorySegment segment = slots[slotIdx];
            if (segment != null) {
                // Update last accessed cache
                lastBlockIdx = blockIdx;
                lastSegment = segment;

                // This slot proved useful - keep it longer by not updating it
                return segment;
            }
        }

        final DirectIOBlockCacheKey key = new DirectIOBlockCacheKey(path, blockOff);

        Optional<BlockCacheValue<RefCountedMemorySegment>> maybeCache = cache.get(key);

        MemorySegment segment;
        if (maybeCache.isPresent()) {
            segment = maybeCache.get().value().segment();
        } else {
            segment = cache.getOrLoad(key).value().segment();
        }

        lastBlockIdx = blockIdx;
        lastSegment = segment;

        // Update slot only if it's wrong or empty (blocks are immutable)
        // Since we had a cache miss, this slot either has wrong block or is empty
        if (blockIndices[slotIdx] != blockIdx) {
            blockIndices[slotIdx] = blockIdx;
            slots[slotIdx] = segment;
        }

        return segment;
    }

    /**
     * Clear all cached slots. Should be called when the IndexInput is closed
     * to prevent memory leaks and allow GC of MemorySegments.
     */
    public void clear() {
        // Clear last accessed cache
        lastBlockIdx = -1;
        lastSegment = null;

        // Clear all slots
        for (int i = 0; i < SLOT_COUNT; i++) {
            slots[i] = null;
            blockIndices[i] = -1;
        }
    }
}

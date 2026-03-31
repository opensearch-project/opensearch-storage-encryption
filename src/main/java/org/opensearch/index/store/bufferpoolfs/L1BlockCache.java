/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import java.io.IOException;

import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCacheValue;

/**
 * L1 block cache interface sitting in front of the Caffeine L2 cache.
 * Implementations provide fast per-VFD lookups with L2 fallback on miss.
 */
public interface L1BlockCache {

    /**
     * Acquires a pinned block value for the given block offset.
     * On L1 hit returns immediately; on miss falls through to L2 and publishes back to L1.
     *
     * @param blockOffset the block-aligned file offset
     * @param hitHolder optional holder to record whether this was a cache hit
     * @return a pinned BlockCacheValue (never null — throws on failure)
     * @throws IOException if the block cannot be acquired after retries
     */
    BlockCacheValue<RefCountedMemorySegment> acquireRefCountedValue(long blockOffset, BlockSlotTinyCache.CacheHitHolder hitHolder)
        throws IOException;

    default BlockCacheValue<RefCountedMemorySegment> acquireRefCountedValue(long blockOffset) throws IOException {
        return acquireRefCountedValue(blockOffset, null);
    }

    void clear();

    /** Returns true if the block at the given offset is present in L1 (no load, no pin). */
    default boolean contains(long blockOffset) {
        return false;
    }

    String stats();

    void resetStats();
}

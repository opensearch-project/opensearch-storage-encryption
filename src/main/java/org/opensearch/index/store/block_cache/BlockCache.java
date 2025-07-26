/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.io.IOException;
import java.util.Optional;

public interface BlockCache<T> {

    /**
     * Returns the block if cached, or null if absent.
     */
    BlockCacheValue<T> get(BlockCacheKey key);

    /**
     * Returns the block, loading it via `BlockLoader` if absent.
     */
    Optional<BlockCacheValue<T>> getOrLoad(BlockCacheKey key, int size, BlockLoader<T> loader) throws IOException;

    /**
     * Asynchronously load the block into the cache if not present.
     */
    void prefetch(BlockCacheKey key, int size);

    /**
     * Put a block into the cache.
     */
    void put(BlockCacheKey key, BlockCacheValue<T> value);

    /**
     * Evict a block from the cache.
     */
    void invalidate(BlockCacheKey key);

    /**
     * Clear all blocks.
     */
    void clear();

    /**
     * cache stats
     */
    String cacheStats();
}

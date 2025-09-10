/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public interface BlockCache<T> {

    /**
     * Returns the block if cached, or null if absent.
     */
    BlockCacheValue<T> get(BlockCacheKey key);

    /**
     * Returns the block, loading it via `BlockLoader` if absent.
     * Throws IOException if the block cannot be loaded.
     */
    BlockCacheValue<T> getOrLoad(BlockCacheKey key) throws IOException;

    /**
     * Asynchronously load the block into the cache if not present.
     */
    void prefetch(BlockCacheKey key);

    /**
     * Put a block into the cache.
     */
    void put(BlockCacheKey key, BlockCacheValue<T> value);

    /**
     * Evict a block from the cache.
     */
    void invalidate(BlockCacheKey key);

    /**
     * Evict all blocks for a given normalized file path.
     */
    void invalidate(Path normalizedFilePath);

    /**
     * Clear all blocks.
     */
    void clear();

    /**
     * Bulk load multiple blocks efficiently using a single I/O operation.
     * Similar to getOrLoad() but for a contiguous range of blocks.
     * 
     * @param filePath file to read from
     * @param startOffset starting file offset (should be block-aligned)
     * @param blockCount number of blocks to read
     * @return list of cache values for blocks that were successfully loaded into the cache
     * @throws IOException if loading fails (including specific BlockLoader exceptions)
     */
    Map<BlockCacheKey, BlockCacheValue<T>> loadBulk(Path filePath, long startOffset, long blockCount) throws IOException;

    /**
     * cache stats
     */
    String cacheStats();
}

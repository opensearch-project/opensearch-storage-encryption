/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.block_loader.BlockLoader;
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE;
import org.opensearch.index.store.pool.Pool;

import com.github.benmanes.caffeine.cache.Cache;

@SuppressForbidden(reason = "uses custom DirectIO")
public final class CaffeineBlockCache<T, V> implements BlockCache<T> {
    private static final Logger LOGGER = LogManager.getLogger(CaffeineBlockCache.class);

    private final Cache<BlockCacheKey, BlockCacheValue<T>> cache;
    private final BlockLoader<V> blockLoader;
    private final Pool<V> segmentPool;

    public CaffeineBlockCache(
        Cache<BlockCacheKey, BlockCacheValue<T>> cache,
        BlockLoader<V> blockLoader,
        Pool<V> segmentPool,
        long maxBlocks
    ) {
        this.blockLoader = blockLoader;
        this.cache = cache;
        this.segmentPool = segmentPool;
    }

    @Override
    public BlockCacheValue<T> get(BlockCacheKey key) {
        return cache.getIfPresent(key);
    }

    /**
    * Retrieves the cached block associated with the given key, or loads it if not present.
    * <p>
    * If the block is present in the cache, it is returned immediately.
    * If the block is absent, the {@link BlockLoader} is invoked to load it. If loading succeeds,
    * the loaded block is inserted into the cache and returned. If loading fails, an exception is thrown.
    * <p>
    * Any {@link IOException} thrown by the loader is propagated, while other exceptions are wrapped
    * in {@link IOException}.
    *
    * @param key  The key identifying the block to retrieve or load.
    * @return The cached or newly loaded block (never null).
    * @throws IOException if the block loading fails with an IO-related error.
    */
    @Override
    public BlockCacheValue<T> getOrLoad(BlockCacheKey key) throws IOException {
        try {
            BlockCacheValue<T> value = cache.get(key, k -> {
                try {
                    V segment = blockLoader.load(k);
                    @SuppressWarnings("unchecked")
                    BlockCacheValue<T> result = (BlockCacheValue<T>) segment;
                    return result;
                } catch (Exception e) {
                    return handleLoadException(k, e);
                }
            });

            if (value == null) {
                throw new IOException("Failed to load block for key: " + key);
            }

            return value;
        } catch (UncheckedIOException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new IOException("Failed to load block for key: " + key, e);
        }
    }

    @Override
    public void prefetch(BlockCacheKey key) {
        try {
            cache.get(key, k -> {
                try {
                    V segment = blockLoader.load(k);
                    // Direct cast - BlockLoader contract guarantees V is BlockCacheValue<T>
                    @SuppressWarnings("unchecked")
                    BlockCacheValue<T> result = (BlockCacheValue<T>) segment;
                    return result;
                } catch (Exception e) {
                    return handleLoadException(k, e);
                }
            });
        } catch (Exception e) {
            // Prefetch failures are non-fatal - log and continue
            LOGGER.debug("Prefetch failed for key: {}", key, e);
        }
    }

    @Override
    public void put(BlockCacheKey key, BlockCacheValue<T> value) {
        cache.put(key, value);
    }

    @Override
    public void invalidate(BlockCacheKey key) {
        cache.invalidate(key);
    }

    @Override
    public void invalidate(Path filePath) {
        Path normalized = filePath.toAbsolutePath().normalize();
        var keysToInvalidate = cache
            .asMap()
            .keySet()
            .stream()
            .filter(key -> key instanceof FileBlockCacheKey directIOKey && directIOKey.filePath().equals(normalized))
            .toList();

        // invalidateAll to trigger removal listener for proper segment cleanup
        // note: invalidateAll doesn't effect eviction count.
        if (!keysToInvalidate.isEmpty()) {
            cache.invalidateAll(keysToInvalidate);
        }
    }

    @Override
    public void clear() {
        // note: invalidateAll doesn't effect eviction count.
        cache.invalidateAll();
    }

    /**
     * Bulk load multiple blocks efficiently using a single I/O operation.
     * Similar to getOrLoad() but for a contiguous range of blocks.
     * 
     * @param filePath file to read from
     * @param startOffset starting file offset (should be block-aligned)
     * @param blockCount number of blocks to read
     * @throws IOException if loading fails (including specific BlockLoader exceptions)
     */
    @Override
    public Map<BlockCacheKey, BlockCacheValue<T>> loadBulk(Path filePath, long startOffset, long blockCount) throws IOException {
        Map<BlockCacheKey, BlockCacheValue<T>> loaded = new LinkedHashMap<>();

        V[] loadedBlocks;

        try {
            loadedBlocks = blockLoader.load(filePath, startOffset, blockCount);

            for (int i = 0; i < loadedBlocks.length; i++) {
                V block = loadedBlocks[i];

                long blockOffset = startOffset + i * CACHE_BLOCK_SIZE;
                BlockCacheKey key = createBlockKey(filePath, blockOffset);

                // Direct cast - BlockLoader contract guarantees V is BlockCacheValue<T>
                @SuppressWarnings("unchecked")
                BlockCacheValue<T> wrapped = (BlockCacheValue<T>) block;
                loaded.put(key, wrapped);

                if (cache.asMap().putIfAbsent(key, wrapped) != null) {
                    // already cached → release our newly loaded segment as we won't use it
                    // we use decRef() not close() - this segment was never inserted into cache,
                    // so we shouldn't increment generation.
                    wrapped.decRef();
                }
            }

        } catch (Exception e) {
            try {
                handleLoadException(createBlockKey(filePath, startOffset), e);
            } catch (UncheckedIOException uie) {
                throw uie.getCause();
            } catch (RuntimeException re) {
                throw new IOException("Failed bulk load: " + filePath, re);
            }
        }

        return loaded;
    }

    // Helper method to create appropriate cache key for file blocks
    private BlockCacheKey createBlockKey(Path filePath, long offset) {
        return new FileBlockCacheKey(filePath, offset);
    }

    private BlockCacheValue<T> handleLoadException(BlockCacheKey key, Exception e) {
        switch (e) {
            case BlockLoader.PoolPressureException ppe -> throw new UncheckedIOException(ppe);
            case BlockLoader.PoolAcquireFailedException pafe -> throw new UncheckedIOException(pafe);
            case BlockLoader.BlockLoadFailedException blfe -> throw new UncheckedIOException(blfe);
            case java.nio.file.NoSuchFileException nsfe -> throw new UncheckedIOException(nsfe);
            case IOException io -> throw new UncheckedIOException(io);
            case RuntimeException rte -> throw rte;
            default -> throw new RuntimeException("Unexpected exception during block load for key: " + key, e);
        }
    }

    @Override
    public String cacheStats() {
        var stats = cache.stats();
        return String
            .format(
                "Cache[size=%d, hits=%d, misses=%d, hitRate=%.2f%%, loads=%d, evictionCount=%d, avgLoadTime=%.2fms]",
                cache.estimatedSize(),
                stats.hitCount(),
                stats.missCount(),
                stats.hitRate() * 100,
                stats.loadCount(),
                stats.evictionCount(),
                stats.averageLoadPenalty() / 1_000_000.0  // Convert to ms
            );
    }

    /**
     * Get the underlying Caffeine cache instance.
     * This is used for sharing the cache storage across multiple BlockCache instances
     * with different loaders.
     */
    public Cache<BlockCacheKey, BlockCacheValue<T>> getCache() {
        return cache;
    }

}

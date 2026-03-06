/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.index.store.metrics.CryptoMetricsService;

import com.github.benmanes.caffeine.cache.Cache;

/**
 * A Caffeine-based implementation of {@link BlockCache} that provides efficient caching
 * of file blocks with automatic loading, eviction, and cleanup.
 * 
 * <p>This cache integrates with Caffeine's high-performance caching library to provide
 * concurrent, thread-safe access to cached blocks. It supports automatic loading via
 * {@link BlockLoader} instances, bulk operations for efficient I/O, and proper cleanup
 * of cached resources through reference counting.
 *
 * @param <T> the type of cached block data
 * @param <V> the type of data loaded by the BlockLoader
 * @opensearch.internal
 */
@SuppressForbidden(reason = "uses custom DirectIO")
public final class CaffeineBlockCache<T, V> implements BlockCache<T> {
    private static final Logger LOGGER = LogManager.getLogger(CaffeineBlockCache.class);

    private final Cache<BlockCacheKey, BlockCacheValue<T>> cache;
    private final BlockLoader<V> blockLoader;
    private final PrefetchTracker prefetchTracker;

    /**
     * Constructs a new CaffeineBlockCache with the specified cache, block loader, and prefetch tracker.
     *
     * @param cache the underlying Caffeine cache instance
     * @param blockLoader the loader used to load blocks when cache misses occur
     * @param maxBlocks the maximum number of blocks to cache (currently unused but kept for API compatibility)
     * @param prefetchTracker tracker for prefetch deduplication, stats, and async execution
     */
    public CaffeineBlockCache(
        Cache<BlockCacheKey, BlockCacheValue<T>> cache,
        BlockLoader<V> blockLoader,
        long maxBlocks,
        PrefetchTracker prefetchTracker
    ) {
        this.blockLoader = blockLoader;
        this.cache = cache;
        this.prefetchTracker = prefetchTracker;
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
    public void invalidateByPathPrefix(Path directoryPath) {
        Path normalized = directoryPath.toAbsolutePath().normalize();
        var keysToInvalidate = cache
            .asMap()
            .keySet()
            .stream()
            .filter(key -> key instanceof FileBlockCacheKey directIOKey && directIOKey.filePath().startsWith(normalized))
            .toList();

        // invalidateAll to trigger removal listener for proper segment cleanup
        // note: invalidateAll doesn't effect eviction count.
        if (!keysToInvalidate.isEmpty()) {
            LOGGER.debug("Invalidating {} cache entries for path prefix: {}", keysToInvalidate.size(), normalized);
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
     * Checks cache first and only loads missing blocks, combining consecutive ranges.
     * 
     * @param filePath file to read from
     * @param startOffset starting file offset (should be block-aligned)
     * @param blockCount number of blocks to read
     * @throws IOException if loading fails (including specific BlockLoader exceptions)
     */
    @Override
    public void loadMissingBlocks(Path filePath, long startOffset, long blockCount) throws IOException {
        prefetchTracker.recordLoadMissingBlocksCall(blockCount);
        try {
            prefetchTracker.execute(() -> {
                try {
                    loadMissingBlocksSync(filePath, startOffset, blockCount);
                } catch (Exception e) {
                    LOGGER.error("failed to prefetch blocks: path={} offset={} count={}", filePath, startOffset, blockCount, e);
                }
            });
        } catch (Exception e) {
            LOGGER.warn("prefetch task rejected: path={} offset={} count={} e={}", filePath, startOffset, blockCount, e.getMessage());
        }
    }

    private void loadMissingBlocksSync(Path filePath, long startOffset, long blockCount) throws IOException {
        long totalLoaded = 0;

        // Check which blocks are already cached
        FileBlockCacheKey[] missingKeys = new FileBlockCacheKey[(int) blockCount];
        int missingCount = 0;
        long cacheHitCount = 0;

        for (int i = 0; i < blockCount; i++) {
            long blockOffset = startOffset + i * CACHE_BLOCK_SIZE;
            FileBlockCacheKey key = (FileBlockCacheKey) createBlockKey(filePath, blockOffset);
            // check if this block is already in progress
            if (prefetchTracker.putIfAbsent(key)) {
                if (cache.getIfPresent(key) == null) {
                    missingKeys[missingCount++] = key;
                } else {
                    prefetchTracker.remove(key);
                    cacheHitCount++;
                }
            }
        }

        if (cacheHitCount > 0) {
            prefetchTracker.recordCacheHits(cacheHitCount);
        }

        if (missingCount == 0) {
            return; // All blocks already cached
        }

        // Load consecutive ranges
        int rangeStart = 0;
        while (rangeStart < missingCount) {
            long rangeStartOffset = missingKeys[rangeStart].offset();
            int rangeLength = 1;

            // Find consecutive blocks
            while (rangeStart + rangeLength < missingCount
                && missingKeys[rangeStart + rangeLength].offset() == rangeStartOffset + rangeLength * CACHE_BLOCK_SIZE) {
                rangeLength++;
            }

            long loadedFor = loadAllBlocks(filePath, rangeStartOffset, rangeLength);
            totalLoaded += loadedFor;

            // Remove loaded blocks from prefetch tracker
            for (int i = 0; i < rangeLength; i++) {
                prefetchTracker.remove(missingKeys[rangeStart + i]);
            }

            rangeStart += rangeLength;
        }

        prefetchTracker.recordBlocksLoaded(totalLoaded);
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
    public long loadAllBlocks(Path filePath, long startOffset, long blockCount) throws IOException {
        long loadedCount = 0;

        V[] loadedBlocks;

        try {
            // Use 50ms timeout for prefetch - fail fast when pool is under pressure
            loadedBlocks = blockLoader.load(filePath, startOffset, blockCount, 50);

            for (int i = 0; i < loadedBlocks.length; i++) {
                V block = loadedBlocks[i];

                long blockOffset = startOffset + i * CACHE_BLOCK_SIZE;
                BlockCacheKey key = createBlockKey(filePath, blockOffset);

                // Direct cast - BlockLoader contract guarantees V is BlockCacheValue<T>
                @SuppressWarnings("unchecked")
                BlockCacheValue<T> wrapped = (BlockCacheValue<T>) block;

                if (cache.asMap().putIfAbsent(key, wrapped) == null) {
                    // Successfully inserted into cache
                    loadedCount++;
                } else {
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
        return loadedCount;
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
     *
     * @return the underlying Caffeine cache instance
     */
    public Cache<BlockCacheKey, BlockCacheValue<T>> getCache() {
        return cache;
    }

    @Override
    public void recordStats() {
        var stats = cache.stats();
        CryptoMetricsService
            .getInstance()
            .recordCacheStats(
                cache.estimatedSize(),
                stats.hitCount(),
                stats.missCount(),
                stats.hitRate() * 100,
                stats.loadCount(),
                stats.evictionCount(),
                stats.averageLoadPenalty() / 1_000_000.0  // Convert to ms
            );
        CryptoMetricsService
            .getInstance()
            .recordPrefetchStats(
                prefetchTracker.getCalls(),
                prefetchTracker.getBlocksRequested(),
                prefetchTracker.getBlocksLoaded(),
                prefetchTracker.getBlocksDeduped(),
                prefetchTracker.getBlocksCacheHit(),
                prefetchTracker.size()
            );
    }

    @Override
    public double getHitRate() {
        return cache.stats().hitRate();
    }

    @Override
    public long getCacheSize() {
        return cache.estimatedSize();
    }

    @Override
    public long hitCount() {
        return cache.stats().hitCount();
    }

    @Override
    public long missCount() {
        return cache.stats().missCount();
    }

}

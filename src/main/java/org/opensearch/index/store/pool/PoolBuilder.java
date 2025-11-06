/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE;

import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheBuilder;

/**
 * Builder for creating shared pool and cache resources with proper lifecycle management.
 * This class handles initialization of node-level shared resources used across all
 * encrypted directories.
 */
public final class PoolBuilder {

    private static final Logger LOGGER = LogManager.getLogger(PoolBuilder.class);

    /** 
    * Initial size for cache data structures (64K entries).
    */
    public static final int CACHE_INITIAL_SIZE = 65536;

    private PoolBuilder() {}

    /**
     * Container for shared pool resources with lifecycle management.
     * This class holds references to the shared memory segment pool, block cache,
     * telemetry thread, cache removal executor, and read-ahead executor service,
     * providing proper cleanup when closed.
     */
    public static class PoolResources implements Closeable {
        private final Pool<RefCountedMemorySegment> segmentPool;
        private final BlockCache<RefCountedMemorySegment> blockCache;
        private final long maxCacheBlocks;
        private final TelemetryThread telemetry;
        private final java.util.concurrent.ThreadPoolExecutor removalExecutor;
        private final ExecutorService readAheadExecutor;

        PoolResources(
            Pool<RefCountedMemorySegment> segmentPool,
            BlockCache<RefCountedMemorySegment> blockCache,
            long maxCacheBlocks,
            TelemetryThread telemetry,
            java.util.concurrent.ThreadPoolExecutor removalExecutor,
            ExecutorService readAheadExecutor
        ) {
            this.segmentPool = segmentPool;
            this.blockCache = blockCache;
            this.maxCacheBlocks = maxCacheBlocks;
            this.telemetry = telemetry;
            this.removalExecutor = removalExecutor;
            this.readAheadExecutor = readAheadExecutor;
        }

        /**
         * Returns the shared memory segment pool.
         *
         * @return the segment pool
         */
        public Pool<RefCountedMemorySegment> getSegmentPool() {
            return segmentPool;
        }

        /**
         * Returns the shared block cache.
         *
         * @return the block cache
         */
        public BlockCache<RefCountedMemorySegment> getBlockCache() {
            return blockCache;
        }

        /**
         * Returns the maximum number of blocks that can be cached.
         *
         * @return the maximum cache blocks
         */
        public long getMaxCacheBlocks() {
            return maxCacheBlocks;
        }

        /**
         * Returns the shared read-ahead executor service.
         * This executor is shared across all per-shard workers for thread reuse while maintaining queue isolation.
         *
         * @return the read-ahead executor service
         */
        public ExecutorService getReadAheadExecutor() {
            return readAheadExecutor;
        }

        /**
         * Closes the shared pool resources, stops the telemetry thread, and shuts down executors.
         */
        @Override
        public void close() {
            if (telemetry != null) {
                telemetry.close();
            }
            if (removalExecutor != null) {
                removalExecutor.shutdown();
                try {
                    if (!removalExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                        removalExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    removalExecutor.shutdownNow();
                }
            }
            if (readAheadExecutor != null) {
                readAheadExecutor.shutdown();
                try {
                    if (!readAheadExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                        readAheadExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    readAheadExecutor.shutdownNow();
                }
            }
        }
    }

    /**
     * Autocloseable telemetry thread for periodic pool statistics logging.
     */
    private static class TelemetryThread implements Closeable {
        private final Thread thread;
        private final Pool<RefCountedMemorySegment> pool;

        TelemetryThread(Pool<RefCountedMemorySegment> pool) {
            this.pool = pool;
            this.thread = new Thread(this::run);
            this.thread.setDaemon(true);
            this.thread.setName("DirectIOBufferPoolStatsLogger");
            this.thread.start();
        }

        private void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(Duration.ofMinutes(5));
                    publishPoolStats();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Throwable t) {
                    LOGGER.warn("Panic in buffer pool stats logger", t);
                }
            }
        }

        private void publishPoolStats() {
            try {
                LOGGER.info("{}", pool.poolStats());
            } catch (Exception e) {
                LOGGER.warn("Failed to log cache stats", e);
            }
        }

        @Override
        public void close() {
            thread.interrupt();
            try {
                thread.join(5000); // Wait up to 5 seconds for graceful shutdown
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Initialized the MemorySegmentPool and BlockCache.
     *
     * @param settings the node settings for configuration
     * @return SharedPoolResources containing the initialized pool and cache
     */
    public static PoolResources build(Settings settings) {
        long reservedPoolSizeInBytes = PoolSizeCalculator.calculatePoolSize(settings);

        reservedPoolSizeInBytes = (reservedPoolSizeInBytes / CACHE_BLOCK_SIZE) * CACHE_BLOCK_SIZE;
        long maxBlocks = reservedPoolSizeInBytes / CACHE_BLOCK_SIZE;

        double poolToCacheRatio = PoolSizeCalculator.NODE_POOL_TO_CACHE_RATIO_SETTING.get(settings);
        double warmupPercentage = PoolSizeCalculator.NODE_WARMUP_PERCENTAGE_SETTING.get(settings);

        Pool<RefCountedMemorySegment> segmentPool = new MemorySegmentPool(reservedPoolSizeInBytes, CACHE_BLOCK_SIZE);
        LOGGER
            .info(
                "Creating shared pool with sizeBytes={}, segmentSize={}, totalSegments={}",
                reservedPoolSizeInBytes,
                CACHE_BLOCK_SIZE,
                maxBlocks
            );

        // Calculate cache size: cache = pool / ratio
        long maxCacheBlocks = (long) (maxBlocks / poolToCacheRatio);
        long warmupBlocks = (long) (maxCacheBlocks * warmupPercentage);
        segmentPool.warmUp(warmupBlocks);
        LOGGER.info("Warmed up {} blocks ({}% of {} cache blocks)", warmupBlocks, warmupPercentage * 100, maxCacheBlocks);

        // Initialize shared cache with removal listener and get its executor
        BlockCacheBuilder.CacheWithExecutor<RefCountedMemorySegment, RefCountedMemorySegment> cacheWithExecutor = BlockCacheBuilder
            .build(CACHE_INITIAL_SIZE, maxCacheBlocks);
        BlockCache<RefCountedMemorySegment> blockCache = cacheWithExecutor.getCache();
        java.util.concurrent.ThreadPoolExecutor removalExecutor = cacheWithExecutor.getExecutor();
        LOGGER.info("Creating shared block cache with blocks={}", maxCacheBlocks);

        // Create shared read-ahead executor service
        // Each per-shard worker will have its own queue but share these threads
        int threads = Math.max(8, Runtime.getRuntime().availableProcessors() / 4);
        AtomicInteger threadId = new AtomicInteger();
        ExecutorService readAheadExecutor = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "readahead-worker-" + threadId.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
        LOGGER.info("Creating shared read-ahead executor with threads={}", threads);

        // Start telemetry
        TelemetryThread telemetry = new TelemetryThread(segmentPool);

        return new PoolResources(segmentPool, blockCache, maxCacheBlocks, telemetry, removalExecutor, readAheadExecutor);
    }
}

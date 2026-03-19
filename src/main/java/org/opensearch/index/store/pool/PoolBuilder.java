/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;

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
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.index.store.read_ahead.impl.QueuingWorker;
import org.opensearch.index.store.read_ahead.impl.ReadAheadSizingPolicy;

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
        private final Pool<?> pool;
        private final BlockCache<?> blockCache;
        private final com.github.benmanes.caffeine.cache.Cache<?, ?> sharedCaffeineCache;
        private final boolean byteBufferMode;
        private final long maxCacheBlocks;
        private final int readAheadQueueSize;
        private final Worker sharedReadaheadWorker;
        private final TelemetryThread telemetry;
        private final java.util.concurrent.ThreadPoolExecutor removalExecutor;
        private final ExecutorService readAheadExecutor;

        PoolResources(
            Pool<?> pool,
            BlockCache<?> blockCache,
            com.github.benmanes.caffeine.cache.Cache<?, ?> sharedCaffeineCache,
            boolean byteBufferMode,
            long maxCacheBlocks,
            int readAheadQueueSize,
            Worker sharedReadaheadWorker,
            TelemetryThread telemetry,
            java.util.concurrent.ThreadPoolExecutor removalExecutor,
            ExecutorService readAheadExecutor
        ) {
            this.pool = pool;
            this.blockCache = blockCache;
            this.sharedCaffeineCache = sharedCaffeineCache;
            this.byteBufferMode = byteBufferMode;
            this.maxCacheBlocks = maxCacheBlocks;
            this.readAheadQueueSize = readAheadQueueSize;
            this.sharedReadaheadWorker = sharedReadaheadWorker;
            this.telemetry = telemetry;
            this.removalExecutor = removalExecutor;
            this.readAheadExecutor = readAheadExecutor;
        }

        public Pool<?> getPool() {
            return pool;
        }

        @SuppressWarnings("unchecked")
        public <T> Pool<T> getPoolAs(Class<T> type) {
            return (Pool<T>) pool;
        }

        public BlockCache<?> getBlockCache() {
            return blockCache;
        }

        @SuppressWarnings("unchecked")
        public <K, V> com.github.benmanes.caffeine.cache.Cache<K, V> getSharedCaffeineCache() {
            return (com.github.benmanes.caffeine.cache.Cache<K, V>) sharedCaffeineCache;
        }

        public boolean isByteBufferMode() {
            return byteBufferMode;
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
         * Returns the calculated read-ahead queue size.
         *
         * @return the read-ahead queue size
         */
        public int getReadAheadQueueSize() {
            return readAheadQueueSize;
        }

        /**
         * Returns the shared read-ahead worker.
         * This worker is shared across all shards/directories with a single queue and executor pool.
         *
         * @return the shared read-ahead worker
         */
        public Worker getSharedReadaheadWorker() {
            return sharedReadaheadWorker;
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
            if (sharedReadaheadWorker != null) {
                try {
                    sharedReadaheadWorker.close();
                } catch (Exception e) {
                    LOGGER.warn("Error closing shared readahead worker", e);
                }
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
        private final Pool<?> pool;
        private final BlockCache<?> blockCache;

        TelemetryThread(Pool<?> pool, BlockCache<?> blockCache) {
            this.pool = pool;
            this.blockCache = blockCache;
            this.thread = new Thread(this::run);
            this.thread.setDaemon(true);
            this.thread.setName("DirectIOBufferPoolStatsLogger");
            this.thread.start();
        }

        private void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(Duration.ofMinutes(5));
                    publishStats();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Throwable t) {
                    LOGGER.warn("Panic in telemetry buffer stats logger", t);
                }
            }
        }

        private void publishStats() {
            try {
                if (pool != null)
                    pool.recordStats();
                blockCache.recordStats();
            } catch (Exception e) {
                LOGGER.warn("Failed to log cache/pool stats", e);
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

        // Calculate off-heap memory for tiered cache ratio and warmup
        long maxHeap = Runtime.getRuntime().maxMemory();
        long totalPhysical = org.opensearch.monitor.os.OsProbe.getInstance().getTotalPhysicalMemorySize();
        if (totalPhysical <= 0) {
            throw new IllegalStateException("Failed to calculate instance's physical memory, bailing out...: " + totalPhysical);
        }
        long offHeap = Math.max(0, totalPhysical - maxHeap);

        double cacheToPoolRatio = PoolSizeCalculator.calculateCacheToPoolRatio(offHeap, settings);
        double warmupPercentage = PoolSizeCalculator.calculateWarmupPercentage(offHeap, settings);

        long maxCacheBlocks = (long) (maxBlocks * cacheToPoolRatio);
        boolean byteBufferMode = org.opensearch.index.store.CryptoDirectoryFactory.byteBufferModeEnabled();

        Pool<?> pool;
        if (byteBufferMode) {
            long bbPoolSize = (long) (maxCacheBlocks * 1.10) * CACHE_BLOCK_SIZE;
            pool = new ByteBufferPool(bbPoolSize, CACHE_BLOCK_SIZE);
            LOGGER
                .info(
                    "ByteBuffer mode enabled. Created ByteBufferPool: capacity={}blocks ({}MB)",
                    (int) (maxCacheBlocks * 1.10),
                    bbPoolSize / (1024 * 1024)
                );
        } else {
            MemorySegmentPool msPool = new MemorySegmentPool(reservedPoolSizeInBytes, CACHE_BLOCK_SIZE);
            long warmupBlocks = (long) (maxCacheBlocks * warmupPercentage);
            msPool.warmUp(warmupBlocks);
            LOGGER
                .info(
                    "MemorySegment mode. Created pool: sizeBytes={}, segments={}, warmed up {} blocks",
                    reservedPoolSizeInBytes,
                    maxBlocks,
                    warmupBlocks
                );
            pool = msPool;
        }

        // Calculate read-ahead queue size based on cache capacity
        // Pool constraint not needed since cache evictions automatically release pool memory
        int readAheadQueueSize = ReadAheadSizingPolicy.calculateQueueSize(maxCacheBlocks);
        LOGGER.info("Calculated read-ahead queue size={} (cache={} blocks)", readAheadQueueSize, maxCacheBlocks);

        // Initialize shared cache with removal listener and get its executor
        BlockCacheBuilder.CacheWithExecutor<RefCountedMemorySegment, RefCountedMemorySegment> cacheWithExecutor = BlockCacheBuilder
            .build(CACHE_INITIAL_SIZE, maxCacheBlocks);
        BlockCache<RefCountedMemorySegment> blockCache = cacheWithExecutor.getCache();
        com.github.benmanes.caffeine.cache.Cache<?, ?> sharedCaffeineCache = cacheWithExecutor.getCache().getCache();
        java.util.concurrent.ThreadPoolExecutor removalExecutor = cacheWithExecutor.getExecutor();
        LOGGER.info("Creating shared block cache with blocks={}", maxCacheBlocks);

        // Calculate worker threads using principled drain-time approach
        int threads = ReadAheadSizingPolicy.calculateWorkerThreads(readAheadQueueSize);

        AtomicInteger threadId = new AtomicInteger();
        ExecutorService readAheadExecutor = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "readahead-worker-" + threadId.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
        LOGGER.info("Creating shared read-ahead executor with threads={} (queue={})", threads, readAheadQueueSize);

        // Create shared read-ahead worker (node-wide, single queue)
        // Executor thread pool naturally limits concurrency - no need for separate maxRunners cap
        // BlockCache is passed per-request to support directory-specific loaders
        Worker sharedReadaheadWorker = new QueuingWorker(readAheadQueueSize, readAheadExecutor);
        LOGGER.info("Created shared read-ahead worker: queueSize={} executorThreads={}", readAheadQueueSize, threads);

        // Start telemetry
        TelemetryThread telemetry = new TelemetryThread(pool, blockCache);

        return new PoolResources(
            pool,
            blockCache,
            sharedCaffeineCache,
            byteBufferMode,
            maxCacheBlocks,
            readAheadQueueSize,
            sharedReadaheadWorker,
            telemetry,
            removalExecutor,
            readAheadExecutor
        );
    }
}

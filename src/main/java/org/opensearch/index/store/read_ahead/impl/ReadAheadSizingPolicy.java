/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE_POWER;

/**
 * Centralized sizing policy for read-ahead infrastructure.
 *
 * This class provides principled, capacity-based calculations for read-ahead
 * worker configuration, ensuring efficient prefetching without overwhelming
 * system resources.
 *
 *
 * Design principles:
 *  - Cache-driven speculation: Cache capacity defines the upper bound of useful
 *    prefetching. No point reading ahead more than the cache can hold.
 *  - Drain-time based threading: Thread count derived from target queue drain time
 *    (5 seconds) and assumed I/O latency (10ms), not CPU core count guesswork
 *  - CPU as safety cap: CPU cores provide upper bound to prevent oversubscription,
 *    but are not the primary sizing factor
 *
 *
 * @see WindowedReadAheadContext
 * @see QueuingWorker
 */
public final class ReadAheadSizingPolicy {

    /**
     * Average size of one read-ahead task in bytes.
     *
     * This represents the typical amount of data prefetched per task, used to
     * convert byte-based budgets into task counts. Default is 256 KiB, which
     * aligns with WindowedReadAheadContext.MAX_BLOCKS_PER_SUBMISSION (64 blocks * 4 KiB).
     *
     * Tuning: Increase for workloads with larger sequential access patterns.
     */
    public static final long AVG_READ_BYTES = 256L * 1024; // 256 KiB

    /**
     * Target time to drain a full queue in milliseconds.
     *
     * Worker threads are sized to drain the entire queue within this time window,
     * ensuring responsiveness. Default is 5 seconds, balancing between:
     * - Fast enough to keep up with bursty sequential access
     * - Slow enough to avoid excessive thread creation
     *
     * Tuning: Decrease for lower latency requirements, increase for batch workloads.
     */
    public static final int TARGET_DRAIN_MILLIS = 5_000;

    /**
     * Assumed average I/O latency per read-ahead task in milliseconds.
     *
     * This conservative estimate is used for thread pool sizing calculations.
     * Default is 10ms, representing typical SSD/NVMe latency for batched reads.
     * Used in combination with TARGET_DRAIN_MILLIS to determine worker thread count.
     *
     * Tuning: Can be adjusted based on storage characteristics:
     */
    public static final int AVG_READ_LATENCY_MILLIS = 10;

    /**
     * Fraction of cache capacity allowed to be queued speculatively.
     *
     * Queue size is calculated as cache_bytes / CACHE_QUEUE_FRACTION.
     * Default is 10 (i.e., 10% of cache), preventing read-ahead from dominating
     * cache with speculative data.
     *
     * Tuning:
     * - Lower (e.g., 20): More conservative, less cache churn
     * - Higher (e.g., 5): More aggressive prefetching for sequential workloads
     */
    public static final int CACHE_QUEUE_FRACTION = 10;

    /**
     * Minimum read-ahead queue size in tasks.
     *
     * Ensures minimum prefetch depth even on tiny caches. Default is 16 tasks
     * (4 MB worth of data at AVG_READ_BYTES).
     */
    public static final int MIN_QUEUE_TASKS = 16;

    /**
     * Maximum read-ahead queue size in tasks.
     *
     * Safety cap to prevent excessive memory usage and queue overhead.
     * Default is 4096 tasks (1 GB worth of data at AVG_READ_BYTES).
     */
    public static final int MAX_QUEUE_TASKS = 4096;

    /**
     * Minimum number of read-ahead worker threads.
     *
     * Ensures basic concurrency even with small queues or fast I/O.
     * Default is 2 threads.
     */
    public static final int MIN_THREADS = 2;

    /**
     * Maximum number of read-ahead worker threads.
     *
     * Safety cap to prevent excessive thread creation. Default is 64 threads.
     * Thread count is typically much lower due to CPU and drain-time constraints.
     */
    public static final int MAX_THREADS = 16;

    private ReadAheadSizingPolicy() {}

    /**
     * Calculates the global read-ahead queue size based on cache capacity.
     *
     * The queue is sized as a fraction of cache capacity (1/CACHE_QUEUE_FRACTION),
     * converted from bytes to tasks using AVG_READ_BYTES. This ensures read-ahead
     * doesn't dominate the cache! this is very important to prevent circular recycles
     * especially on memory constrained hardwares.
     *
     * @param maxCacheBlocks number of blocks the cache can hold
     * @return queue size (number of read-ahead tasks), bounded by MIN/MAX_QUEUE_TASKS
     */
    public static int calculateQueueSize(long maxCacheBlocks) {
        if (maxCacheBlocks <= 0) {
            return MIN_QUEUE_TASKS;
        }

        long cacheBytes = maxCacheBlocks << CACHE_BLOCK_SIZE_POWER;

        // Cache-based speculative budget = cache / N
        long cacheBudgetBytes = Math.max(AVG_READ_BYTES, cacheBytes / CACHE_QUEUE_FRACTION);
        long cacheBudgetTasks = cacheBudgetBytes / AVG_READ_BYTES;

        return (int) Math.max(MIN_QUEUE_TASKS, Math.min(MAX_QUEUE_TASKS, cacheBudgetTasks));
    }

    /**
     * Calculates the number of read-ahead worker threads based on drain-time requirements.
     *
     * Threads are sized so that a full queue can be drained within TARGET_DRAIN_MILLIS,
     * using AVG_READ_LATENCY_MILLIS as the assumed I/O time per task. This ensures the
     * read-ahead system can keep up with demand without excessive thread creation.
     *
     * @param queueSize number of queued tasks (from calculateQueueSize)
     * @return number of worker threads, bounded by MIN/MAX_THREADS and CPU capacity
     */
    public static int calculateWorkerThreads(int queueSize) {
        if (queueSize <= 0) {
            return MIN_THREADS;
        }

        // Calculate threads needed to drain queue within target time
        int queueBasedThreads = (int) Math.ceil((queueSize * (double) AVG_READ_LATENCY_MILLIS) / TARGET_DRAIN_MILLIS);

        // IO-heavy path: cap by CPU conservatively (quarter of cores)
        // This leaves CPU headroom for query execution and other work
        int cpuCap = Math.max(1, Runtime.getRuntime().availableProcessors() / 4);

        int threads = Math.min(queueBasedThreads, cpuCap);

        return Math.max(MIN_THREADS, Math.min(MAX_THREADS, threads));
    }
}

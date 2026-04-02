/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.opensearch.index.store.CryptoDirectoryFactory;

/**
 * Tracks prefetch deduplication state and statistics.
 * Encapsulates the in-flight dedup map, counters, and async executor for prefetch operations.
 *
 * @opensearch.internal
 */
public class PrefetchTracker {

    private final ConcurrentHashMap<BlockCacheKey, Boolean> inflight = new ConcurrentHashMap<>();
    private final AtomicInteger inflightCount = new AtomicInteger();
    private final Executor executor;

    private final LongAdder prefetchCalls = new LongAdder();
    private final LongAdder blocksRequested = new LongAdder();
    private final LongAdder blocksLoaded = new LongAdder();
    private final LongAdder blocksDeduped = new LongAdder();
    private final LongAdder blocksCacheHit = new LongAdder();
    private final LongAdder executeRejections = new LongAdder();
    private final LongAdder prefetchTimeNs = new LongAdder();

    private final int maxInflight;

    /**
     * Creates a prefetch tracker with the given async executor.
     *
     * @param executor the executor for async prefetch operations (must not be null)
     * @param maxInflight maximum in-flight prefetch tasks before dropping new submissions
     */
    public PrefetchTracker(Executor executor, int maxInflight) {
        this.executor = executor;
        this.maxInflight = maxInflight;
    }

    public PrefetchTracker(Executor executor) {
        this(executor, 10_000);
    }

    /**
     * Submits a task for async prefetch execution.
     * Drops the task if too many prefetch operations are already in-flight.
     *
     * @param task the runnable to execute
     */
    public void execute(Runnable task) {
        if (!CryptoDirectoryFactory.isPrefetchEnabled()) {
            return;
        }
        if (inflightCount.get() > maxInflight) {
            executeRejections.increment();
            return;
        }
        try {
            executor.execute(task);
        } catch (Exception e) {
            executeRejections.increment();
        }
    }

    /**
     * Attempts to mark a block as in-flight for prefetch.
     *
     * @param key the block cache key
     * @return true if newly added (should be loaded), false if already in-flight (deduped)
     */
    public boolean putIfAbsent(BlockCacheKey key) {
        if (inflight.putIfAbsent(key, Boolean.TRUE) == null) {
            inflightCount.incrementAndGet();
            return true;
        }
        blocksDeduped.increment();
        return false;
    }

    public void remove(BlockCacheKey key) {
        if (inflight.remove(key) != null) {
            inflightCount.decrementAndGet();
        }
    }

    public int size() {
        return inflightCount.get();
    }

    public boolean isEmpty() {
        return inflightCount.get() == 0;
    }

    public void clear() {
        inflight.clear();
    }

    public void recordPrefetchCall(long blockCount) {
        prefetchCalls.increment();
        blocksRequested.add(blockCount);
    }

    public void recordPrefetchTimeNs(long nanos) {
        prefetchTimeNs.add(nanos);
    }

    public void recordBlocksLoaded(long count) {
        blocksLoaded.add(count);
    }

    public void recordCacheHits(long count) {
        blocksCacheHit.add(count);
    }

    public String stats() {
        long calls = prefetchCalls.sum();
        long requested = blocksRequested.sum();
        long loaded = blocksLoaded.sum();
        long deduped = blocksDeduped.sum();
        long cacheHit = blocksCacheHit.sum();
        long rejections = executeRejections.sum();
        long timeMs = prefetchTimeNs.sum() / 1_000_000;
        double cacheHitRate = requested > 0 ? (100.0 * cacheHit / requested) : 0;
        double loadRatio = requested > 0 ? (100.0 * loaded / requested) : 0;
        return String
            .format(
                "Prefetch[calls=%d, requested=%d, loaded=%d, deduped=%d, cacheHit=%d, hitRatio=%.2f%%, loadRatio=%.2f%%, timeMs=%d, inflight=%d, rejections=%d]",
                calls,
                requested,
                loaded,
                deduped,
                cacheHit,
                cacheHitRate,
                loadRatio,
                timeMs,
                inflight.size(),
                rejections
            );
    }

    public long getCalls() {
        return prefetchCalls.sum();
    }

    public long getBlocksRequested() {
        return blocksRequested.sum();
    }

    public long getBlocksLoaded() {
        return blocksLoaded.sum();
    }

    public long getBlocksDeduped() {
        return blocksDeduped.sum();
    }

    public long getBlocksCacheHit() {
        return blocksCacheHit.sum();
    }

    public long getExecuteRejections() {
        return executeRejections.sum();
    }

    public long getPrefetchTimeNs() {
        return prefetchTimeNs.sum();
    }

    // Testing and benchmarking
    public void resetStats() {
        prefetchCalls.reset();
        blocksRequested.reset();
        blocksLoaded.reset();
        blocksDeduped.reset();
        blocksCacheHit.reset();
        executeRejections.reset();
        prefetchTimeNs.reset();
    }
}

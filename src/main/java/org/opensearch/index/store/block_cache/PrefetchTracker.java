/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks prefetch deduplication state and statistics.
 * Encapsulates the in-flight dedup map, counters, and async executor for prefetch operations.
 *
 * @opensearch.internal
 */
public class PrefetchTracker {

    private final ConcurrentHashMap<BlockCacheKey, Boolean> inflight = new ConcurrentHashMap<>();
    private final Executor executor;

    private final AtomicLong loadMissingBlocksCalls = new AtomicLong();
    private final AtomicLong blocksRequested = new AtomicLong();
    private final AtomicLong blocksLoaded = new AtomicLong();
    private final AtomicLong blocksDeduped = new AtomicLong();
    private final AtomicLong blocksCacheHit = new AtomicLong();

    /**
     * Creates a prefetch tracker with the given async executor.
     *
     * @param executor the executor for async prefetch operations (must not be null)
     */
    public PrefetchTracker(Executor executor) {
        this.executor = executor;
    }

    /**
     * Submits a task for async prefetch execution.
     *
     * @param task the runnable to execute
     */
    public void execute(Runnable task) {
        executor.execute(task);
    }

    /**
     * Attempts to mark a block as in-flight for prefetch.
     *
     * @param key the block cache key
     * @return true if newly added (should be loaded), false if already in-flight (deduped)
     */
    public boolean putIfAbsent(BlockCacheKey key) {
        if (inflight.putIfAbsent(key, Boolean.TRUE) == null) {
            return true;
        }
        blocksDeduped.incrementAndGet();
        return false;
    }

    public void remove(BlockCacheKey key) {
        inflight.remove(key);
    }

    public void removeByFile(Path normalizedPath) {
        inflight.keySet().removeIf(key -> key instanceof FileBlockCacheKey fk && fk.filePath().equals(normalizedPath));
    }

    public int size() {
        return inflight.size();
    }

    public boolean isEmpty() {
        return inflight.isEmpty();
    }

    public void clear() {
        inflight.clear();
    }

    public void recordLoadMissingBlocksCall(long blockCount) {
        loadMissingBlocksCalls.incrementAndGet();
        blocksRequested.addAndGet(blockCount);
    }

    public void recordBlocksLoaded(long count) {
        blocksLoaded.addAndGet(count);
    }

    public void recordCacheHits(long count) {
        blocksCacheHit.addAndGet(count);
    }

    public String stats() {
        long calls = loadMissingBlocksCalls.getAndSet(0);
        long requested = blocksRequested.getAndSet(0);
        long loaded = blocksLoaded.getAndSet(0);
        long deduped = blocksDeduped.getAndSet(0);
        long cacheHit = blocksCacheHit.getAndSet(0);
        double loadRatio = requested > 0 ? (100.0 * loaded / requested) : 0;
        return String
            .format(
                "Prefetch[calls=%d, requested=%d, loaded=%d, deduped=%d, cacheHit=%d, loadRatio=%.2f%%, inflight=%d]",
                calls,
                requested,
                loaded,
                deduped,
                cacheHit,
                loadRatio,
                inflight.size()
            );
    }

    public long getCalls() {
        return loadMissingBlocksCalls.get();
    }

    public long getBlocksRequested() {
        return blocksRequested.get();
    }

    public long getBlocksLoaded() {
        return blocksLoaded.get();
    }

    public long getBlocksDeduped() {
        return blocksDeduped.get();
    }

    public long getBlocksCacheHit() {
        return blocksCacheHit.get();
    }

    // Testing only
    public void resetStats() {
        loadMissingBlocksCalls.set(0);
        blocksRequested.set(0);
        blocksLoaded.set(0);
        blocksDeduped.set(0);
        blocksCacheHit.set(0);
    }
}

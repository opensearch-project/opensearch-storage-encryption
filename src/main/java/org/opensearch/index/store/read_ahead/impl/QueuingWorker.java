/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockLoader;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;
import org.opensearch.index.store.directio.DirectIOBlockCacheKey;
import org.opensearch.index.store.read_ahead.Worker;

/**
 * Async background worker for block readahead.
 *
 * - Deduplicates in-flight requests via BlockCacheKey
 * - Uses a queue to offload I/O to background threads
 * - Delegates caching logic to BlockCache.getOrLoad()
 */
public class QueuingWorker implements Worker {

    private static final Logger LOGGER = LogManager.getLogger(QueuingWorker.class);

    private record ReadAheadTask(BlockCacheKey key, int length) {
    }

    private final BlockingQueue<ReadAheadTask> queue;
    private final Set<BlockCacheKey> inFlight;
    private final ExecutorService executor;

    private final BlockCache<RefCountedMemorySegment> blockCache;
    private final BlockLoader<RefCountedMemorySegment> blockLoader;

    private volatile boolean closed = false;

    /**
     * @param queueCapacity max pending requests
     * @param threads       number of background worker threads
     * @param blockCache    shared block cache
     * @param blockLoader   loader used by blockCache.getOrLoad()
     * @param segmentSize   block/segment size
     */
    public QueuingWorker(
        int queueCapacity,
        int threads,
        BlockCache<RefCountedMemorySegment> blockCache,
        BlockLoader<RefCountedMemorySegment> blockLoader,
        int segmentSize
    ) {
        this.queue = new LinkedBlockingQueue<>(queueCapacity);
        this.inFlight = ConcurrentHashMap.newKeySet();
        this.executor = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "readahead-worker");
            t.setDaemon(true);
            return t;
        });
        this.blockCache = blockCache;
        this.blockLoader = blockLoader;

        for (int i = 0; i < threads; i++) {
            executor.submit(this::processLoop);
        }
    }

    /**
     * Schedule a block for asynchronous readahead if not already in flight.
     *
     * @param path   file path
     * @param offset aligned block offset
     * @param length block length
     * @return true if successfully queued or already scheduled
     */
    @Override
    public boolean schedule(Path path, long offset, int length) {
        if (closed) {
            LOGGER.debug("Schedule called on closed worker for path={} offset={}", path, offset);
            return false;
        }

        BlockCacheKey key = new DirectIOBlockCacheKey(path, offset);
        // Deduplicate in-flight requests
        if (!inFlight.add(key)) {
            return true; // already scheduled
        }

        ReadAheadTask task = new ReadAheadTask(key, length);

        // Non-blocking enqueue
        if (queue.offer(task)) {
            return true;
        } else {
            // Queue full -> cleanup in-flight marker
            LOGGER.warn("Queue full, dropping readahead task: path={} offset={}, queue size={}", path, offset, queue.size());
            inFlight.remove(key);
            return false;
        }
    }

    /**
     * Background processing loop.
     * Polls queue and prefetches blocks asynchronously.
     */
    private void processLoop() {
        LOGGER.trace("Starting readahead worker thread: {}", Thread.currentThread().getName());
        while (!closed) {
            try {
                ReadAheadTask task = queue.poll(1000, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }

                // acquires a segment from pool and loads to cache.
                try {
                    blockCache.getOrLoad(task.key(), task.length(), blockLoader);
                } catch (IOException e) {
                    LOGGER.warn("Failed to prefetch segment offset={} path={}", task.key().offset(), task.key().filePath(), e);
                } finally {
                    inFlight.remove(task.key());
                }

            } catch (InterruptedException e) {
                LOGGER.warn("Readahead worker thread interrupted: {}", Thread.currentThread().getName());
                Thread.currentThread().interrupt();
                return;
            }
        }
        LOGGER.info("Readahead worker thread exiting: {}", Thread.currentThread().getName());
    }

    /**
     * Cancel all pending requests for a given file path.
     */
    @Override
    public void cancel(Path path) {
        queue.removeIf(task -> task.key().filePath().equals(path));
        inFlight.removeIf(key -> key.filePath().equals(path));
    }

    @Override
    public boolean isRunning() {
        return !closed;
    }

    @Override
    public void close() {
        closed = true;
        executor.shutdownNow();
        queue.clear();
        inFlight.clear();
    }
}

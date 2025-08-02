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
import org.opensearch.index.store.read_ahead.ReadaheadWorker;

/**
 * Async background worker for block readahead.
 *
 * - Deduplicates in-flight requests via BlockCacheKey
 * - Uses a queue to offload I/O to background threads
 * - Delegates caching logic to BlockCache.getOrLoad()
 */
public class AsyncReadaheadWorker implements ReadaheadWorker {

    private static final Logger LOGGER = LogManager.getLogger(AsyncReadaheadWorker.class);

    private record ReadaheadTask(BlockCacheKey key, int length) {
    }

    private final BlockingQueue<ReadaheadTask> queue;
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
    public AsyncReadaheadWorker(
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
        if (closed)
            return false;

        BlockCacheKey key = new DirectIOBlockCacheKey(path, offset);

        // Deduplicate in-flight requests
        if (!inFlight.add(key)) {
            return true; // already scheduled
        }

        ReadaheadTask task = new ReadaheadTask(key, length);

        // Non-blocking enqueue
        if (queue.offer(task)) {
            return true;
        } else {
            // Queue full -> cleanup in-flight marker
            inFlight.remove(key);
            return false;
        }
    }

    /**
     * Background processing loop.
     * Polls queue and prefetches blocks asynchronously.
     */
    private void processLoop() {
        while (!closed) {
            try {
                ReadaheadTask task = queue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }

                try {
                    // Prefetch into block cache if not already loaded
                    blockCache.getOrLoad(task.key(), task.length(), blockLoader);
                } catch (IOException e) {
                    LOGGER.warn("Failed to prefetch segment offset={} path={}", task.key().offset(), task.key().filePath(), e);
                } finally {
                    inFlight.remove(task.key());
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
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

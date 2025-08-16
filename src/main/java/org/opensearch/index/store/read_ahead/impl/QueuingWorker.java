/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;
import org.opensearch.index.store.read_ahead.Worker;

public class QueuingWorker implements Worker {

    private static final Logger LOGGER = LogManager.getLogger(QueuingWorker.class);

    private static final class ReadAheadTask {
        final Path path;
        final long offset;
        final long blockCount;
        final long enqueuedNanos;
        long startNanos;
        long doneNanos;

        ReadAheadTask(Path path, long offset, long blockCount) {
            this.path = path;
            this.offset = offset;
            this.blockCount = blockCount;
            this.enqueuedNanos = System.nanoTime();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ReadAheadTask))
                return false;
            ReadAheadTask other = (ReadAheadTask) obj;
            return path.equals(other.path) && offset == other.offset && blockCount == other.blockCount;
        }

        @Override
        public int hashCode() {
            return path.hashCode() * 31 + Long.hashCode(offset) * 13 + Long.hashCode(blockCount);
        }
    }

    private final BlockingDeque<ReadAheadTask> queue;
    private final int queueCapacity;
    private final ExecutorService executor;
    private final Set<ReadAheadTask> inFlight;

    private final BlockCache<RefCountedMemorySegment> blockCache;
    private volatile boolean closed = false;

    private static final AtomicInteger WORKER_ID = new AtomicInteger();

    public QueuingWorker(int queueCapacity, int threads, BlockCache<RefCountedMemorySegment> blockCache) {
        this.queue = new LinkedBlockingDeque<>(queueCapacity);
        this.queueCapacity = queueCapacity;
        this.inFlight = ConcurrentHashMap.newKeySet();
        this.blockCache = blockCache;

        this.executor = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "readahead-worker-" + WORKER_ID.incrementAndGet());
            t.setDaemon(true);
            return t;
        });

        for (int i = 0; i < threads; i++) {
            executor.submit(this::processLoop);
        }
    }

    @Override
    public boolean schedule(Path path, long offset, long blockCount) {
        if (closed) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Schedule on closed worker path={} off={}", path, offset);
            }
            return false;
        }

        final ReadAheadTask task = new ReadAheadTask(path, offset, blockCount);
        if (!inFlight.add(task)) {
            return true; // Already queued
        }

        final boolean accepted = queue.offerLast(task);

        if (!accepted) {
            inFlight.remove(task);
            LOGGER.warn("Queue full, dropping task path={} off={} qsz={}/{}", path, offset, queue.size(), queueCapacity);
            return false;
        }

        if (LOGGER.isDebugEnabled()) {
            long length = blockCount << CACHE_BLOCK_SIZE_POWER;
            LOGGER
                .debug(
                    "RA_ENQ path={} off={} len={} blocks={} qsz={}/{} tns={}",
                    path,
                    offset,
                    length,
                    blockCount,
                    queue.size(),
                    queueCapacity,
                    task.enqueuedNanos
                );
        }
        return true;
    }

    private void processLoop() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Starting readahead worker thread: {}", Thread.currentThread().getName());
        }

        while (!closed) {
            try {
                ReadAheadTask task = queue.takeFirst();

                task.startNanos = System.nanoTime();

                // bulk load the block.
                blockCache.loadBulk(task.path, task.offset, task.blockCount);
                task.doneNanos = System.nanoTime();

                inFlight.remove(task);

                if (LOGGER.isDebugEnabled()) {
                    long iOms = (task.doneNanos - task.startNanos) / 1_000_000L;
                    long length = task.blockCount << CACHE_BLOCK_SIZE_POWER;
                    LOGGER
                        .debug(
                            "RA_IO_DONE_BULK path={}, off={} len={} blocks={} io_ms={} qsz={}",
                            task.path,
                            task.offset,
                            length,
                            task.blockCount,
                            iOms,
                            queue.size()
                        );
                }

            } catch (InterruptedException ie) {
                if (!closed) {
                    LOGGER.warn("Readahead worker thread interrupted: {}", Thread.currentThread().getName());
                }
                Thread.currentThread().interrupt();
                return;
            } catch (NoSuchFileException e) {
                LOGGER.debug("File not found during readahead", e);
            } catch (IOException | UncheckedIOException e) {
                LOGGER.warn("Failed to prefetch", e);
            }
        }

        LOGGER.info("Readahead worker thread exiting: {}", Thread.currentThread().getName());
    }

    @Override
    public void cancel(Path path) {
        queue.removeIf(task -> task.path.equals(path));
        inFlight.removeIf(task -> task.path.equals(path));
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

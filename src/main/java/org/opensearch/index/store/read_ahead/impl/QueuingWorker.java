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
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.read_ahead.Worker;

/**
 * Minimal, Linux-style asynchronous readahead worker with bounded concurrency.
 *
 * <p>This worker queues contiguous block ranges for background prefetch,
 * avoiding overlap and redundant requests using an in-flight set.
 * The cache layer handles skip logic and merging.
 *
 * <p>All operations are backpressured via a bounded queue. Tasks are processed
 * by at most {@code maxRunners} concurrent draining threads to prevent thread storms.
 */
public final class QueuingWorker implements Worker {

    private static final Logger LOGGER = LogManager.getLogger(QueuingWorker.class);

    /** Represents a queued readahead request. */
    private static final class Task {
        final Path path;
        final long offset;     // byte offset
        final long blockCount; // number of blocks
        final long enqueuedNanos;
        long startNanos;
        long doneNanos;

        Task(Path path, long offset, long blockCount) {
            this.path = path;
            this.offset = offset;
            this.blockCount = blockCount;
            this.enqueuedNanos = System.nanoTime();
        }

        long startBlock() {
            return offset >>> CACHE_BLOCK_SIZE_POWER;
        }

        long endBlock() {
            return startBlock() + blockCount;
        }

        @Override
        public int hashCode() {
            return path.hashCode() * 31 + Long.hashCode(offset) + Long.hashCode(blockCount);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Task t))
                return false;
            return path.equals(t.path) && offset == t.offset && blockCount == t.blockCount;
        }
    }

    private final BlockingDeque<Task> queue;
    private final int capacity;
    private final ExecutorService executor;
    private final Set<Task> inFlight;
    private final BlockCache<RefCountedMemorySegment> blockCache;

    private static final int LARGE_WINDOW_THRESHOLD = 512;
    private static final int DUP_WARN_THRESHOLD = 10;
    private static final int MAX_BULK_SIZE = 128; // Maximum blocks per I/O operation

    private final AtomicInteger duplicateCounter = new AtomicInteger();
    private final AtomicInteger activeRunners = new AtomicInteger(0);
    private final int maxRunners;
    private volatile boolean closed = false;

    /**
     * Creates a readahead worker with a bounded queue and limited parallel drainers.
     *
     * @param capacity   maximum queue size
     * @param maxRunners maximum concurrent drain threads
     * @param executor   shared executor service for running worker threads
     * @param blockCache block cache for loading blocks
     */
    public QueuingWorker(int capacity, int maxRunners, ExecutorService executor, BlockCache<RefCountedMemorySegment> blockCache) {
        this.queue = new LinkedBlockingDeque<>(capacity);
        this.capacity = capacity;
        this.executor = executor;
        this.blockCache = blockCache;
        this.inFlight = ConcurrentHashMap.newKeySet();
        this.maxRunners = Math.max(1, maxRunners);
        LOGGER.info("Readahead worker initialized capacity={} maxRunners={}", capacity, maxRunners);
    }

    @Override
    public boolean schedule(Path path, long offset, long blockCount) {
        if (closed) {
            LOGGER.debug("Attempted schedule on closed worker path={} off={} blocks={}", path, offset, blockCount);
            return false;
        }

        // Split large requests into chunks to avoid blocking executor threads.
        if (blockCount > MAX_BULK_SIZE) {
            boolean allAccepted = true;
            for (long i = 0; i < blockCount; i += MAX_BULK_SIZE) {
                long chunkSize = Math.min(MAX_BULK_SIZE, blockCount - i);
                long chunkOffset = offset + (i << CACHE_BLOCK_SIZE_POWER);
                if (!scheduleChunk(path, chunkOffset, chunkSize)) {
                    allAccepted = false;
                }
            }
            return allAccepted;
        }

        return scheduleChunk(path, offset, blockCount);
    }

    /**
     * Internal method to schedule a single chunk (not exceeding MAX_BULK_SIZE).
     */
    private boolean scheduleChunk(Path path, long offset, long blockCount) {
        final long blockStart = offset >>> CACHE_BLOCK_SIZE_POWER;
        final long blockEnd = blockStart + blockCount;

        if (blockCount <= 1) {
            LOGGER.trace("Tiny readahead request path={} off={} blocks={}", path, offset, blockCount);
        } else if (blockCount > LARGE_WINDOW_THRESHOLD) {
            LOGGER.warn("Large readahead request path={} off={} blocks={} (possible runaway window)", path, offset, blockCount);
        }

        // Overlap detection
        for (Task t : inFlight) {
            if (!t.path.equals(path))
                continue;
            if (Math.max(blockStart, t.startBlock()) < Math.min(blockEnd, t.endBlock())) {
                int dup = duplicateCounter.incrementAndGet();
                if (dup == DUP_WARN_THRESHOLD) {
                    LOGGER
                        .warn(
                            "Frequent duplicate readahead detected ({} overlaps so far). "
                                + "Scheduling may be too aggressive or window too small.",
                            dup
                        );
                }
                return true; // skip duplicate
            }
        }

        final Task task = new Task(path, offset, blockCount);
        if (!inFlight.add(task)) {
            LOGGER.trace("Task already in flight path={} off={} blocks={}", path, offset, blockCount);
            return true;
        }

        final boolean accepted = queue.offerLast(task);
        if (!accepted) {
            inFlight.remove(task);
            LOGGER
                .warn(
                    "Readahead queue full, dropping task path={} off={} blocks={} qsz={}/{}",
                    path,
                    offset,
                    blockCount,
                    queue.size(),
                    capacity
                );
            return false;
        }

        // Start drainer if below concurrency cap
        if (activeRunners.get() < maxRunners && activeRunners.incrementAndGet() <= maxRunners) {
            executor.submit(this::drainLoop);
        } else {
            // if we raced over cap, undo increment
            activeRunners.decrementAndGet();
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER
                .debug(
                    "RA_ENQ path={} blocks=[{}-{}) off={} len={}B blocks={} qsz={}/{} inflight={}",
                    path,
                    blockStart,
                    blockEnd,
                    offset,
                    blockCount << CACHE_BLOCK_SIZE_POWER,
                    blockCount,
                    queue.size(),
                    capacity,
                    inFlight.size()
                );
        }
        return true;
    }

    /** Drains tasks from the queue in batches. */
    private void drainLoop() {
        try {
            while (!closed) {
                final Task task = queue.pollFirst(100, TimeUnit.MILLISECONDS);
                if (task == null)
                    break; // queue empty
                processOne(task);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } finally {
            activeRunners.decrementAndGet();
            if (!closed && !queue.isEmpty() && activeRunners.get() < maxRunners) {
                if (activeRunners.incrementAndGet() <= maxRunners) {
                    executor.submit(this::drainLoop);
                }
            }
        }
    }

    /** Processes a single readahead task. */
    private void processOne(Task task) {
        try {
            task.startNanos = System.nanoTime();
            final long queueDelayNs = task.startNanos - task.enqueuedNanos;
            if (queueDelayNs > TimeUnit.MILLISECONDS.toNanos(50)) {
                LOGGER
                    .debug(
                        "High queue wait path={} wait_ms={} qsz={}/{} inflight={}",
                        task.path,
                        queueDelayNs / 1_000_000,
                        queue.size(),
                        capacity,
                        inFlight.size()
                    );
            }

            blockCache.loadBulk(task.path, task.offset, task.blockCount);

            task.doneNanos = System.nanoTime();
            inFlight.remove(task);

            final long ioMs = (task.doneNanos - task.startNanos) / 1_000_000;
            final long startBlock = task.startBlock();
            final long endBlock = task.endBlock();

            if (ioMs > 500) {
                LOGGER
                    .warn(
                        "Slow readahead I/O path={} blocks=[{}-{}) count={} took={}ms qsz={}/{} inflight={}",
                        task.path,
                        startBlock,
                        endBlock,
                        task.blockCount,
                        ioMs,
                        queue.size(),
                        capacity,
                        inFlight.size()
                    );
            }

            LOGGER
                .debug(
                    "RA_IO_DONE path={} blocks=[{}-{}) count={} off={} len={}B io_ms={} qsz={}/{} inflight={}",
                    task.path,
                    startBlock,
                    endBlock,
                    task.blockCount,
                    task.offset,
                    task.blockCount << CACHE_BLOCK_SIZE_POWER,
                    ioMs,
                    queue.size(),
                    capacity,
                    inFlight.size()
                );

        } catch (NoSuchFileException e) {
            inFlight.remove(task);
            LOGGER.debug("File not found during readahead path={}", task.path);
        } catch (IOException | UncheckedIOException e) {
            inFlight.remove(task);
            LOGGER.warn("Readahead failed path={} msg={}", task.path, e.getMessage(), e);
        } catch (RuntimeException e) {
            inFlight.remove(task);
            throw e;
        }
    }

    @Override
    public void cancel(Path path) {
        boolean qRemoved = queue.removeIf(t -> t.path.equals(path));
        boolean fRemoved = inFlight.removeIf(t -> t.path.equals(path));
        if (qRemoved || fRemoved) {
            LOGGER.debug("Cancelled readahead path={} removedQueued={} removedInFlight={}", path, qRemoved, fRemoved);
        }
    }

    @Override
    public boolean isRunning() {
        return !closed;
    }

    @Override
    public void close() {
        if (closed)
            return;
        closed = true;
        queue.clear();
        inFlight.clear();
    }
}

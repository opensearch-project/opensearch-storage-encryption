/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

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
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE_POWER;
import org.opensearch.index.store.read_ahead.Worker;

/**
 * Minimal, Linux-style asynchronous readahead worker with detailed logging.
 *
 * <p>This worker queues contiguous block ranges for background prefetch,
 * avoiding overlap and redundant requests using an in-flight set.
 * The cache layer is responsible for skip logic and gap merging.
 *
 * <p>All operations are non-blocking and backpressured by a bounded queue.
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

    private static final int DUP_WARN_THRESHOLD = 10;

    private final AtomicInteger duplicateCounter = new AtomicInteger();
    private volatile boolean closed = false;

    /** Creates worker with shared executor (avoids per-shard thread explosion) */
    public QueuingWorker(int capacity, ExecutorService sharedExecutor, BlockCache<RefCountedMemorySegment> blockCache) {
        this.queue = new LinkedBlockingDeque<>(capacity);
        this.capacity = capacity;
        this.blockCache = blockCache;
        this.inFlight = ConcurrentHashMap.newKeySet();
        this.executor = sharedExecutor;

        executor.submit(this::processLoop);
        LOGGER.debug("Readahead worker initialized capacity={} sharedExecutor=true", capacity);
    }

    @Override
    public boolean schedule(Path path, long offset, long blockCount) {
        if (closed) {
            LOGGER.debug("Attempted schedule on closed worker path={} off={} blocks={}", path, offset, blockCount);
            return false;
        }

        final long blockStart = offset >>> CACHE_BLOCK_SIZE_POWER;
        final long blockEnd = blockStart + blockCount;

        // Quick sanity diagnostics
        if (blockCount <= 1) {
            LOGGER.trace("Tiny readahead request path={} off={} blocks={}", path, offset, blockCount);
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

    /** Main loop that performs asynchronous prefetch. */
    private void processLoop() {
        LOGGER.trace("Started readahead thread: {}", Thread.currentThread().getName());
        while (!closed) {
            try {
                final Task task = queue.takeFirst();
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
                            "Slow readahead I/O path={} blocks=[{}-{}) took={}ms qsz={}/{} inflight={}",
                            task.path,
                            startBlock,
                            endBlock,
                            ioMs,
                            queue.size(),
                            capacity,
                            inFlight.size()
                        );
                }

                LOGGER
                    .debug(
                        "RA_IO_DONE path={} blocks=[{}-{}) off={} len={}B io_ms={} qsz={}/{} inflight={}",
                        task.path,
                        startBlock,
                        endBlock,
                        task.offset,
                        task.blockCount << CACHE_BLOCK_SIZE_POWER,
                        ioMs,
                        queue.size(),
                        capacity,
                        inFlight.size()
                    );

            } catch (InterruptedException ie) {
                if (!closed)
                    LOGGER.warn("Readahead thread interrupted: {}", Thread.currentThread().getName());
                Thread.currentThread().interrupt();
                break;
            } catch (NoSuchFileException e) {
                LOGGER.debug("File not found during readahead path={}", e.getMessage());
            } catch (IOException | UncheckedIOException e) {
                LOGGER.warn("Readahead failed path={} msg={}", e.getMessage(), e);
            }
        }
    }

    @Override
    public void cancel(Path path) {
        boolean qRemoved = queue.removeIf(t -> t.path.equals(path));
        boolean fRemoved = inFlight.removeIf(t -> t.path.equals(path));

        if (qRemoved || fRemoved) {
            LOGGER.debug("Cancelled readahead for path={} removedQueued={} removedInFlight={}", path, qRemoved, fRemoved);
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

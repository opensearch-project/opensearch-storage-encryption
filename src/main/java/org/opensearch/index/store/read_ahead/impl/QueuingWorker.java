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
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockLoader;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;
import org.opensearch.index.store.directio.DirectIOBlockCacheKey;
import org.opensearch.index.store.read_ahead.Worker;

/**
 * Minimal async worker that offloads prefetch I/O.
 *
 * Design:
 *  - Single global deque (FIFO-ish): FG enqueues at tail, BG consumes from head.
 *  - Per-path minUseful offset: anything at/before this is dropped (enqueue+dequeue).
 *  - In-flight de-dup via BlockCacheKey.
 *  - Delegates caching to BlockCache.getOrLoad().
 *
 * No: distance/near heuristics, soft/hard horizons, tail-drop gymnastics.
 * Keep it simple; add heuristics later only if the data demands it.
 */
public class QueuingWorker implements Worker {

    private static final Logger LOGGER = LogManager.getLogger(QueuingWorker.class);

    private static final class ReadAheadTask {
        final BlockCacheKey key;
        final int length;
        final long enqueuedNanos;
        long startNanos;
        long doneNanos;

        ReadAheadTask(BlockCacheKey key, int length) {
            this.key = key;
            this.length = length;
            this.enqueuedNanos = System.nanoTime();
        }
    }

    private final BlockingDeque<ReadAheadTask> queue;
    private final int queueCapacity;
    private final Set<BlockCacheKey> inFlight;
    private final ExecutorService executor;

    private final BlockCache<RefCountedMemorySegment> blockCache;
    private final BlockLoader<RefCountedMemorySegment> blockLoader;

    private volatile boolean closed = false;

    /** Per-path lower bound for useful data (byte offset, monotonic non-decreasing). */
    private final ConcurrentHashMap<Path, AtomicLong> minUsefulOffsetByPath = new ConcurrentHashMap<>();

    private static final AtomicInteger WORKER_ID = new AtomicInteger();

    public QueuingWorker(
        int queueCapacity,
        int threads,
        BlockCache<RefCountedMemorySegment> blockCache,
        BlockLoader<RefCountedMemorySegment> blockLoader,
        int segmentSize // kept for signature parity; unused by this minimal worker
    ) {
        this.queue = new LinkedBlockingDeque<>(queueCapacity);
        this.queueCapacity = queueCapacity;
        this.inFlight = ConcurrentHashMap.newKeySet();
        this.blockCache = blockCache;
        this.blockLoader = blockLoader;

        this.executor = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "readahead-worker-" + WORKER_ID.incrementAndGet());
            t.setDaemon(true);
            return t;
        });

        for (int i = 0; i < threads; i++) {
            executor.submit(this::processLoop);
        }
    }

    /** Back-compat: schedule without consumer context. */
    @Override
    public boolean schedule(Path path, long offset, int length) {
        if (closed) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Schedule on closed worker path={} off={}", path, offset);
            }
            return false;
        }

        final BlockCacheKey key = new DirectIOBlockCacheKey(path, offset);

        // Drop-behind at enqueue if FG already advanced past this block.
        final AtomicLong lb = minUsefulOffsetByPath.get(path);
        if (lb != null) {
            final long minOff = lb.get();
            if (offset + length <= minOff) {
                // Not useful anymore; don't enqueue.
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("DROP_BEHIND enqueue path={} off={} len={} minOff={}", path, offset, length, minOff);
                }
                return false;
            }
        }

        // De-duplicate in-flight.
        if (!inFlight.add(key)) {
            return true; // already queued or processing
        }

        final ReadAheadTask task = new ReadAheadTask(key, length);
        final boolean accepted = queue.offerLast(task);

        if (!accepted) {
            inFlight.remove(key);
            LOGGER.warn("Queue full, dropping task path={} off={} qsz={}/{}", path, offset, queue.size(), queueCapacity);
            return false;
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER
                .debug(
                    "RA_ENQ path={} off={} len={} qsz={}/{} tns={}",
                    path,
                    offset,
                    length,
                    queue.size(),
                    queueCapacity,
                    task.enqueuedNanos
                );
        }
        return true;
    }

    /** Update per-path lower bound. Call with the next useful offset (aligned or > last read). */
    public void updateMinUsefulOffset(Path path, long fileOffset) {
        minUsefulOffsetByPath.computeIfAbsent(path, p -> new AtomicLong(0L)).getAndUpdate(prev -> fileOffset > prev ? fileOffset : prev);
    }

    /** For FG fast-path logging decisions. */
    public boolean isInflight(BlockCacheKey key) {
        return inFlight.contains(key);
    }

    /** Remove tasks that are already behind FG. Cheap linear pass over current queue. */
    private void purgeStale() {
        queue.removeIf(task -> {
            final AtomicLong lb = minUsefulOffsetByPath.get(task.key.filePath());
            if (lb == null)
                return false;
            final long minOff = lb.get();
            final boolean stale = (task.key.offset() + task.length) <= minOff;
            if (stale) {
                inFlight.remove(task.key);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER
                        .trace(
                            "DROP_BEHIND purge path={} off={} len={} minOff={}",
                            task.key.filePath(),
                            task.key.offset(),
                            task.length,
                            minOff
                        );
                }
            }
            return stale;
        });
    }

    /** Background processing: purge-behind → take → drop-behind guard → load block. */
    private void processLoop() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Starting readahead worker thread: {}", Thread.currentThread().getName());
        }

        while (!closed) {
            try {
                // Opportunistic purge; cheap and keeps the queue tidy.
                purgeStale();

                // Block for work.
                ReadAheadTask task = queue.takeFirst();

                // Guard again on dequeue in case FG progressed while we waited.
                final AtomicLong lb = minUsefulOffsetByPath.get(task.key.filePath());
                if (lb != null) {
                    final long minOff = lb.get();
                    if (task.key.offset() + task.length <= minOff) {
                        inFlight.remove(task.key);
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER
                                .debug(
                                    "DROP_BEHIND dequeue path={} off={} len={} minOff={}",
                                    task.key.filePath(),
                                    task.key.offset(),
                                    task.length,
                                    minOff
                                );
                        }
                        continue;
                    }
                }

                final long now = System.nanoTime();
                final long waitUs = (now - task.enqueuedNanos) / 1_000L;
                task.startNanos = now;

                if (LOGGER.isDebugEnabled()) {
                    LOGGER
                        .debug(
                            "RA_IO_START thread={} key={} off={} len={} queue_wait_us={}",
                            Thread.currentThread().getName(),
                            task.key,
                            task.key.offset(),
                            task.length,
                            waitUs
                        );
                }

                try {
                    blockCache.getOrLoad(task.key, task.length, blockLoader);
                    task.doneNanos = System.nanoTime();
                    final long ioUs = (task.doneNanos - task.startNanos) / 1_000L;

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER
                            .debug(
                                "RA_IO_DONE key={}, off={} len={} io_us={} qsz={}",
                                task.key,
                                task.key.offset(),
                                task.length,
                                ioUs,
                                queue.size()
                            );
                        LOGGER.debug("Loaded block {} {}", task.key, task.length);
                    }

                } catch (UncheckedIOException e) {
                    if (e.getCause() instanceof NoSuchFileException) {
                        LOGGER.debug("File not found, purging all tasks for path={}: {}", task.key.filePath(), e.getCause().getMessage());
                        cancel(task.key.filePath());
                    } else {
                        LOGGER.warn("Failed to prefetch offset={} path={}", task.key.offset(), task.key.filePath(), e);
                    }
                } catch (IOException e) {
                    if (e instanceof NoSuchFileException) {
                        LOGGER.debug("File not found, purging all tasks for path={}: {}", task.key.filePath(), e.getMessage());
                        cancel(task.key.filePath());
                    } else {
                        LOGGER.warn("Failed to prefetch offset={} path={}", task.key.offset(), task.key.filePath(), e);
                    }
                } finally {
                    inFlight.remove(task.key);
                }

            } catch (InterruptedException ie) {
                if (!closed) {
                    LOGGER.warn("Readahead worker thread interrupted: {}", Thread.currentThread().getName());
                }
                Thread.currentThread().interrupt();
                return;
            }
        }

        LOGGER.info("Readahead worker thread exiting: {}", Thread.currentThread().getName());
    }

    @Override
    public void cancel(Path path) {
        queue.removeIf(task -> task.key.filePath().equals(path));
        inFlight.removeIf(key -> key.filePath().equals(path));
        minUsefulOffsetByPath.remove(path);
    }

    @Override
    public boolean isRunning() {
        return !closed;
    }

    @Override
    public void close() {
        closed = true;
        executor.shutdownNow(); // interrupts takeFirst()
        queue.clear();
        inFlight.clear();
        minUsefulOffsetByPath.clear();
    }
}

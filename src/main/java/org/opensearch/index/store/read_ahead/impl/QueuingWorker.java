/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE_POWER;

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
import org.opensearch.index.store.read_ahead.Worker;

/**
 * Minimal, Linux-style asynchronous readahead worker with bounded concurrency.
 *
 * <p>This worker queues contiguous block ranges for background prefetch,
 * avoiding overlap and redundant requests using an in-flight set.
 *
 * <p>Two safety protections:
 *  1) Worker saturation protection (queue pressure): avoid building backlog.
 *  2) Cache thrash protection (windowed miss-rate): if recent cache behavior is
 *     overwhelmingly misses AND worker is pressured, temporarily pause readahead
 *     to avoid evicting useful cache entries.
 */
public final class QueuingWorker implements Worker {

    private static final Logger LOGGER = LogManager.getLogger(QueuingWorker.class);

    /** Represents a queued readahead request. */
    private static final class Task {
        final org.opensearch.index.store.block_cache.BlockCache<?> blockCache;
        final Path path;
        final long offset;     // byte offset
        final long blockCount; // number of blocks
        final long enqueuedNanos;
        long startNanos;
        long doneNanos;

        Task(org.opensearch.index.store.block_cache.BlockCache<?> blockCache, Path path, long offset, long blockCount) {
            this.blockCache = blockCache;
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

    private static final int LARGE_WINDOW_THRESHOLD = 512;
    private static final int DUP_WARN_THRESHOLD = 10;
    private static final int MAX_BULK_SIZE = 128; // Maximum blocks per I/O operation

    private final AtomicInteger duplicateCounter = new AtomicInteger();
    private final AtomicInteger activeRunners = new AtomicInteger(0);
    private volatile boolean closed = false;

    // -------------------------
    // Node-wide readahead gate
    // -------------------------

    /**
     * "Paused" here means: temporarily avoid scheduling more speculative work
     * because the node is under pressure AND the cache is currently thrashing.
     *
     * We intentionally do NOT pause simply because hitRate is low; readahead is meant
     * to improve misses. We only pause on extreme miss-dominance (≥98% misses) in a recent
     * window, and only when the worker is already pressured (≥75% queue full).
     */
    private volatile boolean readAheadPaused = false;

    // Update gate every N completed tasks (cheap, time-free sampling)
    private static final int PAUSE_CHECK_INTERVAL = 256;

    // Warmup period: don't pause during first N tasks while cache fills
    private static final int WARMUP_TASKS = 512;

    // Only trust the sample window if enough cache ops happened since last sample
    private static final long MIN_SAMPLE_OPS = 20_000;

    // Pressure thresholds (hysteresis)
    private static final int PRESSURE_NUM = 3; // 75%
    private static final int PRESSURE_DEN = 4;
    private static final int LOW_Q_NUM = 1;    // 50%
    private static final int LOW_Q_DEN = 2;

    // Miss-rate thresholds (hysteresis to avoid flapping)
    private static final double PAUSE_MISS_RATE = 0.98;  // pause if >= 98% misses AND pressured
    private static final double RESUME_MISS_RATE = 0.90; // resume if <= 90% misses OR queue low

    // If we stay paused too long, force occasional re-evaluation / recovery (avoid getting stuck)
    private static final int MAX_CONSECUTIVE_PAUSE_WINDOWS = 8;
    private int consecutivePauseWindows = 0;

    // Sampling state (cache counters are cumulative; we compute deltas)
    private long lastHits = -1;
    private long lastMisses = -1;
    private int tasksSinceCheck = 0;
    private int totalTasksProcessed = 0;

    // Cache reference for stats (captured from first task)
    private volatile org.opensearch.index.store.block_cache.BlockCache<?> statsCache = null;

    /**
     * Creates a readahead worker with a bounded queue.
     *
     * @param capacity   maximum queue size
     * @param executor   shared executor service for running worker threads
     */
    public QueuingWorker(int capacity, ExecutorService executor) {
        this.queue = new LinkedBlockingDeque<>(capacity);
        this.capacity = capacity;
        this.executor = executor;
        this.inFlight = ConcurrentHashMap.newKeySet();
        LOGGER.debug("Readahead worker initialized capacity={}", capacity);
    }

    @Override
    public <T extends AutoCloseable> boolean schedule(
        org.opensearch.index.store.block_cache.BlockCache<T> blockCache,
        Path path,
        long offset,
        long blockCount
    ) {
        if (closed) {
            LOGGER.debug("Attempted schedule on closed worker path={} off={} blocks={}", path, offset, blockCount);
            return false;
        }

        // If node-wide gate is engaged, reject new scheduling quickly.
        // NOTE: This is a *soft* guard. WindowedReadAheadContext also checks isReadAheadPaused()
        // before waking the worker, so we avoid building backlog.
        if (readAheadPaused) {
            return false;
        }

        // Split large requests into chunks to avoid blocking executor threads.
        if (blockCount > MAX_BULK_SIZE) {
            boolean allAccepted = true;
            for (long i = 0; i < blockCount; i += MAX_BULK_SIZE) {
                long chunkSize = Math.min(MAX_BULK_SIZE, blockCount - i);
                long chunkOffset = offset + (i << CACHE_BLOCK_SIZE_POWER);
                if (!scheduleChunk(blockCache, path, chunkOffset, chunkSize)) {
                    allAccepted = false;
                }
            }
            return allAccepted;
        }

        return scheduleChunk(blockCache, path, offset, blockCount);
    }

    /** Internal method to schedule a single chunk (not exceeding MAX_BULK_SIZE). */
    private <T extends AutoCloseable> boolean scheduleChunk(
        org.opensearch.index.store.block_cache.BlockCache<T> blockCache,
        Path path,
        long offset,
        long blockCount
    ) {
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

        final Task task = new Task(blockCache, path, offset, blockCount);
        if (!inFlight.add(task)) {
            LOGGER.trace("Task already in flight path={} off={} blocks={}", path, offset, blockCount);
            return true;
        }

        final boolean accepted = queue.offerLast(task);
        if (!accepted) {
            inFlight.remove(task);
            LOGGER
                .trace(
                    "Readahead queue full, dropping task path={} off={} blocks={} qsz={}/{}",
                    path,
                    offset,
                    blockCount,
                    queue.size(),
                    capacity
                );
            return false;
        }

        // Start drainer - executor thread pool naturally limits concurrency
        activeRunners.incrementAndGet();
        executor.submit(this::drainLoop);

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
            // If queue still has work and we're not closed, submit another drainer
            if (!closed && !queue.isEmpty()) {
                activeRunners.incrementAndGet();
                executor.submit(this::drainLoop);
            }
        }
    }

    /** Processes a single readahead task. */
    private void processOne(Task task) {
        try {
            // Capture a stable cache ref for node-wide stats sampling (first seen wins)
            if (statsCache == null) {
                statsCache = task.blockCache;
            }

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

            task.blockCache.loadForPrefetch(task.path, task.offset, task.blockCount);

            task.doneNanos = System.nanoTime();
            inFlight.remove(task);

            // Track total tasks for warmup period
            totalTasksProcessed++;

            // Periodically update the node-wide gate.
            if (++tasksSinceCheck >= PAUSE_CHECK_INTERVAL) {
                tasksSinceCheck = 0;
                org.opensearch.index.store.block_cache.BlockCache<?> cache = statsCache;
                updateReadAheadGate(cache);
            }

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

    /**
     * Node-wide gate update:
     * - Only pause if the worker is pressured AND the recent cache miss-rate is extreme.
     * - Resume if queue recovers OR miss-rate improves.
     *
     * Requires BlockCache to expose cumulative hit/miss counters (typed), so we can compute deltas.
     */
    private void updateReadAheadGate(org.opensearch.index.store.block_cache.BlockCache<?> blockCache) {
        if (blockCache == null)
            return;
        try {
            final int cap = capacity;
            final int qsz = queue.size();

            final boolean pressured = cap > 0 && qsz > (cap * PRESSURE_NUM) / PRESSURE_DEN;
            final boolean queueLow = cap > 0 && qsz < (cap * LOW_Q_NUM) / LOW_Q_DEN;

            // If queue is low, always unpause (fast recovery).
            if (readAheadPaused && queueLow) {
                readAheadPaused = false;
                consecutivePauseWindows = 0;
                LOGGER.info("RA_RESUMED: queue recovered qsz={}/{}", qsz, cap);
                // Reset sampling so we don't immediately re-pause on stale deltas.
                lastHits = -1;
                lastMisses = -1;
                return;
            }

            final long hits = blockCache.hitCount();
            final long misses = blockCache.missCount();

            if (lastHits < 0 || lastMisses < 0) {
                lastHits = hits;
                lastMisses = misses;
                return;
            }

            final long dh = hits - lastHits;
            final long dm = misses - lastMisses;
            lastHits = hits;
            lastMisses = misses;

            final long ops = dh + dm;
            if (ops < MIN_SAMPLE_OPS) {
                // Too little signal. If we've been paused for too long, give it a chance to recover.
                if (readAheadPaused && ++consecutivePauseWindows >= MAX_CONSECUTIVE_PAUSE_WINDOWS) {
                    readAheadPaused = false;
                    consecutivePauseWindows = 0;
                    LOGGER.info("RA_RESUMED: forced periodic recovery (low sample) qsz={}/{}", qsz, cap);
                    lastHits = -1;
                    lastMisses = -1;
                }
                return;
            }

            final double missRate = (double) dm / (double) ops;

            // Warmup: Don't pause during first N tasks while cache is filling
            if (totalTasksProcessed < WARMUP_TASKS) {
                if (readAheadPaused) {
                    readAheadPaused = false;
                    consecutivePauseWindows = 0;
                    LOGGER.info("RA_RESUMED: warmup period tasks={}/{}", totalTasksProcessed, WARMUP_TASKS);
                }
                // Reset sampling to avoid stale deltas carrying into first real decision window
                lastHits = -1;
                lastMisses = -1;
                return;
            }

            if (!readAheadPaused) {
                // Thrash protection: only pause when pressured + extreme miss rate.
                if (pressured && missRate >= PAUSE_MISS_RATE) {
                    readAheadPaused = true;
                    consecutivePauseWindows = 0;
                    LOGGER.info("RA_PAUSED: pressured + high missRate missRate={} (dm={}, ops={}) qsz={}/{}", missRate, dm, ops, qsz, cap);
                }
                return;
            }

            // Already paused: resume if miss-rate improves OR queue recovers (hysteresis to avoid flapping).
            if (missRate <= RESUME_MISS_RATE) {
                readAheadPaused = false;
                consecutivePauseWindows = 0;
                LOGGER.info("RA_RESUMED: missRate improved missRate={} (dm={}, ops={}) qsz={}/{}", missRate, dm, ops, qsz, cap);
                lastHits = -1;
                lastMisses = -1;
            } else {
                consecutivePauseWindows++;
                if (consecutivePauseWindows >= MAX_CONSECUTIVE_PAUSE_WINDOWS) {
                    // Avoid indefinite pause if signals are noisy/stuck.
                    readAheadPaused = false;
                    consecutivePauseWindows = 0;
                    LOGGER.info("RA_RESUMED: forced periodic recovery qsz={}/{}", qsz, cap);
                    lastHits = -1;
                    lastMisses = -1;
                }
            }
        } catch (Exception e) {
            LOGGER.debug("Failed to update readahead gate", e);
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
    public int getQueueSize() {
        return queue.size();
    }

    @Override
    public int getQueueCapacity() {
        return capacity;
    }

    @Override
    public boolean isReadAheadPaused() {
        return readAheadPaused;
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

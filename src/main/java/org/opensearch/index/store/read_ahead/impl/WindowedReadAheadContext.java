/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE_POWER;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadPolicy;
import org.opensearch.index.store.read_ahead.Worker;

/**
 * Best-effort readahead context. The principles are inspired by kernel and RocksDB.
 *
 * Principles:
 *  - onAccess() must be extremely cheap (no nanoTime, no queue introspection, no CAS loops for max updates, no logging)
 *  - miss-driven: if there are no cache misses, we do essentially nothing
 *  - bail early if scheduling is not possible (throttle / pause) without building backlog
 *  - skip-and-forget: if scheduling is rejected, drop pending speculative work and move forward
 *  - bounded submissions: never submit huge bursts
 *  - idempotent wakeups: never storm the worker callback from search threads
 */
public class WindowedReadAheadContext implements ReadaheadContext {
    private static final Logger LOGGER = LogManager.getLogger(WindowedReadAheadContext.class);

    private final Path path;
    private final long lastFileSeg;
    private final Worker worker;
    private final org.opensearch.index.store.block_cache.BlockCache<? extends AutoCloseable> blockCache;
    private final WindowedReadaheadPolicy policy;
    private final Runnable signalCallback;

    private static final int MISS_BATCH = 8;

    private static final int MISS_STREAK_PAUSE_THRESHOLD = 64;
    private static final int MISS_STREAK_PAUSE_SKIPS = 8;

    private static final long MAX_BLOCKS_PER_SUBMISSION = 64;

    // 75% of queue capacity checks.
    private static final int QUEUE_PRESSURE_NUM = 3;
    private static final int QUEUE_PRESSURE_DEN = 4;

    // RA_STATS: log only from worker thread, sampled.
    private static final int STATS_EVERY_N_PROCESS_CALLS = 1024;

    /**
     * Wake threshold (in blocks): don't wake the worker unless we have accumulated
     * a meaningful amount of queuedDelta. This reduces wakeups/processQueue calls
     * that don't lead to any scheduling.
     *
     * Keep it time-free and cheap. We compute it in-line using leadBlocks().
     */
    private static long wakeMinDeltaBlocks(int leadBlocks) {
        // Enough to amortize wakeups but still responsive.
        // leadBlocks is often small (e.g., 1..8), so bound by MAX_BLOCKS_PER_SUBMISSION.
        return Math.min(MAX_BLOCKS_PER_SUBMISSION, Math.max(8, leadBlocks * 2L));
    }

    private volatile long desiredEndBlock = 0;
    private volatile long lastScheduledEndBlock = 0;

    private int missCount = 0;

    // Concurrency fix: volatile long avoids potential tearing and improves visibility.
    private volatile long missMaxBlock = -1;

    private int consecutiveMisses = 0;

    private int throttleSkipCount = 0;

    private int wakeupFlag = 0;

    private volatile boolean isClosed = false;

    // Best-effort stats (updated from multiple threads; correctness not required)
    private long accessMisses = 0;
    private long pauseEvents = 0;
    private long throttleSkips = 0;
    private long wakeups = 0;

    private long processCalls = 0;
    private long pressureSkips = 0;
    private long scheduleAttempts = 0;
    private long scheduleAccepted = 0;
    private long scheduleRejected = 0;
    private long blocksAttempted = 0;
    private long blocksAccepted = 0;

    private static final VarHandle THROTTLE_SKIP_VH;
    private static final VarHandle WAKEUP_VH;

    static {
        try {
            THROTTLE_SKIP_VH = MethodHandles.lookup().findVarHandle(WindowedReadAheadContext.class, "throttleSkipCount", int.class);
            WAKEUP_VH = MethodHandles.lookup().findVarHandle(WindowedReadAheadContext.class, "wakeupFlag", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private WindowedReadAheadContext(
        Path path,
        long fileLength,
        Worker worker,
        org.opensearch.index.store.block_cache.BlockCache<? extends AutoCloseable> blockCache,
        WindowedReadaheadPolicy policy,
        Runnable signalCallback
    ) {
        this.path = path;
        this.worker = worker;
        this.blockCache = blockCache;
        this.policy = policy;
        this.signalCallback = signalCallback;
        this.lastFileSeg = Math.max(0L, (fileLength - 1) >>> CACHE_BLOCK_SIZE_POWER);
    }

    public static WindowedReadAheadContext build(
        Path path,
        long fileLength,
        Worker worker,
        org.opensearch.index.store.block_cache.BlockCache<? extends AutoCloseable> blockCache,
        WindowedReadAheadConfig config,
        Runnable signalCallback
    ) {
        var policy = new WindowedReadaheadPolicy(path, config.initialWindow(), config.maxWindowSegments(), config.randomAccessThreshold());
        return new WindowedReadAheadContext(path, fileLength, worker, blockCache, policy, signalCallback);
    }

    /**
     * Hot path. has to be super cheap!
     *
     */
    @Override
    public void onAccess(long blockOffset, boolean wasHit) {
        if (isClosed) {
            return;
        }

        if (wasHit) {
            consecutiveMisses = 0;
            return;
        }

        accessMisses++;

        final long currBlock = blockOffset >>> CACHE_BLOCK_SIZE_POWER;

        if (++consecutiveMisses >= MISS_STREAK_PAUSE_THRESHOLD) {
            pauseEvents++;

            throttleSetAtLeast(MISS_STREAK_PAUSE_SKIPS);
            desiredEndBlock = lastScheduledEndBlock;

            consecutiveMisses = 0;
            missCount = 0;
            missMaxBlock = -1;
            return;
        }

        if (++missCount == 1) {
            missMaxBlock = currBlock;
        } else if (currBlock > missMaxBlock) {
            missMaxBlock = currBlock;
        }

        if (missCount < MISS_BATCH) {
            return;
        }

        missCount = 0;
        final long maxBlock = missMaxBlock;
        missMaxBlock = -1;

        if (throttleTryDecrement()) {
            throttleSkips++;
            desiredEndBlock = lastScheduledEndBlock;
            return;
        }

        // Extend desired tail (best-effort monotonic volatile write; no CAS loop).
        final long target = Math.min(maxBlock + policy.leadBlocks(), lastFileSeg + 1);

        // Only wake if we actually extended desired.
        final long prevDesired = desiredEndBlock;
        if (target > prevDesired) {
            desiredEndBlock = target;

            // Wake only if there's meaningful queued work.
            final long delta = target - lastScheduledEndBlock;
            if (delta >= wakeMinDeltaBlocks(policy.leadBlocks())) {
                maybeWakeWorkerOnce();
            }
        }
    }

    @Override
    public boolean processQueue() {
        if (isClosed) {
            return false;
        }

        processCalls++;

        final long scheduled = lastScheduledEndBlock;
        long desired = desiredEndBlock;

        // If there's no work, clear the wakeup gate and return.
        if (desired <= scheduled) {
            WAKEUP_VH.setRelease(this, 0);
            maybeLogStats();
            return false;
        }

        final int cap = worker.getQueueCapacity();
        if (cap > 0) {
            final int q = worker.getQueueSize();
            if (q > (cap * QUEUE_PRESSURE_NUM) / QUEUE_PRESSURE_DEN) {
                pressureSkips++;

                policy.onQueuePressureMedium();
                desiredEndBlock = lastScheduledEndBlock;
                throttleSetAtLeast(1);

                // We dropped backlog => allow future wakeups.
                WAKEUP_VH.setRelease(this, 0);
                maybeLogStats();
                return false;
            }
        }

        final long endExclusiveRaw = Math.min(desired, lastFileSeg + 1);
        final long endExclusive = Math.min(endExclusiveRaw, scheduled + MAX_BLOCKS_PER_SUBMISSION);

        final long blockCount = endExclusive - scheduled;
        if (blockCount <= 0) {
            // Nothing schedulable: allow future wakeups.
            WAKEUP_VH.setRelease(this, 0);
            maybeLogStats();
            return false;
        }

        scheduleAttempts++;
        blocksAttempted += blockCount;

        final long anchorOffset = scheduled << CACHE_BLOCK_SIZE_POWER;
        final boolean accepted = worker.schedule(blockCache, path, anchorOffset, blockCount);

        if (accepted) {
            scheduleAccepted++;
            blocksAccepted += blockCount;

            lastScheduledEndBlock = endExclusive;
            throttleDecreaseOnSuccess();

            // IMPORTANT: do NOT clear wakeupFlag here if more work remains.
            // One wake should drain multiple schedule() chunks.
            final long newDesired = desiredEndBlock;
            if (newDesired <= lastScheduledEndBlock) {
                WAKEUP_VH.setRelease(this, 0);
            }
            maybeLogStats();
            return true;
        }

        scheduleRejected++;

        // Rejected: drop backlog and allow future wakeups.
        desiredEndBlock = lastScheduledEndBlock;
        throttleIncreaseOnReject();
        policy.onQueueSaturated();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "RA_REJECT path={} blocks={} window={} lead={} throttleSkip={}",
                path,
                blockCount,
                policy.currentWindow(),
                policy.leadBlocks(),
                throttleGet()
            );
        }

        WAKEUP_VH.setRelease(this, 0);
        maybeLogStats();
        return false;
    }

    private void maybeLogStats() {
        if (LOGGER.isInfoEnabled() == false) {
            return;
        }
        if ((processCalls % STATS_EVERY_N_PROCESS_CALLS) != 0) {
            return;
        }

        int q = -1;
        int cap = -1;
        try {
            cap = worker.getQueueCapacity();
            q = worker.getQueueSize();
        } catch (Exception ignored) {
        }

        final long desired = desiredEndBlock;
        final long tail = lastScheduledEndBlock;
        final long queuedDelta = desired - tail;

        final long emptyProcess = processCalls - scheduleAttempts;
        final long avgBlocksPerOk = scheduleAccepted == 0 ? 0 : (blocksAccepted / scheduleAccepted);

        LOGGER.info(
            "RA_STATS path={} missAccess={} desired={} scheduledTail={} queuedDelta={} window={} lead={} " +
            "pause={} throttleSkips={} wakeups={} " +
            "processCalls={} emptyProcess={} pressureSkips={} " +
            "schedule(attempt={}, ok={}, reject={}) blocks(attempted={}, ok={}, avgOk={}) throttleSkip={} q={}/{}",
            path,
            accessMisses,
            desired,
            tail,
            queuedDelta,
            policy.currentWindow(),
            policy.leadBlocks(),
            pauseEvents,
            throttleSkips,
            wakeups,
            processCalls,
            emptyProcess,
            pressureSkips,
            scheduleAttempts,
            scheduleAccepted,
            scheduleRejected,
            blocksAttempted,
            blocksAccepted,
            avgBlocksPerOk,
            throttleGet(),
            q,
            cap
        );
    }

    @Override
    public boolean hasQueuedWork() {
        return desiredEndBlock > lastScheduledEndBlock;
    }

    @Override
    public ReadaheadPolicy policy() {
        return policy;
    }

    @Override
    public void triggerReadahead(long fileOffset) {
        if (isClosed) {
            return;
        }

        final long start = fileOffset >>> CACHE_BLOCK_SIZE_POWER;
        final long target = Math.min(start + policy.currentWindow(), lastFileSeg + 1);
        final long prev = desiredEndBlock;
        if (target > prev) {
            desiredEndBlock = target;

            final long delta = target - lastScheduledEndBlock;
            if (delta >= wakeMinDeltaBlocks(policy.leadBlocks())) {
                maybeWakeWorkerOnce();
            }
        }
    }

    @Override
    public void reset() {
        desiredEndBlock = lastScheduledEndBlock;
        missCount = 0;
        missMaxBlock = -1;
        consecutiveMisses = 0;
    }

    @Override
    public void cancel() {
        if (worker != null) {
            worker.cancel(path);
        }
    }

    @Override
    public boolean isReadAheadEnabled() {
        return !isClosed;
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        isClosed = true;
        cancel();
        // Allow flag reset (not strictly needed, but avoids weirdness if reused in tests)
        WAKEUP_VH.setRelease(this, 0);
    }

    private void maybeWakeWorkerOnce() {
        if (signalCallback == null) {
            return;
        }
        if ((int) WAKEUP_VH.compareAndExchange(this, 0, 1) == 0) {
            wakeups++;
            signalCallback.run();
        }
    }

    private int throttleGet() {
        return (int) THROTTLE_SKIP_VH.getVolatile(this);
    }

    private boolean throttleTryDecrement() {
        int prev;
        do {
            prev = (int) THROTTLE_SKIP_VH.getVolatile(this);
            if (prev <= 0) {
                return false;
            }
        } while (!THROTTLE_SKIP_VH.compareAndSet(this, prev, prev - 1));
        return true;
    }

    private void throttleDecreaseOnSuccess() {
        int prev;
        do {
            prev = (int) THROTTLE_SKIP_VH.getVolatile(this);
            if (prev <= 0) {
                return;
            }
        } while (!THROTTLE_SKIP_VH.compareAndSet(this, prev, prev - 1));
    }

    private void throttleIncreaseOnReject() {
        int prev, next;
        do {
            prev = (int) THROTTLE_SKIP_VH.getVolatile(this);
            next = Math.min(64, Math.max(4, prev == 0 ? 4 : prev * 2));
        } while (!THROTTLE_SKIP_VH.compareAndSet(this, prev, next));
    }

    private void throttleSetAtLeast(int minSkips) {
        int prev;
        do {
            prev = (int) THROTTLE_SKIP_VH.getVolatile(this);
            if (prev >= minSkips) {
                return;
            }
        } while (!THROTTLE_SKIP_VH.compareAndSet(this, prev, minSkips));
    }
}

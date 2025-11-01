/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE_POWER;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadPolicy;
import org.opensearch.index.store.read_ahead.Worker;

/**
 * Handles cache hits/misses, batches readahead requests, rate-limits worker wakeups.
 *
 * Hot path: no atomics, best-effort counters.
 * Delegates pattern tracking to Policy, manages I/O scheduling.
 */
public class WindowedReadAheadContext implements ReadaheadContext {
    private static final Logger LOGGER = LogManager.getLogger(WindowedReadAheadContext.class);

    private final Path path;
    private final long lastFileSeg;
    private final Worker worker;
    private final WindowedReadaheadPolicy policy;
    private final Thread processingThread;

    // ---- Adaptive thresholds ----
    private static final int HIT_NOP_THRESHOLD_BASE = 8;
    private static final int HIT_NOP_THRESHOLD_MAX = 512;
    private int hitNopThreshold = HIT_NOP_THRESHOLD_BASE;

    private static final int MISS_BATCH = 3;                   // batch N misses before triggering
    private static final long MIN_UNPARK_INTERVAL_NS = 300_000L; // 300µs min wake interval
    private static final long MERGE_SPIN_NS = 60_000L;         // 60µs worker spin to coalesce

    // ---- Shared state ----
    private volatile long desiredEndBlock = 0;
    private volatile long lastScheduledEndBlock = 0;

    // ---- Reader-local, non-volatile ----
    private int consecutiveHits = 0;
    private int missCount = 0;
    private long missMaxBlock = -1;

    private volatile long lastUnparkNanos = 0;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private static final VarHandle DESIRED_END_VH;
    static {
        try {
            DESIRED_END_VH = MethodHandles.lookup().findVarHandle(WindowedReadAheadContext.class, "desiredEndBlock", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private WindowedReadAheadContext(Path path, long fileLength, Worker worker, WindowedReadaheadPolicy policy, Thread processingThread) {
        this.path = path;
        this.worker = worker;
        this.policy = policy;
        this.processingThread = processingThread;
        this.lastFileSeg = Math.max(0L, (fileLength - 1) >>> CACHE_BLOCK_SIZE_POWER);
    }

    /** Factory */
    public static WindowedReadAheadContext build(
        Path path,
        long fileLength,
        Worker worker,
        WindowedReadAheadConfig config,
        Thread processingThread
    ) {
        var policy = new WindowedReadaheadPolicy(path, config.initialWindow(), config.maxWindowSegments(), config.randomAccessThreshold());
        return new WindowedReadAheadContext(path, fileLength, worker, policy, processingThread);
    }

    // hot path. keep it always optimized and fast
    @Override
    public void onAccess(long blockOffset, boolean wasHit) {
        final long currBlock = blockOffset >>> CACHE_BLOCK_SIZE_POWER;

        if (wasHit) {
            handleCacheHit(currBlock);
            return;
        }
        handleCacheMiss(currBlock);
    }

    private void handleCacheHit(long currBlock) {
        if (++consecutiveHits < hitNopThreshold)
            return;

        guardedExtend(currBlock);

        // exponentially back off hit window to reduce wakeups
        if (hitNopThreshold < HIT_NOP_THRESHOLD_MAX)
            hitNopThreshold <<= 1;

        consecutiveHits = 0;
    }

    private void handleCacheMiss(long currBlock) {
        consecutiveHits = 0;
        hitNopThreshold = HIT_NOP_THRESHOLD_BASE;

        missCount++;
        if (currBlock > missMaxBlock)
            missMaxBlock = currBlock;

        final int window = Math.max(1, policy.currentWindow());
        final boolean enoughMisses = missCount >= MISS_BATCH;
        final boolean farAhead = (missMaxBlock - lastScheduledEndBlock) >= Math.max(1, window / 4);

        if (enoughMisses || farAhead) {
            handleSequentialMiss();
        } else if (currBlock < lastScheduledEndBlock - window) {
            handleRandomMiss();
        }
    }

    /** Sequential miss: extend readahead window and schedule */
    private void handleSequentialMiss() {
        final long target = Math.min(missMaxBlock + policy.leadBlocks(), lastFileSeg + 1);
        extendDesiredTo(target);
        missCount = 0;
        missMaxBlock = -1;
        maybeUnpark();
    }

    /** Random or backward miss: cancel pending readahead */
    private void handleRandomMiss() {
        desiredEndBlock = lastScheduledEndBlock;
        missCount = 0;
        missMaxBlock = -1;
    }

    /** Extend readahead if we are close to scheduled tail */
    private void guardedExtend(long currBlock) {
        final long scheduledEnd = lastScheduledEndBlock;
        if (scheduledEnd <= 0)
            return;
        final long guardStart = Math.max(0, scheduledEnd - policy.leadBlocks());
        if (currBlock >= guardStart && currBlock < scheduledEnd) {
            final long newEnd = Math.min(currBlock + policy.leadBlocks(), lastFileSeg + 1);
            extendDesiredTo(newEnd);
            maybeUnpark();
        }
    }

    @Override
    public boolean processQueue() {
        if (closed.get())
            return false;

        long desired = desiredEndBlock;
        final long scheduled = lastScheduledEndBlock;
        if (desired <= scheduled)
            return false;

        // brief spin to absorb recent updates
        final long spinUntil = System.nanoTime() + MERGE_SPIN_NS;
        while (System.nanoTime() < spinUntil) {
            long d2 = desiredEndBlock;
            if (d2 <= desired)
                break;
            desired = d2;
            Thread.onSpinWait();
        }

        final long startSeg = scheduled;
        final long endExclusive = Math.min(desired, lastFileSeg + 1);
        final long blockCount = endExclusive - startSeg;
        if (blockCount < 1)
            return false;

        final long anchorOffset = startSeg << CACHE_BLOCK_SIZE_POWER;
        boolean accepted = worker.schedule(path, anchorOffset, blockCount);
        if (accepted)
            lastScheduledEndBlock = endExclusive;

        if (LOGGER.isDebugEnabled()) {
            LOGGER
                .debug(
                    "RA_TRIGGER path={} startSeg={} endExclusive={} blocks={} accepted={} desired={} watermark={}",
                    path,
                    startSeg,
                    endExclusive,
                    blockCount,
                    accepted,
                    desired,
                    lastScheduledEndBlock
                );
        }
        return accepted;
    }

    private void extendDesiredTo(long newEndExclusive) {
        long prev;
        do {
            prev = desiredEndBlock;
            if (newEndExclusive <= prev)
                return;
        } while (!DESIRED_END_VH.weakCompareAndSetRelease(this, prev, newEndExclusive));
    }

    private void maybeUnpark() {
        final long delta = desiredEndBlock - lastScheduledEndBlock;
        if (delta <= 0)
            return;

        final int w = Math.max(1, policy.currentWindow());
        final int minBatch = Math.max(16, Math.max(policy.leadBlocks(), w / 2));
        if (delta < minBatch)
            return;

        final long now = System.nanoTime();
        if (now - lastUnparkNanos < MIN_UNPARK_INTERVAL_NS)
            return;

        lastUnparkNanos = now;
        LockSupport.unpark(processingThread);
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
        final long start = fileOffset >>> CACHE_BLOCK_SIZE_POWER;
        extendDesiredTo(Math.min(start + policy.currentWindow(), lastFileSeg + 1));
        maybeUnpark();
    }

    @Override
    public void reset() {
        desiredEndBlock = lastScheduledEndBlock;
        missCount = 0;
        missMaxBlock = -1;
        consecutiveHits = 0;
    }

    @Override
    public void cancel() {
        if (worker != null)
            worker.cancel(path);
    }

    @Override
    public boolean isReadAheadEnabled() {
        return !closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true))
            cancel();
    }
}

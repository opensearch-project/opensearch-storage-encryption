/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE_POWER;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadPolicy;
import org.opensearch.index.store.read_ahead.Worker;

/**
 * Batching, low-overhead readahead context for sequential access patterns.
 *
 * <p>Design:
 * <ul>
 *   <li>Hot path (onAccess) has no per-access atomics or fences.</li>
 *   <li>Hits are suppressed and only extend when near the scheduled tail (guard band).</li>
 *   <li>Misses are batched; we extend once per batch and rate-limit unparks.</li>
 *   <li>Worker always schedules a single contiguous span from the tail.</li>
 * </ul>
 *
 * <p>Threading assumptions:
 * <ul>
 *   <li>Typically one context per IndexInput clone (single reader thread).</li>
 *   <li>If multiple reader threads ever share this context, {@link #extendDesiredTo(long)}
 *       uses an atomic max via VarHandle to remain correct.</li>
 * </ul>
 */
public class WindowedReadAheadContext implements ReadaheadContext {
    private static final Logger LOGGER = LogManager.getLogger(WindowedReadAheadContext.class);

    private final Path path;
    private final long fileLength;
    private final long lastFileSeg; // inclusive, in block units
    private final Worker worker;
    private final WindowedReadaheadPolicy policy;
    private final Thread processingThread;

    private static final int HIT_NOP_THRESHOLD_BASE = 8;
    private static final int HIT_NOP_THRESHOLD_MAX = 512;
    private int hitNopThreshold = HIT_NOP_THRESHOLD_BASE;

    private static final int MISS_BATCH = 3;                        // misses to batch before readahead
    private static final long MIN_UNPARK_INTERVAL_NS = 300_000L;    // 300 µs rate-limit

    // these are states shared between readers and worker, so need synchonizations.
    private volatile long desiredEndBlock = 0;        // coalesced target (exclusive, in blocks)
    private volatile long lastScheduledEndBlock = 0;  // watermark (exclusive, in blocks)

    // Best-effort counters (non-volatile): typically one context per IndexInput (single thread).
    // If shared across threads, races are acceptable for readahead heuristics.
    // Keeping non-volatile saves ~2-3 cycles per access on hot path.
    private int consecutiveHits = 0;
    private int missCount = 0;
    private long missMaxBlock = -1;

    // Small rate limiter for unparks. Volatile to avoid redundant wake storms if multiple threads call onAccess.
    private volatile long lastUnparkNanos = 0;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    // VarHandle used ONLY to implement atomic-max for desiredEndBlock if this context is shared.
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
        this.fileLength = fileLength;
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
        var policy = new WindowedReadaheadPolicy(
            path,
            config.initialWindow(),
            config.maxWindowSegments(),
            config.shrinkOnRandomThreshold()
        );
        return new WindowedReadAheadContext(path, fileLength, worker, policy, processingThread);
    }

    // HOT PATH: Called per access, has to be very cheap.
    @Override
    public void onAccess(long blockOffset, boolean wasHit) {
        final long currBlock = blockOffset >>> CACHE_BLOCK_SIZE_POWER;

        // -----------------------------
        // Cache Hit Path
        // -----------------------------
        if (wasHit) {
            if (++consecutiveHits < hitNopThreshold) {
                // Still within suppression window
                return;
            }

            // Guard-band: only extend when we're close to the scheduled tail
            final long scheduledEnd = lastScheduledEndBlock;
            if (scheduledEnd > 0) {
                final long guardStart = Math.max(0, scheduledEnd - policy.leadBlocks());
                if (currBlock >= guardStart && currBlock < scheduledEnd) {
                    final long newEnd = Math.min(currBlock + policy.leadBlocks(), lastFileSeg + 1);
                    extendDesiredTo(newEnd);
                    maybeUnpark();
                }
            }

            // Exponential backoff: if cache is extremely hot, reduce wakeups
            if (hitNopThreshold < HIT_NOP_THRESHOLD_MAX) {
                hitNopThreshold <<= 1; // double the threshold
            }

            consecutiveHits = 0;
            return;
        }

        // -----------------------------
        // Cache Miss Path
        // -----------------------------
        consecutiveHits = 0;
        hitNopThreshold = HIT_NOP_THRESHOLD_BASE; // reset adaptive suppression

        missCount++;
        if (currBlock > missMaxBlock) {
            missMaxBlock = currBlock;
        }

        final int window = Math.max(1, policy.currentWindow());
        final boolean enoughMisses = missCount >= MISS_BATCH;
        final boolean farAhead = (missMaxBlock - lastScheduledEndBlock) >= Math.max(1, window / 4);

        if (enoughMisses || farAhead) {
            final long target = Math.min(missMaxBlock + policy.leadBlocks(), lastFileSeg + 1);
            extendDesiredTo(target);
            missCount = 0;
            missMaxBlock = -1;
            maybeUnpark();
        }
    }

    @Override
    public boolean processQueue() {
        if (closed.get())
            return false;

        final long desired = desiredEndBlock;
        final long scheduled = lastScheduledEndBlock;
        if (desired <= scheduled)
            return false;

        final long startSeg = scheduled;
        final long endExclusive = Math.min(desired, lastFileSeg + 1);
        final long blockCount = endExclusive - startSeg;
        if (blockCount < 1)
            return false;

        final long anchorOffset = startSeg << CACHE_BLOCK_SIZE_POWER;
        final boolean accepted = worker.schedule(path, anchorOffset, blockCount);
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

    /**
     * Extend desiredEndBlock to at least {@code newEndExclusive} using an atomic max.
     * This is practically free in the common case (when no extension is needed),
     * and preserves correctness if multiple reader threads share the context.
     */
    private void extendDesiredTo(long newEndExclusive) {
        long prev;
        do {
            // Plain volatile read first (cheaper than getAcquire)
            prev = desiredEndBlock;
            if (newEndExclusive <= prev)
                return;
            // weak CAS is fine; contention is rare and best-effort acceptable
        } while (!DESIRED_END_VH.weakCompareAndSetRelease(this, prev, newEndExclusive));
    }

    /**
     * Wake the processing thread only when the delta is meaningful and not too frequent.
     * The minimum batch size is dynamic, derived from current policy window.
     */
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
    public void onCacheMiss(long fileOffset) {
        onAccess(fileOffset, false);
    }

    /**
     * Called when the caller observed a cache hit but doesn’t provide the file offset.
     * We conservatively extend from the scheduled tail after a small hit streak.
     */
    @Override
    public void onCacheHit() {
        if (++consecutiveHits < HIT_NOP_THRESHOLD_BASE)
            return;

        final long scheduledEnd = lastScheduledEndBlock;
        if (scheduledEnd > 0) {
            extendDesiredTo(Math.min(scheduledEnd + policy.leadBlocks(), lastFileSeg + 1));
            maybeUnpark();
        }
        consecutiveHits = 0;
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
        // do not reset lastUnparkNanos to preserve rate-limiter window
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

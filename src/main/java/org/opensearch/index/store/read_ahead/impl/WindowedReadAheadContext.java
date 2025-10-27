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
 * Windowed readahead context implementation that manages adaptive prefetching
 * for sequential file access patterns.
 * 
 * <p>This implementation uses a configurable window-based readahead strategy
 * that adapts to access patterns. It coordinates with a Worker to schedule
 * bulk prefetch operations and integrates with cache miss/hit feedback to
 * optimize readahead behavior.
 *
 * @opensearch.internal
 */
public class WindowedReadAheadContext implements ReadaheadContext {
    private static final Logger LOGGER = LogManager.getLogger(WindowedReadAheadContext.class);

    private final Path path;
    private final long fileLength;
    private final Worker worker;
    private final WindowedReadaheadPolicy policy;
    private final Thread processingThread; // Reference for unpark

    /**
     * Readahead signal encoding: 64-bit value packs cache hit/miss flag + block offset
     *
     * <pre>
     * ┌──────────────────────────────── 64-bit Encoded Signal ─────────────────────────────────┐
     * │                                                                                        │
     * │  ┌─────────┬───────────────────────────────────────────────────────────────────────┐   │
     * │  │  Bit 63 │  Bits 62 ─────────────────────────────── Bits 0                       │   │
     * │  ├─────────┼───────────────────────────────────────────────────────────────────────┤   │
     * │  │ Hit (1) │                    Block Offset (63 bits)                             │   │
     * │  │ Miss(0) │              (supports files up to 8 Exabytes)                        │   │
     * │  └─────────┴───────────────────────────────────────────────────────────────────────┘   │
     * └────────────────────────────────────────────────────────────────────────────────────────┘
     * </pre>
     */
    private static final long HIT_FLAG = 1L << 63;    // High bit: cache hit indicator
    private static final long OFFSET_MASK = ~HIT_FLAG; // Lower 63 bits: block offset
    private static final long EMPTY_SIGNAL = -1L;      // Sentinel: no pending signal

    // VarHandle for optimized readahead signal updates (hot path uses setRelease)
    private static final VarHandle READAHEAD_SIGNAL_VH;
    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            READAHEAD_SIGNAL_VH = l.findVarHandle(WindowedReadAheadContext.class, "readaheadSignal", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("unused") // accessed via VarHandle
    private volatile long readaheadSignal = EMPTY_SIGNAL;

    // Watermark: last scheduled end block (exclusive) to avoid redundant triggers
    private volatile long lastScheduledEndBlock = 0;

    // Scheduling state (per file)
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private WindowedReadAheadContext(Path path, long fileLength, Worker worker, WindowedReadaheadPolicy policy, Thread processingThread) {
        this.path = path;
        this.fileLength = fileLength;
        this.worker = worker;
        this.policy = policy;
        this.processingThread = processingThread;
    }

    /**
     * Creates a new WindowedReadAheadContext with the specified configuration.
     *
     * @param path the file path for readahead operations
     * @param fileLength the total length of the file in bytes
     * @param worker the worker to schedule readahead operations
     * @param config the readahead configuration settings
     * @param processingThread the background thread for unpark notifications
     * @return a new WindowedReadAheadContext instance
     */
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

    @Override
    public void onAccess(long blockOffset, boolean wasHit) {
        long encoded = (blockOffset & OFFSET_MASK) | (wasHit ? HIT_FLAG : 0);

        // Fast path: we use setRelease (like lazySet) - eventual visibility is sufficient
        // Missing a few signals is acceptable over peformance for readahead
        long prev = (long) READAHEAD_SIGNAL_VH.getAndSetRelease(this, encoded);

        // Only unpark if the background thread had no pending work
        if (prev == EMPTY_SIGNAL) {
            LockSupport.unpark(processingThread);
        }
    }

    @Override
    public boolean processQueue() {
        if (closed.get())
            return false;

        // Get and clear readahead signal atomically (consumer uses full barrier)
        long encoded = (long) READAHEAD_SIGNAL_VH.getAndSet(this, EMPTY_SIGNAL);
        if (encoded == EMPTY_SIGNAL) {
            return false; // No pending signal
        }

        // Decode hit/miss and offset
        boolean wasHit = (encoded & HIT_FLAG) != 0;
        long blockOffset = encoded & OFFSET_MASK;
        long currentBlock = blockOffset >>> CACHE_BLOCK_SIZE_POWER;

        if (wasHit) {
            policy.onCacheHit();

            // Hit-ahead: if we're near the tail of scheduled window, extend it
            // Check if current block is in the "guard band" near lastScheduledEndBlock
            long scheduledEnd = lastScheduledEndBlock;
            if (scheduledEnd > 0) {
                long leadBlocks = policy.leadBlocks();
                long guardStart = scheduledEnd - leadBlocks;

                // If hit crosses into guard band, pre-trigger extension
                if (currentBlock >= guardStart && currentBlock < scheduledEnd) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Hit-ahead trigger: block={} guard=[{}-{})", currentBlock, guardStart, scheduledEnd);
                    }
                    trigger(blockOffset);
                }
            }
        } else {
            // Cache miss - ncheck if we should trigger readahead
            if (policy.shouldTrigger(blockOffset)) {
                trigger(blockOffset);
            }
        }

        return true;
    }

    private void trigger(long anchorFileOffset) {
        if (closed.get() || worker == null)
            return;

        final long startSeg = anchorFileOffset >>> CACHE_BLOCK_SIZE_POWER;
        final long lastSeg = (fileLength - 1) >>> CACHE_BLOCK_SIZE_POWER;
        final long safeEndSeg = Math.max(0, lastSeg);

        final long windowSegs = policy.currentWindow();
        if (windowSegs <= 0 || startSeg > safeEndSeg)
            return;

        final long endExclusive = Math.min(startSeg + windowSegs, safeEndSeg + 1);
        if (startSeg >= endExclusive)
            return;

        // Watermark check: if desired end is already covered, skip scheduling
        long currentWatermark = lastScheduledEndBlock;
        if (endExclusive <= currentWatermark) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Skipping trigger: endExclusive={} <= watermark={}", endExclusive, currentWatermark);
            }
            return;
        }

        final long blockCount = endExclusive - startSeg;

        if (blockCount > 0) {
            // Schedule the entire window
            final boolean accepted = worker.schedule(path, anchorFileOffset, blockCount);

            if (accepted) {
                // Update watermark: we've scheduled up to endExclusive
                lastScheduledEndBlock = endExclusive;
            }

            LOGGER
                .debug(
                    "RA_BULK_TRIGGER path={} anchorOff={} startSeg={} endExclusive={} windowSegs={} scheduledBlocks={} accepted={} watermark={}",
                    path,
                    anchorFileOffset,
                    startSeg,
                    endExclusive,
                    windowSegs,
                    blockCount,
                    accepted,
                    lastScheduledEndBlock
                );

            if (!accepted) {
                LOGGER
                    .info(
                        "Window bulk readahead backpressure path={} length={} startSeg={} endExclusive={} windowBlocks={}",
                        path,
                        fileLength,
                        startSeg,
                        endExclusive,
                        blockCount
                    );
            }
        }
    }

    @Override
    public void onCacheMiss(long fileOffset) {
        onAccess(fileOffset, false);
    }

    @Override
    public void onCacheHit() {
        onAccess(-1L, true); // Offset doesn't matter for hits
    }

    @Override
    public boolean hasQueuedWork() {
        return (long) READAHEAD_SIGNAL_VH.get(this) != EMPTY_SIGNAL;
    }

    @Override
    public ReadaheadPolicy policy() {
        return this.policy;
    }

    @Override
    public void triggerReadahead(long fileOffset) {
        trigger(fileOffset);
    }

    @Override
    public void reset() {
        policy.reset();
    }

    @Override
    public void cancel() {
        if (worker != null) {
            worker.cancel(path);
        }
    }

    @Override
    public boolean isReadAheadEnabled() {
        return !closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            cancel();
        }
    }
}

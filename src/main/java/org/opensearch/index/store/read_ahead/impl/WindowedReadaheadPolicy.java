/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE_POWER;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.read_ahead.ReadaheadPolicy;

/**
 * Adaptive readahead policy inspired by Linux kernel readahead logic.
 * 
 * <p>This policy uses a "marker + lead" approach to predict when to trigger readahead:
 * <ul>
 * <li><strong>Sequential Access:</strong> When reads follow a predictable pattern (curr == last + 1),
 *     the window grows up to 2x (capped at maxWindow) and readahead is triggered.</li>
 * <li><strong>Small Gaps:</strong> Forward jumps within a small threshold are treated as
 *     "mostly sequential" - window shrinks slightly but readahead still triggers.</li>
 * <li><strong>Large Gaps/Backward:</strong> Random access patterns reset the window to initial size
 *     and disable readahead until sequential behavior resumes.</li>
 * <li><strong>Cache Hits:</strong> High cache hit streaks shrink the window to avoid over-prefetching.</li>
 * </ul>
 * 
 * <p><strong>Key Concepts:</strong>
 * <ul>
 * <li><strong>Window:</strong> Number of blocks to prefetch (grows/shrinks based on access patterns)</li>
 * <li><strong>Marker:</strong> Future position that triggers readahead when crossed (curr + lead)</li>
 * <li><strong>Lead:</strong> Distance ahead of current position to place the marker (typically window/3)</li>
 * </ul>
 * 
 * <p>This approach balances prefetch effectiveness with resource consumption by adapting
 * to actual access patterns rather than using fixed readahead sizes.
 */
public final class WindowedReadaheadPolicy implements ReadaheadPolicy {
    private static final Logger LOGGER = LogManager.getLogger(WindowedReadaheadPolicy.class);

    private static final VarHandle VH_HIT_STREAK;

    static {
        try {
            VH_HIT_STREAK = MethodHandles.lookup().findVarHandle(WindowedReadaheadPolicy.class, "hitStreak", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("unused")
    private volatile int hitStreak = 0;

    private final Path path;
    private final int initialWindow;
    private final int maxWindow;
    private final int minLead;

    /**
     * Controls tolerance for small forward gaps before treating access as random.
     * A gap up to (window/smallGapDivisor) is considered "mostly sequential".
     * Example: smallGapDivisor=4 allows gaps up to window/4 blocks.
     */
    private final int smallGapDivisor;

    /**
     * Immutable state for the readahead policy.
     */
    private static final class State {
        /** Last accessed segment (-1 if uninitialized) */
        final long lastSeg;
        /** Marker segment - triggers readahead when crossed */
        final long markerSeg;
        /** Current window size in segments */
        final int window;

        State(long lastSeg, long markerSeg, int window) {
            this.lastSeg = lastSeg;
            this.markerSeg = markerSeg;
            this.window = window;
        }

        static State init(int initWin) {
            return new State(-1L, -1L, initWin);
        }
    }

    private final AtomicReference<State> ref;

    public WindowedReadaheadPolicy(
        Path path,
        int initialWindow,
        int maxWindow,
        int shrinkOnRandomThreshold /*unused now but kept for ctor compat*/
    ) {
        this(path, initialWindow, maxWindow, /*minLead*/1, /*smallGapDivisor*/4);
    }

    public WindowedReadaheadPolicy(Path path, int initialWindow, int maxWindow, int minLead, int smallGapDivisor) {
        if (initialWindow < 1)
            throw new IllegalArgumentException("initialWindow must be >= 1");
        if (maxWindow < initialWindow)
            throw new IllegalArgumentException("maxWindow must be >= initialWindow");
        if (minLead < 1)
            throw new IllegalArgumentException("minLead must be >= 1");
        if (smallGapDivisor < 2)
            throw new IllegalArgumentException("smallGapDivisor must be >= 2");

        this.path = path;
        this.initialWindow = initialWindow;
        this.maxWindow = maxWindow;
        this.minLead = minLead;
        this.smallGapDivisor = smallGapDivisor;
        this.ref = new AtomicReference<>(State.init(initialWindow));
    }

    private int leadFor(int window) {
        return Math.max(minLead, window / 3);
    }

    /**
     * Records a cache hit to track hit streaks.
     * High hit streaks indicate over-prefetching and will shrink the window.
     */
    public void onCacheHit() {
        VH_HIT_STREAK.getAndAdd(this, 1);
    }

    @Override
    public boolean shouldTrigger(long currentOffset) {
        final long currSeg = currentOffset >>> CACHE_BLOCK_SIZE_POWER;
        final int streak = (int) VH_HIT_STREAK.getAndSet(this, 0);
        for (;;) {
            final State s = ref.get();
            if (streak > s.window) {
                onCacheHitShrink();
                return false;
            }

            // First access — trigger and seed state
            if (s.lastSeg == -1L) {
                final int win = initialWindow;
                final long marker = currSeg + leadFor(win);
                if (ref.compareAndSet(s, new State(currSeg, marker, win))) {
                    LOGGER.trace("Path={}, Trigger={}, currSeg={}, newMarker={}, win={}", path, true, currSeg, marker, win);
                    return true;
                }
                continue;
            }

            final long gap = currSeg - s.lastSeg; // signed
            int newWin = s.window;
            long proposedMarker = s.markerSeg; // keep as-is unless we trigger/cross
            boolean trigger;

            final int seqGapBuffer = Math.max(2, Math.min(s.window / 2, 4));
            final boolean isSequential = gap >= 1 && gap <= seqGapBuffer;

            if (isSequential) {
                // Sequential forward → always trigger, grow window
                trigger = true;
                newWin = Math.min(s.window << 1, maxWindow);
            } else if (gap > seqGapBuffer) {
                // Forward jump
                final int smallGap = Math.max(1, s.window / smallGapDivisor);
                if (gap <= smallGap) {
                    // Small jump that crosses marker → trigger, cautiously shrink window
                    trigger = true;
                    newWin = Math.max(1, s.window >>> 1); // shrink window
                } else {
                    // Large jump or didn't cross marker → reset window, do not trigger
                    trigger = false;
                    newWin = initialWindow;
                }
            } else if (gap == 0) {
                trigger = false;
            } else {
                // Backward/same → reset window, don't trigger
                trigger = false;
                newWin = initialWindow;
            }

            final State next = new State(currSeg, proposedMarker, newWin);
            if (ref.compareAndSet(s, next)) {
                LOGGER
                    .debug(
                        "Path={}, Gap={}, isSequential={}, Trigger={}, currSeg={}, newMarker={}, win={}",
                        path,
                        gap,
                        isSequential,
                        trigger,
                        currSeg,
                        proposedMarker,
                        newWin
                    );
                return trigger;
            }
        }
    }

    @Override
    public int currentWindow() {
        return ref.get().window;
    }

    public long currentMarker() {
        return ref.get().markerSeg;
    }

    /**
     * Returns the current lead distance (how far ahead the marker is placed).
     * Useful for callers that need to know the readahead trigger threshold.
     */
    public int leadBlocks() {
        return leadFor(ref.get().window);
    }

    @Override
    public int initialWindow() {
        return initialWindow;
    }

    @Override
    public int maxWindow() {
        return maxWindow;
    }

    /**
     * Handles medium queue pressure by shrinking the window to reduce load.
     * Called when the readahead queue is under moderate stress.
     */
    public void onQueuePressureMedium() {
        ref.updateAndGet(s -> new State(s.lastSeg, s.markerSeg, Math.max(1, s.window >>> 1)));
    }

    /**
     * Handles high queue pressure by resetting window to initial size.
     * Called when the readahead queue is under severe stress.
     */
    public void onQueuePressureHigh() {
        ref.updateAndGet(s -> new State(s.lastSeg, s.markerSeg, initialWindow));
    }

    /**
     * Handles queue saturation by applying medium pressure response.
     * Called when the readahead queue is completely full.
     */
    public void onQueueSaturated() {
        onQueuePressureMedium();
    }

    /**
     * Shrinks the window in response to high cache hit streaks.
     * This reduces unnecessary prefetching when the cache is already effective.
     */
    public void onCacheHitShrink() {
        ref.updateAndGet(s -> new State(s.lastSeg, s.markerSeg, Math.max(initialWindow, s.window >>> 1)));
    }

    /**
     * Resets the policy to its initial state.
     * Useful when starting to read a new file or after access pattern changes.
     */
    public void reset() {
        ref.set(State.init(initialWindow));
    }
}

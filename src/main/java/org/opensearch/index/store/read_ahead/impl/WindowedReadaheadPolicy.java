/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE_POWER;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.read_ahead.ReadaheadPolicy;

/**
 * Adaptive windowed readahead policy.
 *
 * Grows window exponentially on sequential access,
 * decays it gradually on random/backward access to avoid thrashing.
 */
public class WindowedReadaheadPolicy implements ReadaheadPolicy {
    private static final Logger LOGGER = LogManager.getLogger(WindowedReadaheadPolicy.class);

    private final Path path;
    private final int initialWindow;
    private final int maxWindow;
    private final int minLead;
    private final int smallGapDivisor;

    /** Immutable state snapshot. */
    private static final class State {
        final long lastSeg;
        final int window;

        State(long lastSeg, int window) {
            this.lastSeg = lastSeg;
            this.window = window;
        }

        static State init(int initWin) {
            return new State(-1L, initWin);
        }
    }

    private final AtomicReference<State> ref;

    /**
     * Creates a new WindowedReadaheadPolicy with the specified parameters.
     *
     * @param path the file path this policy applies to
     * @param initialWindow the starting window size
     * @param maxWindow the maximum window size
     * @param smallGapDivisor the divisor for determining small forward jumps
     */
    public WindowedReadaheadPolicy(Path path, int initialWindow, int maxWindow, int smallGapDivisor) {
        this(path, initialWindow, maxWindow, 1, smallGapDivisor);
    }

    /**
     * Creates a new WindowedReadaheadPolicy with full parameter control.
     *
     * @param path the file path this policy applies to
     * @param initialWindow the starting window size (must be &gt;= 1)
     * @param maxWindow the maximum window size (must be &gt;= initialWindow)
     * @param minLead the minimum lead distance (must be &gt;= 1)
     * @param smallGapDivisor the divisor for determining small forward jumps (must be &gt;= 2)
     */
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

    /** Gradual decay instead of full reset */
    private int decay(int window) {
        if (window <= initialWindow)
            return initialWindow;
        // Reduce by 25% of current size
        return Math.max(initialWindow, window - Math.max(1, window / 4));
    }

    @Override
    public boolean shouldTrigger(long currentOffset) {
        final long currSeg = currentOffset >>> CACHE_BLOCK_SIZE_POWER;
        for (;;) {
            final State s = ref.get();
            if (s.lastSeg == -1L) {
                final int win = initialWindow;
                if (ref.compareAndSet(s, new State(currSeg, win))) {
                    LOGGER.trace("Init: path={}, currSeg={}, win={}", path, currSeg, win);
                    return true;
                }
                continue;
            }

            final long gap = currSeg - s.lastSeg;
            int newWin;
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
                    // Small jump → trigger, shrink window
                    trigger = true;
                    newWin = Math.max(initialWindow, s.window >>> 1);
                } else {
                    // Large jump → hard reset (Linux behavior)
                    trigger = false;
                    newWin = initialWindow;
                }
            } else if (gap == 0) {
                // Same position → no-op
                trigger = false;
                newWin = s.window;
            } else {
                // Backward seek → decay for small, reset for large
                trigger = false;
                long absGap = (gap == Long.MIN_VALUE) ? Long.MAX_VALUE : (gap < 0 ? -gap : gap);
                newWin = (absGap > s.window / 2) ? initialWindow : decay(s.window);
            }

            final State next = new State(currSeg, newWin);
            if (ref.compareAndSet(s, next)) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER
                        .debug(
                            "path={}, gap={}, seq={}, trigger={}, win(old→new)={}->{}, currSeg={}, lead={}",
                            path,
                            gap,
                            isSequential,
                            trigger,
                            s.window,
                            newWin,
                            currSeg,
                            leadFor(newWin)
                        );
                }
                return trigger;
            }
        }
    }

    @Override
    public int currentWindow() {
        return ref.get().window;
    }

    /**
     * Returns the current lead distance (how far ahead the marker is placed).
     * Useful for callers that need to know the readahead trigger threshold.
     * 
     * @return the current lead distance in blocks
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
        ref.updateAndGet(s -> new State(s.lastSeg, Math.max(initialWindow, s.window >>> 1)));
    }

    /**
     * Handles high queue pressure by resetting window to initial size.
     * Called when the readahead queue is under severe stress.
     */
    public void onQueuePressureHigh() {
        ref.updateAndGet(s -> new State(s.lastSeg, initialWindow));
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
        ref.updateAndGet(s -> new State(s.lastSeg, Math.max(initialWindow, s.window >>> 1)));
    }

    /**
     * Resets the policy to its initial state.
     * Useful when starting to read a new file or after access pattern changes.
     */
    public void reset() {
        ref.set(State.init(initialWindow));
    }
}

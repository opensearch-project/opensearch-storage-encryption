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
 * Linux-style adaptive readahead (marker + lead).
 *
 * Rules (roughly mirroring the kernel logic):
 *  - First access: seed window=initial, place marker = curr + lead(window), trigger.
 *  - Sequential (curr == last + 1):
 *      * If we crossed marker → trigger and grow window (min(2x, max)).
 *      * Advance marker = curr + lead(window).
 *  - Small forward gap (0 less gap less equal smallGapThresh) → treat as seq-ish, shrink a bit, re-place marker.
 *  - Large forward jump OR any backward jump → reset to initial window, re-place marker; no trigger now.
 *  - Lead is derived from window (conservative: win/2; mildly aggressive: win/3 or win/4).
 */
public final class WindowedReadaheadPolicy implements ReadaheadPolicy {
    private static final Logger LOGGER = LogManager.getLogger(WindowedReadaheadPolicy.class);

    private final Path path;
    private final int initialWindow;       // segments
    private final int maxWindow;           // segments
    private final int minLead;             // segments (>=1)

    // How tolerant we are to small forward gaps before treating as random.
    // Linux will sometimes be forgiving of small gaps; we use a fraction of current window.
    private final int smallGapDivisor;     // e.g. 4 → allow gap up to win/4 as "seq-ish"

    private static final class State {
        final long lastSeg;    // -1 if uninit
        final long markerSeg;  // next trigger point (reader crosses => trigger)
        final int window;     // current window (segments)

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

    @Override
    public boolean shouldTrigger(long currentOffset) {
        final long currSeg = currentOffset >>> CACHE_BLOCK_SIZE_POWER;

        for (;;) {
            final State s = ref.get();

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
            boolean trigger = false;

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

    /** Expose current lead for callers that want to pass a near-threshold to the worker. */
    public int leadSegments() {
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

    /** Queue backpressure hooks (optional, simple + Linux-ish “be humble under pressure”). */
    public void onQueuePressureMedium() {
        ref.updateAndGet(s -> new State(s.lastSeg, s.markerSeg, Math.max(1, s.window >>> 1)));
    }

    public void onQueuePressureHigh() {
        ref.updateAndGet(s -> new State(s.lastSeg, s.markerSeg, initialWindow));
    }

    public void onQueueSaturated() {
        onQueuePressureMedium();
    }

    public void reset() {
        ref.set(State.init(initialWindow));
    }
}

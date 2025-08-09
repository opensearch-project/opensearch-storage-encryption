/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE_POWER;

import java.util.concurrent.atomic.AtomicReference;

import org.opensearch.index.store.read_ahead.ReadaheadPolicy;

/** Atomic, Linux-like adaptive readahead policy (marker + lead). */
public final class WindowedReadaheadPolicy implements ReadaheadPolicy {

    private final int initialWindow;           // segments
    private final int maxWindow;               // segments
    private final int shrinkOnRandomThreshold; // steps before halving window
    private final int minLead;                 // minimum lead in segments

    // All mutable fields are inside this immutable snapshot
    private static final class State {
        final long lastOffset;  // -1 if uninit
        final long lastSeg;     // -1 if uninit
        final long markerSeg;   // -1 if uninit
        final int window;
        final int seqStreak;
        final int rndStreak;

        State(long lastOffset, long lastSeg, long markerSeg, int window, int seqStreak, int rndStreak) {
            this.lastOffset = lastOffset;
            this.lastSeg = lastSeg;
            this.markerSeg = markerSeg;
            this.window = window;
            this.seqStreak = seqStreak;
            this.rndStreak = rndStreak;
        }

        static State init(int initWin) {
            return new State(-1L, -1L, -1L, initWin, 0, 0);
        }
    }

    private final AtomicReference<State> ref;

    public WindowedReadaheadPolicy(int initialWindow, int maxWindow, int shrinkOnRandomThreshold) {
        this(initialWindow, maxWindow, shrinkOnRandomThreshold, 1);
    }

    public WindowedReadaheadPolicy(int initialWindow, int maxWindow, int shrinkOnRandomThreshold, int minLead) {
        if (initialWindow < 1)
            throw new IllegalArgumentException("initialWindow must be >= 1");
        if (maxWindow < initialWindow)
            throw new IllegalArgumentException("maxWindow must be >= initialWindow");
        if (shrinkOnRandomThreshold < 1)
            throw new IllegalArgumentException("shrinkOnRandomThreshold must be >= 1");
        if (minLead < 1)
            throw new IllegalArgumentException("minLead must be >= 1");
        this.initialWindow = initialWindow;
        this.maxWindow = maxWindow;
        this.shrinkOnRandomThreshold = shrinkOnRandomThreshold;
        this.minLead = minLead;
        this.ref = new AtomicReference<>(State.init(initialWindow));
    }

    // Compute lead based on a window value.
    private int leadFor(int window) {
        int lead = Math.max(minLead, window / 2);
        return Math.max(1, lead);
    }

    @Override
    public boolean shouldTrigger(long currentOffset) {
        final long currSeg = currentOffset >>> CACHE_BLOCK_SIZE_POWER;

        while (true) {
            final State s = ref.get();

            // First access → prime + plant marker and trigger
            if (s.lastOffset == -1L) {
                final int win = initialWindow;
                final long marker = currSeg + leadFor(win);
                final State ns = new State(currentOffset, currSeg, marker, win, 0, 0);
                if (ref.compareAndSet(s, ns))
                    return true;
                continue;
            }

            final boolean forward = currSeg >= s.lastSeg;
            boolean trigger = false;

            int win = s.window;
            int seq = s.seqStreak;
            int rnd = s.rndStreak;
            long marker = s.markerSeg;

            if (forward) {
                if (currSeg >= s.markerSeg) {
                    // crossed marker → trigger + ramp
                    trigger = true;
                    seq = seq + 1;
                    rnd = 0;
                    if (seq >= 2) {
                        win = Math.min(win << 1, maxWindow);
                    }
                    marker = currSeg + leadFor(win);
                }
            } else {
                // random/backwards → shrink after threshold, reposition marker
                trigger = false;
                seq = 0;
                rnd = rnd + 1;
                if (rnd >= shrinkOnRandomThreshold) {
                    win = Math.max(1, win >>> 1);
                    rnd = 0; // optional: reset after shrink
                }
                marker = currSeg + Math.max(1, Math.min(leadFor(win), win));
            }

            final State ns = new State(currentOffset, currSeg, marker, win, seq, rnd);
            if (ref.compareAndSet(s, ns)) {
                return trigger;
            }
        }
    }

    @Override
    public int currentWindow() {
        return ref.get().window;
    }

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

    public void onQueuePressureMedium() {
        ref.updateAndGet(s -> new State(s.lastOffset, s.lastSeg, s.markerSeg, Math.max(1, s.window >>> 1), s.seqStreak, s.rndStreak));
    }

    public void onQueuePressureHigh() {
        ref.updateAndGet(s -> new State(s.lastOffset, s.lastSeg, s.markerSeg, 1, s.seqStreak, s.rndStreak));
    }

    public void onQueueSaturated() {
        onQueuePressureMedium();
    }

    public void reset() {
        ref.set(State.init(initialWindow));
    }
}

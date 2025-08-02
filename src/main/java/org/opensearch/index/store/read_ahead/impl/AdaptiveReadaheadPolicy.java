/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import org.opensearch.index.store.read_ahead.ReadaheadPolicy;

/**
 * Adaptive ReadaheadPolicy implementation.
 *
 * - Doubles window size on sequential access (up to maxWindow).
 * - Shrinks window on random access.
 * - Triggers readahead on first sequential step.
 *
 * Inspired by Linux readahead_state and tuned for Lucene/OpenSearch.
 */
public class AdaptiveReadaheadPolicy implements ReadaheadPolicy {

    private final int initialWindow;
    private final int maxWindow;
    private final int shrinkOnRandomThreshold;

    // expectedNextbyte offset in the file of the last segment that was accessed.
    private long lastOffset = -1;
    private int sequentialStreak = 0;
    private int randomStreak = 0;

    private int currentWindow;

    /**
     * @param initialWindow initial readahead window (segments)
     * @param maxWindow max readahead window (segments)
     */
    public AdaptiveReadaheadPolicy(int initialWindow, int maxWindow, int shrinkOnRandomThreshold) {
        if (initialWindow < 1) {
            throw new IllegalArgumentException("initialWindow must be >= 1");
        }
        if (maxWindow < initialWindow) {
            throw new IllegalArgumentException("maxWindow must be >= initialWindow");
        }
        this.initialWindow = initialWindow;
        this.maxWindow = maxWindow;
        this.currentWindow = initialWindow;
        this.shrinkOnRandomThreshold = shrinkOnRandomThreshold;
    }

    @Override
    public synchronized boolean shouldTrigger(long currentOffset, int segmentSize) {
        boolean trigger;

        // trigger one immediate readahead on first read.
        if (this.lastOffset == -1) {
            this.lastOffset = currentOffset;
            this.sequentialStreak = 0;
            this.currentWindow = initialWindow;

            trigger = true; // one immediate readahead after the first block.

            return trigger;
        }

        // Check sequential access (expected next offset)
        long expectedNext = this.lastOffset + segmentSize;
        if (currentOffset == expectedNext) {
            sequentialStreak++;

            randomStreak = 0; // reset random streak.

            trigger = sequentialStreak >= 1; // trigger immediately on first sequential step

            // Exponential growth capped by maxWindow
            currentWindow = Math.min(currentWindow * 2, maxWindow);
        } else {
            // Random access -> shrink window and reset streak
            sequentialStreak = 0;
            randomStreak++;

            if (randomStreak >= shrinkOnRandomThreshold) {
                currentWindow = Math.max(1, currentWindow / 2);
                randomStreak = 0; // reset after shrink
            }

            trigger = false;
        }

        this.lastOffset = currentOffset;

        return trigger;
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
     * @return current adaptive readahead window in segments
     */
    public synchronized int currentWindow() {
        return currentWindow;
    }

    /**
     * Reset state, e.g., after a large random seek or reset.
     */
    public synchronized void reset() {
        this.lastOffset = -1;
        this.sequentialStreak = 0;
        this.currentWindow = initialWindow;
        this.randomStreak = 0;
    }
}

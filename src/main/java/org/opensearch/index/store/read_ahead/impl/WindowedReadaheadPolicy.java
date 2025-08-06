/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE;

import org.opensearch.index.store.read_ahead.ReadaheadPolicy;

/**
 * Adaptive ReadaheadPolicy implementation with windowing strategy.
 *
 * Window Management:
 *   ┌─────────────────────────────────────────────────────────┐
 *   │ Sequential Access Pattern:                              │
 *   │   Read(100) → Read(104) → Read(108) → Read(112)        │
 *   │   Window:  1 →     1    →     2    →     4              │
 *   └─────────────────────────────────────────────────────────┘
 *
 *   ┌─────────────────────────────────────────────────────────┐
 *   │ Random Access Pattern:                                  │
 *   │   Read(100) → Read(200) → Read(50) → Read(300)         │
 *   │   Window:  1 →     1    →    1    →    1/2              │
 *   └─────────────────────────────────────────────────────────┘
 *
 * Strategy:
 * - Grows window exponentially after 2+ (smoothing) sequential accesses
 * - Shrinks window by half after N random accesses
 * - Forward strides (gaps) are treated as sequential
 *
 * Inspired by Linux readahead_state.
 */
public class WindowedReadaheadPolicy implements ReadaheadPolicy {

    private final int initialWindow;
    private final int maxWindow;
    private final int shrinkOnRandomThreshold;

    // Expected next byte offset in the file of the last segment that was accessed.
    private long lastOffset = -1;
    private int sequentialStreak = 0;
    private int randomStreak = 0;

    private int currentWindow;

    /**
     * @param initialWindow initial readahead window (segments)
     * @param maxWindow max readahead window (segments)
     * @param shrinkOnRandomThreshold number of random accesses before shrinking window
     */
    public WindowedReadaheadPolicy(int initialWindow, int maxWindow, int shrinkOnRandomThreshold) {
        if (initialWindow < 1) {
            throw new IllegalArgumentException("initialWindow must be >= 1");
        }
        if (maxWindow < initialWindow) {
            throw new IllegalArgumentException("maxWindow must be >= initialWindow");
        }
        if (shrinkOnRandomThreshold < 1) {
            throw new IllegalArgumentException("shrinkOnRandomThreshold must be >= 1");
        }
        this.initialWindow = initialWindow;
        this.maxWindow = maxWindow;
        this.currentWindow = initialWindow;
        this.shrinkOnRandomThreshold = shrinkOnRandomThreshold;
    }

    @Override
    public synchronized boolean shouldTrigger(long currentOffset) {
        // First access: trigger an immediate readahead to hide first read latency
        if (lastOffset == -1) {
            lastOffset = currentOffset;
            sequentialStreak = 0;
            randomStreak = 0;
            currentWindow = initialWindow;
            return true; // Trigger first readahead immediately
        }

        long expectedNext = lastOffset + CACHE_BLOCK_SIZE;
        boolean trigger;

        /*
         * Forward Progress Detection:
         *
         *   lastOffset    expectedNext     currentOffset
         *       |             |                |
         *   ────┼─────────────┼────────────────┼─────► file
         *      100           104              108
         *
         * Case 1: currentOffset >= expectedNext  → Sequential/Forward stride
         * Case 2: currentOffset <  expectedNext  → Random/Backwards access
         *
         * This treats any forward progress (including gaps) as sequential
         * while detecting true backwards/random access patterns.
         */
        if (currentOffset >= expectedNext) {
            sequentialStreak++;
            randomStreak = 0; // reset random access counter

            trigger = true; // always trigger readahead on forward progress

            // Conservative window growth strategy:
            // Only grow after 2+ sequential accesses to avoid over-prefetching
            if (sequentialStreak >= 2) {
                currentWindow = Math.min(currentWindow * 2, maxWindow);
            }
        } else {
            // Random/Backwards access; reset sequential counter.
            sequentialStreak = 0;
            randomStreak++;

            // Shrink window after repeated random accesses
            // Keep counting to allow further shrinking if pattern continues
            if (randomStreak >= shrinkOnRandomThreshold) {
                currentWindow = Math.max(1, currentWindow / 2);
            }

            trigger = false; // disable readahead for random access patterns
        }

        lastOffset = currentOffset;
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

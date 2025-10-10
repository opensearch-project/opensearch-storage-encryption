/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

/**
 * Immutable configuration for AdaptiveReadaheadContext.
 *
 * Provides tuning for:
 *  - Initial readahead window size
 *  - Maximum readahead window segments
 *  - Cache hit streak threshold to disable RA
 *  - Random access streak threshold to shrink window
 *
 */
public final class WindowedReadAheadConfig {

    private final int initialWindow;
    private final int maxWindowBlocks;
    private final int hitStreakThreshold;
    private final int shrinkOnRandomThreshold;

    private WindowedReadAheadConfig(int initialWindow, int maxWindowBlocks, int hitStreakThreshold, int shrinkOnRandomThreshold) {
        this.initialWindow = initialWindow;
        this.maxWindowBlocks = maxWindowBlocks;
        this.hitStreakThreshold = hitStreakThreshold;
        this.shrinkOnRandomThreshold = shrinkOnRandomThreshold;
    }

    /**
     * Returns the initial window size for readahead operations.
     *
     * @return the initial number of segments to prefetch.
     */
    public int initialWindow() {
        return initialWindow;
    }

    /**
     * Returns the maximum allowed readahead window size.
     *
     * @return the maximum number of segments to prefetch in a window.
     */
    public int maxWindowSegments() {
        return maxWindowBlocks;
    }

    /**
     * Returns the hit streak threshold for window growth.
     *
     * @return the number of sequential hits required to grow the window.
     */
    public int hitStreakThreshold() {
        return hitStreakThreshold;
    }

    /**
     * Returns the random access threshold for window shrinking.
     *
     * @return the number of random accesses after which the window will shrink.
     */
    public int shrinkOnRandomThreshold() {
        return shrinkOnRandomThreshold;
    }

    /**
     * Creates a config with default values:
     * - initialWindow: 1
     * - maxWindowBlocks: 8  
     * - hitStreakThreshold: 4
     * - shrinkOnRandomThreshold: 2
     *
     * @return a new WindowedReadAheadConfig instance with default settings
     */
    public static WindowedReadAheadConfig defaultConfig() {
        return new WindowedReadAheadConfig(1, 8, 4, 2);
    }

    /**
     * Creates a config with custom values.
     *
     * @param initialWindow the initial number of segments to prefetch
     * @param maxWindowBlocks the maximum number of segments in the readahead window
     * @param hitStreakThreshold the number of sequential hits required to grow the window
     * @param shrinkOnRandomThreshold the number of random accesses after which the window will shrink
     * @return a new WindowedReadAheadConfig instance with the specified settings
     */
    public static WindowedReadAheadConfig of(int initialWindow, int maxWindowBlocks, int hitStreakThreshold, int shrinkOnRandomThreshold) {
        return new WindowedReadAheadConfig(initialWindow, maxWindowBlocks, hitStreakThreshold, shrinkOnRandomThreshold);
    }
}

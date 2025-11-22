/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

/**
 * Configuration for adaptive readahead.
 *
 * Controls:
 *  - initialWindow: Starting window size
 *  - maxWindow: Max window growth
 *  - randomAccessThreshold: Gap divisor (gap > window/threshold → random)
 */
public final class WindowedReadAheadConfig {

    private final int initialWindow;
    private final int maxWindow;
    private final int randomAccessThreshold;

    private WindowedReadAheadConfig(int initialWindow, int maxWindow, int randomAccessThreshold) {
        this.initialWindow = initialWindow;
        this.maxWindow = maxWindow;
        this.randomAccessThreshold = randomAccessThreshold;
    }

    /**
     * Returns the initial readahead window size.
     *
     * @return the starting window size
     */
    public int initialWindow() {
        return initialWindow;
    }

    /**
     * Returns the maximum number of segments in the readahead window.
     *
     * @return the max window growth size
     */
    public int maxWindowSegments() {
        return maxWindow;
    }

    /**
     * Returns the gap divisor for random detection: gap &gt; window/threshold → random access.
     *
     * @return the random access threshold
     */
    public int randomAccessThreshold() {
        return randomAccessThreshold;
    }

    /**
     * Creates a default configuration with init=4, max=32, randomThreshold=16.
     *
     * @return the default configuration
     */
    public static WindowedReadAheadConfig defaultConfig() {
        return new WindowedReadAheadConfig(4, 32, 16);
    }

    /**
     * Creates a custom configuration with specified parameters.
     *
     * @param initialWindow the starting window size
     * @param maxWindow the max window growth size
     * @param randomAccessThreshold the gap divisor for random detection
     * @return a new configuration with the specified values
     */
    public static WindowedReadAheadConfig of(int initialWindow, int maxWindow, int randomAccessThreshold) {
        return new WindowedReadAheadConfig(initialWindow, maxWindow, randomAccessThreshold);
    }
}

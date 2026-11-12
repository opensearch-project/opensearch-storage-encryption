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
 *  - minSequentialRun: Sequential steps a reader takes before readahead starts (0 = first access)
 */
public final class WindowedReadAheadConfig {

    private final int initialWindow;
    private final int maxWindow;
    private final int randomAccessThreshold;
    private final int minSequentialRun;

    /** Default sequential steps before readahead starts: a read, then two adjacent blocks. */
    static final int DEFAULT_MIN_SEQUENTIAL_RUN = 2;

    private WindowedReadAheadConfig(int initialWindow, int maxWindow, int randomAccessThreshold, int minSequentialRun) {
        this.initialWindow = initialWindow;
        this.maxWindow = maxWindow;
        this.randomAccessThreshold = randomAccessThreshold;
        this.minSequentialRun = minSequentialRun;
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
     * Returns how many sequential steps a reader takes before readahead starts.
     *
     * @return the minimum sequential run; 0 means the first access triggers
     */
    public int minSequentialRun() {
        return minSequentialRun;
    }

    /**
     * Creates a default configuration with init=4, max=32, randomThreshold=16, minSequentialRun=2.
     *
     * @return the default configuration
     */
    public static WindowedReadAheadConfig defaultConfig() {
        return new WindowedReadAheadConfig(4, 32, 16, DEFAULT_MIN_SEQUENTIAL_RUN);
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
        return of(initialWindow, maxWindow, randomAccessThreshold, DEFAULT_MIN_SEQUENTIAL_RUN);
    }

    /**
     * Creates a custom configuration with specified parameters.
     *
     * @param initialWindow the starting window size
     * @param maxWindow the max window growth size
     * @param randomAccessThreshold the gap divisor for random detection
     * @param minSequentialRun sequential steps before readahead starts (0 = first access)
     * @return a new configuration with the specified values
     */
    public static WindowedReadAheadConfig of(int initialWindow, int maxWindow, int randomAccessThreshold, int minSequentialRun) {
        return new WindowedReadAheadConfig(initialWindow, maxWindow, randomAccessThreshold, minSequentialRun);
    }
}

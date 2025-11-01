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

    public int initialWindow() {
        return initialWindow;
    }

    public int maxWindowSegments() {
        return maxWindow;
    }

    /** Gap divisor for random detection: gap > window/threshold → random access */
    public int randomAccessThreshold() {
        return randomAccessThreshold;
    }

    /** Default: init=4, max=32, randomThreshold=4 */
    public static WindowedReadAheadConfig defaultConfig() {
        return new WindowedReadAheadConfig(4, 32, 16);
    }

    public static WindowedReadAheadConfig of(int initialWindow, int maxWindow, int randomAccessThreshold) {
        return new WindowedReadAheadConfig(initialWindow, maxWindow, randomAccessThreshold);
    }
}

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
public final class AdaptiveReadaheadConfig {

    private final int initialWindow;
    private final int maxWindowSegments;
    private final int hitStreakThreshold;
    private final int shrinkOnRandomThreshold;

    private AdaptiveReadaheadConfig(Builder builder) {
        this.initialWindow = builder.initialWindow;
        this.maxWindowSegments = builder.maxWindowSegments;
        this.hitStreakThreshold = builder.hitStreakThreshold;
        this.shrinkOnRandomThreshold = builder.shrinkOnRandomThreshold;
    }

    /**
     * @return the initial number of segments to prefetch.
     */
    public int initialWindow() {
        return initialWindow;
    }

    /**
     * @return the maximum number of segments to prefetch in a window.
     */
    public int maxWindowSegments() {
        return maxWindowSegments;
    }

    /**
     * @return the number of sequential hits required to grow the window.
     */
    public int hitStreakThreshold() {
        return hitStreakThreshold;
    }

    /**
     * @return the number of random accesses after which the window will shrink.
     */
    public int shrinkOnRandomThreshold() {
        return shrinkOnRandomThreshold;
    }

    /**
     * Builder pattern for {@link AdaptiveReadaheadConfig}.
     */
    public static class Builder {

        /**
        * Initial number of segments to prefetch on first sequential read.
        * Default: 1
        */
        private int initialWindow = 1;

        /**
         * Maximum number of segments to prefetch in a single window.
         * Limits the growth of read-ahead.
         * Default: 8
         */
        private int maxWindowSegments = 8;

        /**
         * Number of consecutive sequential reads required
         * to grow the prefetch window.
         * Default: 4
         */
        private int hitStreakThreshold = 4;

        /**
         * Number of random (non-sequential) reads after which
         * the prefetch window will shrink.
         * Default: 2
         */
        private int shrinkOnRandomThreshold = 2;

        /** Sets the initial window size for prefetching. */
        public Builder initialWindow(int val) {
            this.initialWindow = val;
            return this;
        }

        /** Sets the maximum window size for prefetching. */
        public Builder maxWindowSegments(int val) {
            this.maxWindowSegments = val;
            return this;
        }

        /** Sets the number of sequential hits before growing the window. */
        public Builder hitStreakThreshold(int val) {
            this.hitStreakThreshold = val;
            return this;
        }

        /** Sets the number of random accesses before shrinking the window. */
        public Builder shrinkOnRandomThreshold(int val) {
            this.shrinkOnRandomThreshold = val;
            return this;
        }

        /**
         * Builds the {@link AdaptiveReadaheadConfig} instance.
         *
         * @return a new AdaptiveReadaheadConfig
         */
        public AdaptiveReadaheadConfig build() {
            return new AdaptiveReadaheadConfig(this);
        }
    }
}

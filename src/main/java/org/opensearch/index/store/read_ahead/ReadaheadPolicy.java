/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead;

/**
 * Defines how readahead should behave depending on access pattern.
 */
public interface ReadaheadPolicy {

    /**
     * Called on every segment access to update internal state.
     *
     * @param currentOffset current accessed file offset
     * @param segmentSize segment size in bytes
     * @return true if this access should trigger readahead
     */
    boolean shouldTrigger(long currentOffset, int segmentSize);

    /**
     * @return initial readahead window (in segments)
     */
    int initialWindow();

    /**
     * @return max readahead window (in segments)
     */
    int maxWindow();
}

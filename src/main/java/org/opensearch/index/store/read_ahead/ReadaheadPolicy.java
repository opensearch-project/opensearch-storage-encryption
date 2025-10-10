/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead;

/**
 * Defines how readahead should behave depending on access pattern.
 * 
 * <p>This interface allows different implementations to control when readahead operations
 * should be triggered and how the readahead window size should be managed based on
 * observed file access patterns.
 * 
 * @opensearch.internal
 */
public interface ReadaheadPolicy {

    /**
     * Called on every segment access to update internal state.
     *
     * @param currentOffset current accessed file offset
     * @return true if this access should trigger readahead
     */
    boolean shouldTrigger(long currentOffset);

    /**
     * Gets the initial readahead window size when starting readahead operations.
     * 
     * @return initial readahead window size in segments
     */
    int initialWindow();

    /**
     * Gets the current readahead window size based on observed access patterns.
     * 
     * @return current readahead window size in segments
     */
    int currentWindow();

    /**
     * Gets the maximum allowed readahead window size to prevent excessive prefetching.
     * 
     * @return maximum readahead window size in segments
     */
    int maxWindow();
}

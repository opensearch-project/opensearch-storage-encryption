/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead;

import java.io.Closeable;
import java.nio.file.Path;

/**
 * Asynchronous readahead worker interface.
 *
 * Implementations schedule block prefetching for sequential I/O
 * and deduplicate in-flight requests to avoid redundant reads.
 */
public interface Worker extends Closeable {

    /**
     * Schedule a prefetch request for blocks if not already in flight.
     *
     * @param <T> the type of cached block values
     * @param blockCache the directory-specific block cache to use for loading
     * @param path       file path to prefetch
     * @param offset     aligned block offset (in bytes)
     * @param blockCount number of blocks to prefetch
     * @return true if successfully scheduled or already in flight
     */
    <T extends AutoCloseable> boolean schedule(
        org.opensearch.index.store.block_cache.BlockCache<T> blockCache,
        Path path,
        long offset,
        long blockCount
    );

    /**
     * Checks if the worker is currently active and processing readahead requests.
     *
     * @return true if the worker is actively running
     */
    boolean isRunning();

    /**
     * Returns the current queue size (number of pending tasks).
     *
     * @return number of tasks in the queue
     */
    int getQueueSize();

    /**
     * Returns the queue capacity.
     *
     * @return maximum queue capacity
     */
    int getQueueCapacity();

    /**
     * Cancel all pending requests for a specific file path.
     *
     * @param path file path whose pending readahead should be canceled
     */
    void cancel(Path path);

    /**
     * Checks if readahead is currently paused due to cache thrashing.
     * This is a cheap volatile read meant to be called from the hot path.
     *
     * @return true if readahead should be paused
     */
    boolean isReadAheadPaused();

    /**
     * Close the worker and cancel all pending requests.
     */
    @Override
    void close();
}

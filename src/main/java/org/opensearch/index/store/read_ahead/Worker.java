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
     * @param path       file path to prefetch
     * @param offset     aligned block offset (in bytes)
     * @param blockCount number of blocks to prefetch
     * @return true if successfully scheduled or already in flight
     */
    boolean schedule(Path path, long offset, long blockCount);

    /**
     * Checks if the worker is currently active and processing readahead requests.
     *
     * @return true if the worker is actively running
     */
    boolean isRunning();

    /**
     * Cancel all pending requests for a specific file path.
     *
     * @param path file path whose pending readahead should be canceled
     */
    void cancel(Path path);

    /**
     * Close the worker and cancel all pending requests.
     */
    @Override
    void close();
}

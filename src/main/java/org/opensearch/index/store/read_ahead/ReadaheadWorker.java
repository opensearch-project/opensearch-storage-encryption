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
public interface ReadaheadWorker extends Closeable {

    /**
     * Schedule a prefetch request for a block if not already in flight.
     *
     * @param path   file path to prefetch
     * @param offset aligned block offset (in bytes)
     * @param length length in bytes to prefetch (aligned to block size)
     * @return true if successfully scheduled or already in flight
     */
    boolean schedule(Path path, long offset, int length);

    /**
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

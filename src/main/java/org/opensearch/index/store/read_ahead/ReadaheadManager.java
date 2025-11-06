/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead;

import java.io.Closeable;
import java.nio.file.Path;

/**
 * Central manager for coordinating readahead operations across multiple file streams.
 * 
 * <p>This interface provides the main coordination point for the readahead system, managing
 * multiple file streams and their associated readahead contexts. It handles registration of
 * files for readahead monitoring, cache hit/miss notifications, and lifecycle management.
 * 
 * <p>Implementations should be thread-safe as they may be called concurrently from multiple
 * index input streams accessing different files.
 * 
 * @opensearch.internal
 */
public interface ReadaheadManager extends Closeable {

    /**
     * Registers a file for readahead monitoring and returns a context for tracking its access patterns.
     * 
     * <p>This method creates a new readahead context that will be used to track access patterns
     * for the specified file. The context maintains state needed for readahead decision making
     * such as access history, current position, and readahead policy state.
     * 
     * @param path the file path to register for readahead monitoring
     * @param fileLength the total length of the file in bytes for boundary checking
     * @return a readahead context for tracking access patterns and triggering readahead operations
     */
    ReadaheadContext register(Path path, long fileLength);

    /**
     * Cancel all readahead for a given stream context.
     *
     * @param context the readahead context to cancel
     */
    void cancel(ReadaheadContext context);

    /**
     * Cancel all pending requests for a given file.
     *
     * @param path file path to cancel
     */
    void cancel(Path path);

    /**
     * Shutdown the entire readahead system, canceling all contexts and workers.
     */
    @Override
    void close();
}

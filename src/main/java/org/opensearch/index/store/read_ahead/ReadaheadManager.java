/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead;

import java.io.Closeable;
import java.nio.file.Path;

public interface ReadaheadManager extends Closeable {

    ReadaheadContext register(Path path, long fileLength);

    /**
     * Notify that a segment was accessed, possibly triggering readahead.
     *
     * @param context       per-index input context
     * @param startFileOffset  the fileoffset from where we start reading.
     * @param cacheMiss     true if the block was not in cache (enables RA)
     */
    void onSegmentAccess(ReadaheadContext context, long startFileOffset, boolean cacheMiss);

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

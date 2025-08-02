/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead;

import java.io.Closeable;
import java.nio.file.Path;

/**
 * High-level facade for managing readahead operations for DirectIO IndexInputs.
 * <p>
 * Coordinates:
 * <ul>
 *     <li>Per-stream {@link ReadaheadContext}</li>
 *     <li>Adaptive windowing policy</li>
 *     <li>Async scheduling to a {@link ReadaheadWorker}</li>
 * </ul>
 *
 * Typical flow:
 * <pre>
 *   ReadaheadContext ctx = manager.register(path, segmentSize);
 *
 *   // on each segment load in IndexInput:
 *   manager.onSegmentAccess(ctx, segmentIndex, cacheMiss);
 *
 *   // on close:
 *   manager.cancel(ctx);
 * </pre>
 */
public interface ReadaheadManager extends Closeable {

    ReadaheadContext register(Path path, int segmentSize, int segmentSizePower, long fileLength);

    /**
     * Notify that a segment was accessed, possibly triggering readahead.
     *
     * @param context       per-index input context
     * @param segmentIndex  the segment index that was accessed
     * @param cacheMiss     true if the block was not in cache (enables RA)
     */
    void onSegmentAccess(ReadaheadContext context, int segmentIndex, boolean cacheMiss);

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

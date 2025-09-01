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
     * Notify that a cache miss occurred, possibly triggering readahead.
     *
     * @param context       per-index input context
     * @param startFileOffset  the fileoffset from where we start reading.
     */
    void onCacheMiss(ReadaheadContext context, long startFileOffset);

    /**
     * Notify that a cache hit occurred to track hit streaks.
     *
     * @param context the readahead context
     */
    void onCacheHit(ReadaheadContext context);

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

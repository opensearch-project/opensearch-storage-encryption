/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;
import org.opensearch.index.store.read_ahead.ReadaheadWorker;

/**
 * Simplified ReadaheadManager for single IndexInput.
 *
 * - Holds a single ReadaheadContext for the lifetime of the IndexInput.
 * - Delegates scheduling to a ReadaheadWorker.
 * - No registry map needed because lifetime is tied to IndexInput.
 */
public class ReadaheadManagerImpl implements ReadaheadManager {

    private static final Logger LOGGER = LogManager.getLogger(ReadaheadManagerImpl.class);

    private final ReadaheadWorker worker;
    private volatile boolean closed = false;
    private ReadaheadContext context;

    public ReadaheadManagerImpl(ReadaheadWorker worker) {
        this.worker = worker;
    }

    @Override
    public ReadaheadContext register(Path path, int segmentSize, int segmentSizePower, long fileLength) {
        if (closed) {
            throw new IllegalStateException("ReadaheadManager is closed");
        }

        AdaptiveReadaheadConfig config = new AdaptiveReadaheadConfig.Builder()
            .initialWindow(1)
            .maxWindowSegments(8)
            .hitStreakThreshold(5)
            .shrinkOnRandomThreshold(3)
            .build();

        this.context = AdaptiveReadaheadContext.build(path, segmentSize, segmentSizePower, fileLength, worker, config);

        LOGGER.debug("Registered readahead context for {}", path);
        return this.context;
    }

    @Override
    public void onSegmentAccess(ReadaheadContext ctx, int segmentIndex, boolean cacheMiss) {
        if (closed || ctx == null)
            return;
        AdaptiveReadaheadContext arc = (AdaptiveReadaheadContext) ctx;

        // Compute byte offset using shift (segmentIndex * segmentSize)
        long fileOffset = ((long) segmentIndex) << arc.segmentSizePower();

        arc.onSegmentAccess(fileOffset, cacheMiss);
    }

    @Override
    public void cancel(ReadaheadContext ctx) {
        if (ctx != null) {
            ctx.close();
            LOGGER.debug("Cancelled readahead for context {}", ctx);
        }
    }

    @Override
    public void cancel(Path path) {
        if (context != null) {
            context.close();
            LOGGER.debug("Cancelled readahead for {}", path);
        }
    }

    @Override
    public void close() {
        try (worker) {
            closed = true;
            try {
                if (context != null) {
                    context.close();
                }
            } catch (Exception e) {
                LOGGER.warn("Error closing readahead context", e);
            }
        }
    }
}

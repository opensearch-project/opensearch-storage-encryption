/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;
import org.opensearch.index.store.read_ahead.Worker;

/**
 * ReadaheadManager for single IndexInput.
 *
 * - Holds a single ReadaheadContext for the lifetime of the IndexInput.
 * - Delegates scheduling to a ReadAheadWorker.
 */
public class ReadaheadManagerImpl implements ReadaheadManager {

    private static final Logger LOGGER = LogManager.getLogger(ReadaheadManagerImpl.class);

    private final Worker worker;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private ReadaheadContext context;

    public ReadaheadManagerImpl(Worker worker) {
        this.worker = worker;
    }

    @Override
    public synchronized ReadaheadContext register(Path path, long fileLength) {
        if (closed.get()) {
            throw new IllegalStateException("ReadaheadManager is closed");
        }
        if (context != null) {
            throw new IllegalStateException("ReadaheadContext already registered");
        }

        WindowedReadAheadConfig config = WindowedReadAheadConfig.of(4, 16, 4, 50);

        this.context = WindowedReadAheadContext.build(path, fileLength, worker, config);

        return this.context;
    }

    @Override
    public void onCacheMiss(ReadaheadContext ctx, long startFileOffset) {
        if (closed.get() || ctx == null) {
            return;
        }
        ctx.onCacheMiss(startFileOffset);
    }

    @Override
    public void onCacheHit(ReadaheadContext ctx) {
        if (closed.get() || ctx == null) {
            return;
        }
        ctx.onCacheHit();
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
            context = null;
            LOGGER.debug("Cancelled readahead for {}", path);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
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

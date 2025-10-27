/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;
import org.opensearch.index.store.read_ahead.Worker;

/**
 * Simple readahead manager implementation designed for single IndexInput usage.
 * 
 * <p>This implementation provides a straightforward approach to readahead management by maintaining
 * a single readahead context per manager instance. It's designed for scenarios where each IndexInput
 * gets its own dedicated manager, providing isolated readahead behavior per file stream.
 * 
 * <p>Key characteristics:
 * <ul>
 * <li><strong>Single context:</strong> Maintains exactly one ReadaheadContext for the lifetime of the manager</li>
 * <li><strong>Worker delegation:</strong> Delegates all actual prefetch scheduling to the underlying Worker</li>
 * <li><strong>Default configuration:</strong> Uses predefined WindowedReadAheadConfig with reasonable defaults</li>
 * <li><strong>Lifecycle management:</strong> Provides proper cleanup and state management for contexts</li>
 * <li><strong>Thread safety:</strong> Uses synchronization and atomic operations for safe concurrent access</li>
 * </ul>
 * 
 * <p>The manager automatically creates a WindowedReadAheadContext with default configuration when
 * a file is registered, making it suitable for most standard use cases without requiring detailed
 * readahead configuration.
 * 
 * @opensearch.internal
 */
public class ReadaheadManagerImpl implements ReadaheadManager {

    private static final Logger LOGGER = LogManager.getLogger(ReadaheadManagerImpl.class);

    private final Worker worker;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private ReadaheadContext context;
    private final Thread processingThread;

    /**
     * Creates a new readahead manager that delegates prefetch operations to the specified worker.
     *
     * <p>The manager will use the provided worker to schedule and execute all readahead operations.
     * The worker should be properly configured and running before being passed to this constructor.
     *
     * <p>A background thread is started to process pending access notifications asynchronously,
     * keeping the hot path (block access) extremely fast.
     *
     * @param worker the worker instance to handle readahead scheduling and execution
     * @throws NullPointerException if worker is null
     */
    public ReadaheadManagerImpl(Worker worker) {
        this.worker = worker;

        // Start background thread to process access notifications
        this.processingThread = new Thread(this::processAccessLoop, "readahead-access-processor");
        this.processingThread.setDaemon(true);
        this.processingThread.start();
    }

    /**
     * Background thread loop that processes pending access notifications.
     * Runs continuously until the manager is closed.
     *
     * <p>Uses park/unpark for low-latency event-driven wakeups instead of polling.
     */
    private void processAccessLoop() {
        while (!closed.get()) {
            try {
                // Process queued readahead tasks if context exists
                ReadaheadContext ctx = this.context;
                boolean processed = false;

                if (ctx != null) {
                    processed = ctx.processQueue();
                }

                // Park if nothing was processed and queue is empty
                if (!processed && ctx != null && !ctx.hasQueuedWork()) {
                    LockSupport.park();
                }
            } catch (Exception e) {
                LOGGER.warn("Error processing readahead access notification", e);
            }
        }
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

        // Pass processing thread reference for unpark notifications
        this.context = WindowedReadAheadContext.build(path, fileLength, worker, config, processingThread);

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
                // Interrupt and wait for processing thread to stop
                processingThread.interrupt();
                try {
                    processingThread.join(1000); // Wait up to 1 second
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                // Close context
                if (context != null) {
                    context.close();
                }
            } catch (Exception e) {
                LOGGER.warn("Error closing readahead manager", e);
            }
        }
    }
}

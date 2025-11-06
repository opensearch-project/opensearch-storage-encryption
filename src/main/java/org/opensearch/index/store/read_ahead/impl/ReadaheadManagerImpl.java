/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;
import org.opensearch.index.store.read_ahead.Worker;

/**
 * Lightweight readahead manager implementation for single IndexInput usage.
 *
 * <p>This implementation provides a minimal coordination layer between the hot path
 * (IndexInput reads) and the Worker threads that perform actual I/O. It's designed
 * for scenarios where each IndexInput gets its own dedicated manager, providing
 * isolated readahead behavior per file stream.
 *
 * <p>Key characteristics:
 * <ul>
 * <li><strong>Single context:</strong> Maintains exactly one ReadaheadContext for the lifetime of the manager</li>
 * <li><strong>Worker delegation:</strong> Delegates all actual prefetch scheduling to the underlying Worker</li>
 * <li><strong>Threadless coordination:</strong> No dedicated threads - processes work inline when signaled</li>
 * <li><strong>Lock-protected:</strong> Uses ReentrantLock to ensure atomic signal+process (prevents lost work)</li>
 * <li><strong>Rate-limited:</strong> Relies on context's rate limiting (300µs) to avoid hot path overhead</li>
 * <li><strong>Default configuration:</strong> Uses predefined WindowedReadAheadConfig with reasonable defaults</li>
 * <li><strong>Lifecycle management:</strong> Provides proper cleanup and state management for contexts</li>
 * </ul>
 *
 * <p>The manager coordinates readahead operations without maintaining dedicated threads.
 * Work is processed inline when signaled, with rate limiting provided by the context layer.
 * A lock ensures that no readahead work is lost due to race conditions between signaling
 * and processing.
 *
 * @opensearch.internal
 */
public class ReadaheadManagerImpl implements ReadaheadManager {

    private static final Logger LOGGER = LogManager.getLogger(ReadaheadManagerImpl.class);

    private final Worker worker;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private ReadaheadContext context;

    // Lock to ensure atomicity of signal + processWork
    private final Lock lock = new ReentrantLock();

    /**
     * Creates a new readahead manager that delegates prefetch operations to the specified worker.
     *
     * <p>The manager provides lightweight coordination without dedicated threads.
     * The worker should be properly configured and running before being passed to this constructor.
     *
     * @param worker the worker instance to handle readahead scheduling and execution
     * @throws NullPointerException if worker is null
     */
    public ReadaheadManagerImpl(Worker worker) {
        this.worker = worker;
    }

    /**
     * Signal that work is available for processing.
     * Called by the readahead context when new readahead requests need to be scheduled.
     *
     * <p>This method is called from the hot path but is already rate-limited (300µs intervals)
     * by the context, so we can safely process work inline without adding significant latency.
     *
     * <p>Uses a lock to ensure atomicity: no work is lost between signal and processWork.
     */
    void signal() {
        lock.lock();
        try {
            processWork();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public synchronized ReadaheadContext register(Path path, long fileLength) {
        if (closed.get())
            throw new IllegalStateException("ReadaheadManager is closed");
        if (context != null)
            throw new IllegalStateException("ReadaheadContext already registered");

        WindowedReadAheadConfig config = WindowedReadAheadConfig.defaultConfig();
        this.context = WindowedReadAheadContext.build(path, fileLength, worker, config, this::signal);

        return this.context;
    }

    /**
    * Process pending readahead work.
    * Drains the readahead queue and submits tasks to the worker.
    *
    * <p>MUST be called under lock to prevent race conditions.
    *
    * @return true if work was processed, false otherwise
    */
    private boolean processWork() {
        if (closed.get()) {
            return false;
        }

        ReadaheadContext ctx = this.context;
        if (ctx == null) {
            return false;
        }

        return ctx.processQueue();
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

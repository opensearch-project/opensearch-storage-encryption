/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE_POWER;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadPolicy;
import org.opensearch.index.store.read_ahead.Worker;

/**
 * Windowed readahead context implementation that manages adaptive prefetching
 * for sequential file access patterns.
 * 
 * <p>This implementation uses a configurable window-based readahead strategy
 * that adapts to access patterns. It coordinates with a Worker to schedule
 * bulk prefetch operations and integrates with cache miss/hit feedback to
 * optimize readahead behavior.
 *
 * @opensearch.internal
 */
public class WindowedReadAheadContext implements ReadaheadContext {
    private static final Logger LOGGER = LogManager.getLogger(WindowedReadAheadContext.class);

    private final Path path;
    private final long fileLength;
    private final Worker worker;
    private final WindowedReadaheadPolicy policy;

    // Removed cache-awareness - let worker handle cache decisions

    // Scheduling state (per file)
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private WindowedReadAheadContext(Path path, long fileLength, Worker worker, WindowedReadaheadPolicy policy) {
        this.path = path;
        this.fileLength = fileLength;
        this.worker = worker;
        this.policy = policy;
    }

    /**
     * Creates a new WindowedReadAheadContext with the specified configuration.
     *
     * @param path the file path for readahead operations
     * @param fileLength the total length of the file in bytes
     * @param worker the worker to schedule readahead operations
     * @param config the readahead configuration settings
     * @return a new WindowedReadAheadContext instance
     */
    public static WindowedReadAheadContext build(Path path, long fileLength, Worker worker, WindowedReadAheadConfig config) {
        var policy = new WindowedReadaheadPolicy(
            path,
            config.initialWindow(),
            config.maxWindowSegments(),
            config.shrinkOnRandomThreshold()
        );
        return new WindowedReadAheadContext(path, fileLength, worker, policy);
    }

    @Override
    public void onCacheMiss(long fileOffset) {
        if (closed.get())
            return;

        // Cache miss - check if we should trigger readahead
        if (!policy.shouldTrigger(fileOffset)) {
            return;
        }

        trigger(fileOffset);
    }

    @Override
    public void onCacheHit() {
        if (closed.get())
            return;

        policy.onCacheHit();
    }

    private void trigger(long anchorFileOffset) {
        if (closed.get() || worker == null)
            return;

        final long startSeg = anchorFileOffset >>> CACHE_BLOCK_SIZE_POWER;
        final long lastSeg = (fileLength - 1) >>> CACHE_BLOCK_SIZE_POWER;
        final long safeEndSeg = Math.max(0, lastSeg - 3); // Skip last 4 segments (footer)

        final long windowSegs = policy.currentWindow();
        if (windowSegs <= 0 || startSeg > safeEndSeg)
            return;

        final long endExclusive = Math.min(startSeg + windowSegs, safeEndSeg + 1);
        if (startSeg >= endExclusive)
            return;

        final long blockCount = endExclusive - startSeg;

        if (blockCount > 0) {
            // schedule the entire window.
            final boolean accepted = worker.schedule(path, anchorFileOffset, blockCount);
            LOGGER
                .debug(
                    "RA_BULK_TRIGGER path={} anchorOff={} startSeg={} endExclusive={} windowSegs={} scheduledBlocks={} accepted={}",
                    path,
                    anchorFileOffset,
                    startSeg,
                    endExclusive,
                    windowSegs,
                    blockCount,
                    accepted
                );

            if (!accepted) {
                LOGGER
                    .info(
                        "Window bulk readahead backpressure path={} length={} startSeg={} endExclusive={} windowBlocks={}",
                        path,
                        fileLength,
                        startSeg,
                        endExclusive,
                        blockCount
                    );
            }
        }
    }

    @Override
    public ReadaheadPolicy policy() {
        return this.policy;
    }

    @Override
    public void triggerReadahead(long fileOffset) {
        trigger(fileOffset);
    }

    @Override
    public void reset() {
        policy.reset();
    }

    @Override
    public void cancel() {
        if (worker != null) {
            worker.cancel(path);
        }
    }

    @Override
    public boolean isReadAheadEnabled() {
        return !closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            cancel();
        }
    }
}

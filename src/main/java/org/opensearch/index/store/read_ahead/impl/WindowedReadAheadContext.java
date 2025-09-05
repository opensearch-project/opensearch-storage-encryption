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

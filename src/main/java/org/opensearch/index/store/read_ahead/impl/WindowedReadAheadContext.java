/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE;
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

    // Cache-awareness
    private final int hitStreakThreshold;
    private int cacheHitStreak = 0;
    private boolean readaheadEnabled = true;

    // Scheduling state (per file)
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private WindowedReadAheadContext(Path path, long fileLength, Worker worker, int hitStreakThreshold, WindowedReadaheadPolicy policy) {
        this.path = path;
        this.fileLength = fileLength;
        this.worker = worker;
        this.hitStreakThreshold = hitStreakThreshold;
        this.policy = policy;
    }

    public static WindowedReadAheadContext build(Path path, long fileLength, Worker worker, WindowedReadAheadConfig config) {
        var policy = new WindowedReadaheadPolicy(
            path,
            config.initialWindow(),
            config.maxWindowSegments(),
            config.shrinkOnRandomThreshold()
        );
        return new WindowedReadAheadContext(path, fileLength, worker, config.hitStreakThreshold(), policy);
    }

    @Override
    public synchronized void onSegmentAccess(long fileOffset, boolean cacheMiss) {
        if (closed.get())
            return;

        // Let worker know how far FG has advanced (for drop-behind)
        if (worker instanceof QueuingWorker qw) {
            qw.updateMinUsefulOffset(path, fileOffset);
        }

        // Simple cache-awareness with light hysteresis
        if (cacheMiss) {
            readaheadEnabled = true;
            cacheHitStreak = 0;
        } else if (readaheadEnabled) {
            if (++cacheHitStreak >= hitStreakThreshold) {
                readaheadEnabled = false;
                cacheHitStreak = 0; // reset for next enable phase
            }
        }

        if (!readaheadEnabled)
            return;

        if (!policy.shouldTrigger(fileOffset)) {
            return;
        }

        trigger(fileOffset);
    }

    private void trigger(long anchorFileOffset) {
        if (closed.get() || worker == null)
            return;

        final long currSeg = anchorFileOffset >>> CACHE_BLOCK_SIZE_POWER;
        final long startSeg = currSeg + 2;

        final long lastSeg = (fileLength - 1) >>> CACHE_BLOCK_SIZE_POWER;
        final long safeEndSeg = Math.max(0, lastSeg - 3); // Skip footers (last 4 segments)

        final long windowSegs = policy.currentWindow();
        // Clamp end: no more than windowSegs, and not past safeEndSeg
        final long endExclusive = Math.min(startSeg + windowSegs, safeEndSeg + 1);

        if (startSeg >= endExclusive) {
            return;
        }

        int scheduled = 0;
        long firstAccepted = -1L, firstRejected = -1L;

        for (long seg = startSeg; seg < endExclusive; seg++) {
            final long off = seg << CACHE_BLOCK_SIZE_POWER;
            final long remaining = fileLength - off;
            if (remaining <= 0) {
                break;
            }

            final int len = (int) Math.min(CACHE_BLOCK_SIZE, remaining);

            if (worker.schedule(path, off, len)) {
                if (firstAccepted == -1L)
                    firstAccepted = seg;
                scheduled++;
            } else {
                firstRejected = seg; // queue backpressure
                LOGGER
                    .info("There is backpressure path={} length={} startSeg={} endExclusive={}", path, fileLength, startSeg, endExclusive);
                break;
            }
        }

        LOGGER
            .debug(
                "RA_TRIGGER path={} anchorOff={} startSeg={} endExclusive={} windowSegs={} scheduled={} firstAccepted={} firstRejected={}",
                path,
                anchorFileOffset,
                startSeg,
                endExclusive,
                windowSegs,
                scheduled,
                firstAccepted,
                firstRejected
            );

    }

    @Override
    public ReadaheadPolicy policy() {
        return this.policy;
    }

    @Override
    public synchronized void triggerReadahead(long fileOffset) {
        trigger(fileOffset);
    }

    @Override
    public synchronized void reset() {
        policy.reset();
        cacheHitStreak = 0;
        readaheadEnabled = true;
    }

    @Override
    public void cancel() {
        if (worker != null) {
            worker.cancel(path);
        }
    }

    @Override
    public boolean isReadAheadEnabled() {
        return readaheadEnabled;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            cancel();
        }
    }
}

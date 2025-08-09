/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE;
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE_POWER;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
    private final AtomicLong lastScheduledSegment = new AtomicLong(-1L);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public WindowedReadAheadContext(Path path, long fileLength, Worker worker, int hitStreakThreshold, WindowedReadaheadPolicy policy) {
        this.path = path;
        this.fileLength = fileLength;
        this.worker = worker;
        this.hitStreakThreshold = hitStreakThreshold;
        this.policy = policy;
    }

    public static WindowedReadAheadContext build(Path path, long fileLength, Worker worker, WindowedReadAheadConfig config) {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(
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

        if (LOGGER.isTraceEnabled()) {
            final long seg = fileOffset >>> CACHE_BLOCK_SIZE_POWER; // fileOffset should be aligned by caller
            final int winSnapshot = policy.currentWindow();
            final int leadSnapshot = Math.max(1, winSnapshot / 2);
            LOGGER
                .trace(
                    "FG_ACCESS path={} off={} seg={} miss={} win={} lead={} lastSched={}",
                    path,
                    fileOffset,
                    seg,
                    cacheMiss,
                    winSnapshot,
                    leadSnapshot,
                    lastScheduledSegment.get()
                );
        }

        // Simple cache-awareness: disable readahead after a run of hits
        if (cacheMiss) {
            readaheadEnabled = true;
            cacheHitStreak = 0;
        } else {
            if (++cacheHitStreak >= hitStreakThreshold) {
                readaheadEnabled = false;
            }
        }

        if (!readaheadEnabled)
            return;

        // Window policy decides if we should trigger for this offset
        if (!policy.shouldTrigger(fileOffset))
            return;

        final int window = policy.currentWindow();
        final long currentSeg = fileOffset >>> CACHE_BLOCK_SIZE_POWER;

        // Choose a small lookahead within the window so we don't start exactly at the current seg
        final int lead = Math.max(1, window / 2);

        // Don’t re-schedule segments we already asked for
        final long startSegmentIndex = Math.max(currentSeg + lead, lastScheduledSegment.get() + 1);

        triggerReadahead(fileOffset, startSegmentIndex, window);
    }

    private void triggerReadahead(long fileOffset, long startSegmentIndex, int windowSegments) {
        if (closed.get() || worker == null)
            return;

        final long maxSegmentIndex = fileLength >>> CACHE_BLOCK_SIZE_POWER;

        // Skip very end-of-file to avoid tiny trailing reads
        if (startSegmentIndex >= (maxSegmentIndex - 2)) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Skipping readahead near EOF: startSeg={} maxSeg={}", startSegmentIndex, maxSegmentIndex);
            }
            return;
        }

        final long endSegmentIndex = Math.min(startSegmentIndex + windowSegments, maxSegmentIndex + 1);

        int scheduled = 0;
        int skippedAlreadyScheduled = 0;
        boolean stoppedByQueue = false;

        for (long seg = startSegmentIndex; seg < endSegmentIndex; seg++) {
            if (seg <= lastScheduledSegment.get()) {
                skippedAlreadyScheduled++;
                continue;
            }

            final long segmentOffset = seg << CACHE_BLOCK_SIZE_POWER;
            if (segmentOffset >= fileLength)
                break;

            final long remaining = fileLength - segmentOffset;
            final int length = (int) Math.min(CACHE_BLOCK_SIZE, remaining);

            if (LOGGER.isTraceEnabled()) {
                LOGGER
                    .trace(
                        "RA_TRY path={} startSeg={} seg={} off={} win={} len={} lastSched={}",
                        path,
                        startSegmentIndex,
                        seg,
                        segmentOffset,
                        windowSegments,
                        length,
                        lastScheduledSegment.get()
                    );
            }

            final boolean accepted = worker.schedule(path, segmentOffset, length);

            if (accepted) {
                lastScheduledSegment.lazySet(seg);
                scheduled++;
            } else {
                stoppedByQueue = true;
                break;
            }
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER
                .trace(
                    "RA_TRIGGER path={} win={} startSeg={} endSeg={} scheduled={} skipped_due_to_lastSched={} stoppedByQueue={} lastSched={}",
                    path,
                    windowSegments,
                    startSegmentIndex,
                    (endSegmentIndex - 1),
                    scheduled,
                    skippedAlreadyScheduled,
                    stoppedByQueue,
                    lastScheduledSegment.get()
                );
        }
    }

    @Override
    public ReadaheadPolicy policy() {
        return this.policy;
    }

    @Override
    public synchronized void triggerReadahead(long fileOffset, long startSegmentIndex) {
        triggerReadahead(fileOffset, startSegmentIndex, policy.currentWindow());
    }

    @Override
    public synchronized void reset() {
        policy.reset();
        cacheHitStreak = 0;
        readaheadEnabled = true;
        lastScheduledSegment.set(-1L);
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

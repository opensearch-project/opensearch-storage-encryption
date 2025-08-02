/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadWorker;

/**
 * Adaptive per-IndexInput readahead context.
 *
 * Delegates windowing and sequential/random detection to {@link AdaptiveReadaheadPolicy},
 * while handling cache-hit awareness and scheduling of async prefetches.
 */
public class AdaptiveReadaheadContext implements ReadaheadContext {

    private final Path path;
    private final int segmentSizePower;
    private final int segmentSize;

    private final long fileLength;
    private final ReadaheadWorker worker;
    private final AdaptiveReadaheadPolicy policy;

    // ----- Cache-awareness state -----
    private final int hitStreakThreshold;
    private int cacheHitStreak = 0;
    private boolean readaheadEnabled = true;

    // ----- Scheduling state -----
    private int lastScheduledSegment = -1;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public AdaptiveReadaheadContext(
        Path path,
        int segmentSize,
        int segmentSizePower,
        long fileLength,
        ReadaheadWorker worker,
        int hitStreakThreshold,
        AdaptiveReadaheadPolicy policy
    ) {
        this.path = path;
        this.segmentSize = segmentSize;
        this.segmentSizePower = segmentSizePower;
        this.fileLength = fileLength;
        this.worker = worker;
        this.hitStreakThreshold = hitStreakThreshold;
        this.policy = policy;
    }

    public static AdaptiveReadaheadContext build(
        Path path,
        int segmentSize,
        int segmentSizePower,
        long fileLength,
        ReadaheadWorker worker,
        AdaptiveReadaheadConfig config
    ) {
        AdaptiveReadaheadPolicy policy = new AdaptiveReadaheadPolicy(
            config.initialWindow(),
            config.maxWindowSegments(),
            config.shrinkOnRandomThreshold()
        );

        return new AdaptiveReadaheadContext(path, segmentSize, segmentSizePower, fileLength, worker, config.hitStreakThreshold(), policy);
    }

    @Override
    public synchronized void onSegmentAccess(long fileOffset, boolean cacheMiss) {
        if (closed.get())
            return;

        // 1. Cache-awareness
        if (cacheMiss) {
            readaheadEnabled = true;
            cacheHitStreak = 0;
        } else {
            cacheHitStreak++;
            if (cacheHitStreak >= hitStreakThreshold) {
                readaheadEnabled = false;
            }
        }

        // 2. Policy-driven sequential/random detection
        boolean trigger = policy.shouldTrigger(fileOffset, segmentSize);

        // 3. Trigger async readahead if conditions allow
        if (readaheadEnabled && trigger) {
            int window = policy.currentWindow();

            int startSegmentIndex = (int) (fileOffset >>> segmentSizePower) + 1;

            triggerReadahead(startSegmentIndex, window);
        }
    }

    private void triggerReadahead(int startSegmentIndex, int windowSegments) {
        if (closed.get() || worker == null)
            return;

        int endSegmentIndex = startSegmentIndex + windowSegments;
        for (int seg = startSegmentIndex; seg < endSegmentIndex; seg++) {

            if (seg <= lastScheduledSegment) {
                continue;
            }
            lastScheduledSegment = seg;

            // Compute offset in bytes using shift
            long offset = ((long) seg) << segmentSizePower;
            if (offset >= fileLength)
                break;

            long remaining = fileLength - offset;
            int length = (int) Math.min(segmentSize, remaining);  // length in bytes

            worker.schedule(path, offset, length);
        }
    }

    @Override
    public int currentWindowSegments() {
        return policy.currentWindow();
    }

    @Override
    public synchronized void triggerReadahead(int startSegmentIndex) {
        triggerReadahead(startSegmentIndex, policy.currentWindow());
    }

    @Override
    public synchronized void reset() {
        policy.reset();
        cacheHitStreak = 0;
        readaheadEnabled = true;
        lastScheduledSegment = -1;
    }

    @Override
    public void cancel() {
        if (worker != null) {
            worker.cancel(path);
        }
    }

    @Override
    public boolean isReadaheadEnabled() {
        return readaheadEnabled;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            cancel();
        }
    }

    public int segmentSizePower() {
        return segmentSizePower;
    }

    public int segmentSize() {
        return segmentSize;
    }
}

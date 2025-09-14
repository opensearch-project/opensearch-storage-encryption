/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;
import org.opensearch.index.store.read_ahead.Worker;

public class QueuingWorker implements Worker {

    private static final Logger LOGGER = LogManager.getLogger(QueuingWorker.class);

    private static final class ReadAheadTask {
        final Path path;
        final long offset;
        final long blockCount;
        final long enqueuedNanos;
        long startNanos;
        long doneNanos;

        ReadAheadTask(Path path, long offset, long blockCount) {
            this.path = path;
            this.offset = offset;
            this.blockCount = blockCount;
            this.enqueuedNanos = System.nanoTime();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ReadAheadTask))
                return false;
            ReadAheadTask other = (ReadAheadTask) obj;
            return path.equals(other.path) && offset == other.offset && blockCount == other.blockCount;
        }

        @Override
        public int hashCode() {
            return path.hashCode() * 31 + Long.hashCode(offset) * 13 + Long.hashCode(blockCount);
        }
    }

    private final BlockingDeque<ReadAheadTask> queue;
    private final int queueCapacity;
    private final ExecutorService executor;
    private final Set<ReadAheadTask> inFlight;

    private final BlockCache<RefCountedMemorySegment> blockCache;
    private volatile boolean closed = false;

    private static final AtomicInteger WORKER_ID = new AtomicInteger();

    private static final class CacheGap {
        final long blockIndex;
        final long blockCount;

        CacheGap(long blockIndex, long blockCount) {
            this.blockIndex = blockIndex;
            this.blockCount = blockCount;
        }

        @Override
        public String toString() {
            return String.format("CacheGap{blockIndex=%d, blocks=%d}", blockIndex, blockCount);
        }
    }

    public QueuingWorker(int queueCapacity, int threads, BlockCache<RefCountedMemorySegment> blockCache) {
        this.queue = new LinkedBlockingDeque<>(queueCapacity);
        this.queueCapacity = queueCapacity;
        this.inFlight = ConcurrentHashMap.newKeySet();
        this.blockCache = blockCache;

        this.executor = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "readahead-worker-" + WORKER_ID.incrementAndGet());
            t.setDaemon(true);
            return t;
        });

        for (int i = 0; i < threads; i++) {
            executor.submit(this::processLoop);
        }
    }

    @Override
    public boolean schedule(Path path, long offset, long blockCount) {
        if (closed) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Schedule on closed worker path={} off={}", path, offset);
            }
            return false;
        }

        // Cache-aware scheduling: find uncached regions
        List<CacheGap> uncachedGaps = findUncachedRanges(path, offset, blockCount);

        if (uncachedGaps.isEmpty()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("All blocks cached, skipping: path={} off={} blocks={}", path, offset, blockCount);
            }
            return true;
        }

        // Consolidate gaps using initialWindow (4) as merge threshold
        List<CacheGap> consolidatedGaps = consolidateGaps(uncachedGaps, 4);

        if (LOGGER.isDebugEnabled()) {
            LOGGER
                .debug(
                    "Cache gaps: path={} orig={} consolidated={} totalBlocks={}",
                    path,
                    uncachedGaps.size(),
                    consolidatedGaps.size(),
                    blockCount
                );
        }

        // Schedule consolidated gaps
        boolean allAccepted = true;
        for (CacheGap gap : consolidatedGaps) {
            boolean accepted = scheduleGap(path, gap);
            allAccepted &= accepted;
        }

        return allAccepted;
    }

    /**
     * Check if the given block range overlaps with any in-flight task.
     * Uses exact overlap detection without padding for correctness.
     */
    private boolean hasOverlappingInFlight(Path path, long blockIndex, long blockCount) {
        long end = blockIndex + blockCount;

        for (ReadAheadTask task : inFlight) {
            if (!task.path.equals(path)) {
                continue;
            }

            long taskStart = task.offset >>> CACHE_BLOCK_SIZE_POWER;
            long taskEnd = taskStart + task.blockCount;

            // Standard interval overlap: [a,b) and [c,d) overlap if max(a,c) < min(b,d)
            if (Math.max(blockIndex, taskStart) < Math.min(end, taskEnd)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Schedule a single cache gap for loading.
     */
    private boolean scheduleGap(Path path, CacheGap gap) {
        // Check for overlapping in-flight tasks first
        if (hasOverlappingInFlight(path, gap.blockIndex, gap.blockCount)) {
            return true; // Treat as accepted (avoid duplicate work)
        }

        final long offset = gap.blockIndex << CACHE_BLOCK_SIZE_POWER;
        final ReadAheadTask task = new ReadAheadTask(path, offset, gap.blockCount);
        if (!inFlight.add(task)) {
            return true; // Already queued (exact match)
        }

        final boolean accepted = queue.offerLast(task);

        if (!accepted) {
            inFlight.remove(task);
            LOGGER
                .warn(
                    "Queue full, dropping gap path={} blockIdx={} blocks={} qsz={}/{}",
                    path,
                    gap.blockIndex,
                    gap.blockCount,
                    queue.size(),
                    queueCapacity
                );
            return false;
        }

        if (LOGGER.isDebugEnabled()) {
            long length = gap.blockCount << CACHE_BLOCK_SIZE_POWER;
            LOGGER
                .debug(
                    "RA_ENQ_GAP path={} blockIdx={} off={} len={} blocks={} qsz={}/{} tns={}",
                    path,
                    gap.blockIndex,
                    offset,
                    length,
                    gap.blockCount,
                    queue.size(),
                    queueCapacity,
                    task.enqueuedNanos
                );
        }
        return true;
    }

    /**
     * Analyze cache coverage for a range and return uncached contiguous regions.
     */
    private List<CacheGap> findUncachedRanges(Path path, long startOffset, long blockCount) {
        List<CacheGap> gaps = new ArrayList<>();
        long startBlockIndex = startOffset >>> CACHE_BLOCK_SIZE_POWER;
        long currentGapStartIndex = -1;

        for (long i = 0; i < blockCount; i++) {
            long blockIndex = startBlockIndex + i;
            long blockOffset = blockIndex << CACHE_BLOCK_SIZE_POWER;
            BlockCacheKey key = new FileBlockCacheKey(path, blockOffset);

            if (blockCache.get(key) != null) {
                if (currentGapStartIndex == -1) {
                    // Start of new gap
                    currentGapStartIndex = blockIndex;
                }
                // Gap continues (no action needed)
            } else if (currentGapStartIndex != -1) {
                // End of gap - record it
                long gapBlocks = blockIndex - currentGapStartIndex;
                gaps.add(new CacheGap(currentGapStartIndex, gapBlocks));
                currentGapStartIndex = -1;
            }
        }

        // Handle final gap
        if (currentGapStartIndex != -1) {
            long gapBlocks = (startBlockIndex + blockCount) - currentGapStartIndex;
            gaps.add(new CacheGap(currentGapStartIndex, gapBlocks));
        }

        return gaps;
    }

    /**
     * Consolidate nearby uncached regions using initialWindow as merge threshold.
     * If gap between regions <= initialWindow blocks, merge them.
     */
    private List<CacheGap> consolidateGaps(List<CacheGap> rawGaps, int mergeThreshold) {
        if (rawGaps.isEmpty()) {
            return rawGaps;
        }

        List<CacheGap> consolidated = new ArrayList<>();
        CacheGap current = rawGaps.get(0);

        for (int i = 1; i < rawGaps.size(); i++) {
            CacheGap next = rawGaps.get(i);

            // Compute where the current gap ends (in block indices)
            long currentEnd = current.blockIndex + current.blockCount;

            // Gap between current and next (in blocks)
            long gapBetweenBlocks = next.blockIndex - currentEnd;

            if (gapBetweenBlocks <= mergeThreshold) {
                // Merge: small gap, extend current to cover next
                long nextEnd = next.blockIndex + next.blockCount;
                long mergedBlockCount = nextEnd - current.blockIndex;

                current = new CacheGap(current.blockIndex, mergedBlockCount);
            } else {
                // Split: large gap, keep current and move on
                consolidated.add(current);
                current = next;
            }
        }

        consolidated.add(current);
        return consolidated;
    }

    private void processLoop() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Starting readahead worker thread: {}", Thread.currentThread().getName());
        }

        while (!closed) {
            try {
                ReadAheadTask task = queue.takeFirst();

                task.startNanos = System.nanoTime();

                // bulk load the block.
                blockCache.loadBulk(task.path, task.offset, task.blockCount);
                task.doneNanos = System.nanoTime();

                inFlight.remove(task);

                long blockStart = task.offset >>> CACHE_BLOCK_SIZE_POWER;
                long blockEnd = blockStart + task.blockCount - 1;
                LOGGER
                    .debug(
                        "RA_IO_DONE_BULK path={} blockRange=[{}-{}] off={} len={} blocks={} io_ms={} qsz={}/{}",
                        task.path,
                        blockStart,
                        blockEnd,
                        task.offset,
                        task.blockCount << CACHE_BLOCK_SIZE_POWER,
                        task.blockCount,
                        (task.doneNanos - task.startNanos) / 1_000_000L,
                        queue.size(),
                        queueCapacity
                    );

            } catch (InterruptedException ie) {
                if (!closed) {
                    LOGGER.warn("Readahead worker thread interrupted: {}", Thread.currentThread().getName());
                }
                Thread.currentThread().interrupt();
                return;
            } catch (NoSuchFileException e) {
                LOGGER.debug("File not found during readahead", e);
            } catch (IOException | UncheckedIOException e) {
                LOGGER.warn("Failed to prefetch", e);
            }
        }

        LOGGER.info("Readahead worker thread exiting: {}", Thread.currentThread().getName());
    }

    @Override
    public void cancel(Path path) {
        queue.removeIf(task -> task.path.equals(path));
        inFlight.removeIf(task -> task.path.equals(path));
    }

    @Override
    public boolean isRunning() {
        return !closed;
    }

    @Override
    public void close() {
        closed = true;
        executor.shutdownNow();
        queue.clear();
        inFlight.clear();
    }
}

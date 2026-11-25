/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE_POWER;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadPolicy;
import org.opensearch.index.store.read_ahead.Worker;

/**
 * Best-effort readahead context. The principles are inspired by kernel and RocksDB.
 *
 * Principles:
 *  - onAccess() must be extremely cheap (no nanoTime, no queue introspection, no CAS loops for max updates, no logging)
 *  - miss-driven: if there are no cache misses, we do essentially nothing
 *  - bail early if scheduling is not possible (paused / pressure) without building backlog
 *  - skip-and-forget: if scheduling is rejected, drop pending speculative work and move forward
 *  - bounded submissions: never submit huge bursts
 *  - idempotent wakeups: never storm the worker callback from search threads
 *
 * Key ideas:
 *  - batching should be based on "growth since last wake" (not growth since last scheduled),
 *    so we keep making progress whenever the worker gets an opportunity to run.
 *  - prefetch always starts just after the reader's block: if the reader is past the scheduled
 *    tail (first read deep in the file, or a seek), the tail moves to the reader.
 *  - hits on prefetched blocks keep the window open: when the reader gets within lead blocks of
 *    the scheduled tail, the next window is requested without waiting for a miss.
 *  - one context per reader: clones and slices get their own context from
 *    {@link #newReaderContext()}, since they read at independent positions.
 */
public final class WindowedReadAheadContext implements ReadaheadContext {
    private static final Logger LOGGER = LogManager.getLogger(WindowedReadAheadContext.class);

    private final Path path;
    private final long lastFileSeg;
    private final WindowedReadAheadConfig config;
    // The context of the opened file; null for that context itself. Closing it stops this one.
    private final WindowedReadAheadContext root;
    private final Worker worker;
    private final BlockCache<? extends AutoCloseable> blockCache;
    private final WindowedReadaheadPolicy policy;
    private final Runnable signalCallback;

    // Bound per processQueue() call.
    private static final long MAX_BLOCKS_PER_SUBMISSION = 64;

    // 75% of queue capacity checks (per-context guard to avoid building backlog).
    private static final int QUEUE_PRESSURE_NUM = 3;
    private static final int QUEUE_PRESSURE_DEN = 4;

    // Desired tail (exclusive, in blocks), and scheduled tail (exclusive, in blocks).
    private volatile long desiredEndBlock = 0;
    private volatile long lastScheduledEndBlock = 0;
    // First block of the run of blocks scheduled so far (inclusive), so a hit can tell whether it was prefetched.
    private volatile long runStartBlock = 0;

    /**
     * Batching baseline: last desiredEndBlock value for which we successfully woke the worker.
     * We batch wakeups based on (desiredEndBlock - lastWakeDesiredEndBlock).
     *
     * Why track this separately from lastScheduledEndBlock?
     *
     * If we batched on (desiredEndBlock - lastScheduledEndBlock), we'd have a starvation problem:
     *  - Worker is busy processing previous batch
     *  - lastScheduledEndBlock doesn't advance (worker hasn't drained yet)
     *  - desiredEndBlock keeps growing from new misses
     *  - But delta threshold is never met because we already woke once (wakeFlag=1)
     *  - Worker finishes, but no new wake signal → it starves until next miss
     *
     * By tracking lastWakeDesiredEndBlock, we batch on "growth since last wake":
     *  - Even if worker is busy, we keep waking it as desired grows
     *  - Worker always has fresh work when it gets an opportunity
     *  - "Make progress on opportunity" principle
     */
    private volatile long lastWakeDesiredEndBlock = 0;

    // Idempotent wakeup gate (0 -> not woken, 1 -> woken).
    private int wakeupFlag = 0;

    private volatile boolean isClosed = false;

    // Best-effort stats (not required to be exact)
    private long accessMisses = 0;
    private long wakeups = 0;

    private long processCalls = 0;
    private long pressureSkips = 0;
    private long pausedSkips = 0;
    private long scheduleAttempts = 0;
    private long scheduleAccepted = 0;
    private long scheduleRejected = 0;
    private long blocksAttempted = 0;
    private long blocksAccepted = 0;

    private static final VarHandle WAKEUP_VH;

    static {
        try {
            WAKEUP_VH = MethodHandles.lookup().findVarHandle(WindowedReadAheadContext.class, "wakeupFlag", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private WindowedReadAheadContext(
        Path path,
        long lastFileSeg,
        Worker worker,
        BlockCache<? extends AutoCloseable> blockCache,
        WindowedReadAheadConfig config,
        Runnable signalCallback,
        WindowedReadAheadContext root
    ) {
        this.path = path;
        this.worker = worker;
        this.blockCache = blockCache;
        this.config = config;
        this.policy = newPolicy(path, config);
        // A reader context has no manager: a wakeup runs its own processQueue() on the reader's thread.
        this.signalCallback = root != null && signalCallback != null ? this::processQueue : signalCallback;
        this.lastFileSeg = lastFileSeg;
        this.root = root;
    }

    private static WindowedReadaheadPolicy newPolicy(Path path, WindowedReadAheadConfig config) {
        return new WindowedReadaheadPolicy(
            path,
            config.initialWindow(),
            config.maxWindowSegments(),
            1,
            config.randomAccessThreshold(),
            config.minSequentialRun()
        );
    }

    public static WindowedReadAheadContext build(
        Path path,
        long fileLength,
        Worker worker,
        BlockCache<? extends AutoCloseable> blockCache,
        WindowedReadAheadConfig config,
        Runnable signalCallback
    ) {
        final long lastFileSeg = Math.max(0L, (fileLength - 1) >>> CACHE_BLOCK_SIZE_POWER);
        return new WindowedReadAheadContext(path, lastFileSeg, worker, blockCache, config, signalCallback, null);
    }

    /**
     * Returns a context for a clone or slice of this file, with its own position and window.
     * Its wakeups run its own {@link #processQueue()}; a reader is used by one thread at a time,
     * so no lock is needed.
     */
    @Override
    public ReadaheadContext newReaderContext() {
        final WindowedReadAheadContext owner = root != null ? root : this;
        return new WindowedReadAheadContext(path, lastFileSeg, worker, blockCache, config, signalCallback, owner);
    }

    private boolean closed() {
        return isClosed || (root != null && root.isClosed);
    }

    /**
     * Hot path: must be extremely cheap.
     *
     * On a hit we only check whether the reader is consuming prefetched blocks and is within
     * leadBlocks() of the scheduled tail; if so the window grows and the next window is requested.
     * On a miss:
     *  - bail if worker is globally paused (node-wide thrash/pressure)
     *  - ask policy if this access pattern should trigger readahead
     *  - extend desired tail to currBlock + 1 + currentWindow(), starting after the reader
     *  - wake worker once if we grew enough since the last wake
     */
    @Override
    public void onAccess(long blockOffsetBytes, boolean wasHit) {
        if (closed()) {
            return;
        }

        final long currBlock = blockOffsetBytes >>> CACHE_BLOCK_SIZE_POWER;

        final long tail = lastScheduledEndBlock;
        // The reader is inside the run we scheduled (or just past it): its own sequential progress.
        final boolean inRun = tail > runStartBlock && currBlock >= runStartBlock && currBlock <= tail;

        if (wasHit) {
            // Only hits on blocks we prefetched say anything about this reader's pattern, and only
            // the ones within leadBlocks() of the tail need action.
            if (inRun == false || tail - currBlock > policy.leadBlocks()) {
                return;
            }
            if (worker.isReadAheadPaused()) {
                return;
            }
            policy.onReadaheadHit(currBlock);
        } else {
            if (worker.isReadAheadPaused()) {
                return;
            }

            accessMisses++;

            if (inRun) {
                // A prefetched block that has not landed yet (or was evicted): still sequential.
                policy.onReadaheadHit(currBlock);
            } else if (policy.shouldTrigger(currBlock) == false) {
                return;
            }
        }

        extendTo(currBlock, policy.currentWindow());
    }

    /** Asks for the blocks after {@code currBlock}, up to {@code window} of them, and wakes the worker. */
    private void extendTo(long currBlock, int window) {
        final long start = Math.min(currBlock + 1, lastFileSeg + 1);
        final long target = Math.min(start + window, lastFileSeg + 1);

        // Prefetch must start at the reader. If the scheduled tail is behind the reader (first read
        // past the start of the file, or prefetch fell behind) or far ahead of it (left over from
        // before a backward seek), restart the run just after the reader.
        final long tail = lastScheduledEndBlock;
        if (tail < start || tail > start + config.maxWindowSegments()) {
            lastScheduledEndBlock = start;
            runStartBlock = start;
            desiredEndBlock = start;
            lastWakeDesiredEndBlock = start;
        }

        // Best-effort monotonic extend
        final long prevDesired = desiredEndBlock;
        if (target <= prevDesired) {
            return;
        }
        desiredEndBlock = target;

        // Wake immediately on growth - idempotent gate prevents storms.
        // processQueue() naturally batches up to MAX_BLOCKS_PER_SUBMISSION (64).
        // This ensures we "make progress on opportunity" for all access patterns (sparse, burst, sequential).
        if (maybeWakeWorkerOnce()) {
            lastWakeDesiredEndBlock = target;
        }
    }

    @Override
    public boolean processQueue() {
        if (closed()) {
            return false;
        }

        processCalls++;

        // If node-wide gate is paused, drop backlog and clear wake gate.
        if (worker.isReadAheadPaused()) {
            pausedSkips++;
            dropBacklogAndResetWakeBaseline();
            // maybeLogStats(); // enable back for debugging.
            return false;
        }

        final long scheduled = lastScheduledEndBlock;
        final long desired = desiredEndBlock;

        if (desired <= scheduled) {
            WAKEUP_VH.setRelease(this, 0);
            // maybeLogStats();
            return false;
        }

        // Per-context queue pressure guard (cheap introspection is OK here; processQueue is not hot).
        final int cap = worker.getQueueCapacity();
        if (cap > 0) {
            final int q = worker.getQueueSize();
            if (q > (cap * QUEUE_PRESSURE_NUM) / QUEUE_PRESSURE_DEN) {
                pressureSkips++;
                policy.onQueuePressureMedium();
                dropBacklogAndResetWakeBaseline();
                // maybeLogStats();
                return false;
            }
        }

        final long endExclusiveRaw = Math.min(desired, lastFileSeg + 1);
        final long endExclusive = Math.min(endExclusiveRaw, scheduled + MAX_BLOCKS_PER_SUBMISSION);

        final long blockCount = endExclusive - scheduled;
        if (blockCount <= 0) {
            WAKEUP_VH.setRelease(this, 0);
            // maybeLogStats();
            return false;
        }

        scheduleAttempts++;
        blocksAttempted += blockCount;

        final long anchorOffset = scheduled << CACHE_BLOCK_SIZE_POWER;
        final boolean accepted = worker.schedule(blockCache, path, anchorOffset, blockCount);

        if (accepted && closed()) {
            // The file closed while we scheduled: its cancel may have run before our task was queued.
            worker.cancel(path);
            return false;
        }

        if (accepted) {
            scheduleAccepted++;
            blocksAccepted += blockCount;

            lastScheduledEndBlock = endExclusive;

            // Always reopen the wake gate: if more work remains, the next access wakes the worker
            // again. Keeping it closed here would stop readahead for good, since nothing else
            // calls processQueue().
            WAKEUP_VH.setRelease(this, 0);
            // maybeLogStats();
            return true;
        }

        scheduleRejected++;

        // Rejected: drop backlog and allow future wakeups.
        policy.onQueueSaturated();
        dropBacklogAndResetWakeBaseline();

        // maybeLogStats();
        return false;
    }

    private void dropBacklogAndResetWakeBaseline() {
        desiredEndBlock = lastScheduledEndBlock;
        lastWakeDesiredEndBlock = lastScheduledEndBlock;
        WAKEUP_VH.setRelease(this, 0);
    }

    @SuppressWarnings("unused")
    private void maybeLogStats() {
        int q = -1;
        int cap = -1;
        try {
            cap = worker.getQueueCapacity();
            q = worker.getQueueSize();
        } catch (Exception ignored) {}

        final long desired = desiredEndBlock;
        final long tail = lastScheduledEndBlock;
        final long queuedDelta = desired - tail;

        final long emptyProcess = processCalls - scheduleAttempts;
        final long avgBlocksPerOk = scheduleAccepted == 0 ? 0 : (blocksAccepted / scheduleAccepted);

        LOGGER
            .info(
                "RA_STATS path={} missAccess={} desired={} scheduledTail={} queuedDelta={} window={} lead={} "
                    + "wakeups={} processCalls={} emptyProcess={} pressureSkips={} pausedSkips={} "
                    + "schedule(attempt={}, ok={}, reject={}) blocks(attempted={}, ok={}, avgOk={}) q={}/{}",
                path,
                accessMisses,
                desired,
                tail,
                queuedDelta,
                policy.currentWindow(),
                policy.leadBlocks(),
                wakeups,
                processCalls,
                emptyProcess,
                pressureSkips,
                pausedSkips,
                scheduleAttempts,
                scheduleAccepted,
                scheduleRejected,
                blocksAttempted,
                blocksAccepted,
                avgBlocksPerOk,
                q,
                cap
            );
    }

    @Override
    public boolean hasQueuedWork() {
        return desiredEndBlock > lastScheduledEndBlock;
    }

    @Override
    public ReadaheadPolicy policy() {
        return policy;
    }

    @Override
    public void triggerReadahead(long fileOffsetBytes) {
        if (closed()) {
            return;
        }
        if (worker.isReadAheadPaused()) {
            return;
        }

        // Prefetch from the given block itself, so start the run one block earlier.
        extendTo((fileOffsetBytes >>> CACHE_BLOCK_SIZE_POWER) - 1, policy.currentWindow());
    }

    @Override
    public void reset() {
        desiredEndBlock = lastScheduledEndBlock;
        lastWakeDesiredEndBlock = lastScheduledEndBlock;
        WAKEUP_VH.setRelease(this, 0);
        policy.reset();
    }

    @Override
    public void cancel() {
        worker.cancel(path);
    }

    @Override
    public boolean isReadAheadEnabled() {
        return !closed();
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        isClosed = true;
        cancel();
        desiredEndBlock = lastScheduledEndBlock;
        lastWakeDesiredEndBlock = lastScheduledEndBlock;
        WAKEUP_VH.setRelease(this, 0);
    }

    private boolean maybeWakeWorkerOnce() {
        if (signalCallback == null) {
            return false;
        }
        if ((int) WAKEUP_VH.compareAndExchange(this, 0, 1) == 0) {
            wakeups++;
            signalCallback.run();
            return true;
        }
        return false;
    }
}

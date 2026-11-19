/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.block.RefCountedByteBuffer;
import org.opensearch.index.store.metrics.CryptoMetricsService;

/**
 * ByteBuffer pool that reuses a fixed set of slots instead of allocating a direct buffer per block.
 *
 * <p>{@link ByteBufferPool} calls {@link ByteBuffer#allocateDirect(int)} for every block and leaves the
 * free to the GC. Under cache churn (merges, bulk indexing) that is millions of 8 KB malloc/free pairs,
 * and glibc keeps much of the freed memory instead of returning it to the OS, so the process grows
 * well past the pool size. This pool instead carves the pool's memory into slots inside a few large
 * slabs, allocated once (lazily, slab by slab) and never freed while the node runs.
 *
 * <p>Each acquire hands out a fresh view {@link ByteBuffer} of one free slot. The view, not the
 * {@link RefCountedByteBuffer} wrapper, is registered with a {@link Cleaner}: a slot goes back on the
 * free list only once the GC finds the view unreachable, so a reader that still holds the buffer (a
 * slice of the index input mid-read, or the cache) can never see the slot reused. Readers must use the
 * view as-is: a {@code slice()} or {@code duplicate()} of it would keep the slab alive but not the view.
 *
 * <p>When no slot is free (evicted views not yet collected), the pool wakes a background reclaimer
 * thread, which requests a GC; read threads never run the GC themselves and wait with the lock
 * released. A read ({@link #acquire}) waits up to a short bound for a slot, then may use a standalone
 * direct buffer for that block (counted as overflow), but only while fewer than
 * {@link #MAX_OVERFLOW_FRACTION} of the pool's slots worth of overflow buffers are live, so direct
 * memory stays bounded. Past that cap it keeps waiting for a slot or a freed overflow buffer, and fails
 * after {@link #DEMAND_WAIT_NANOS}. {@link #tryAcquire} (the write cache and prefetch) never uses
 * overflow: it waits up to its timeout and then fails, as in {@link ByteBufferPool}.
 *
 * @opensearch.internal
 */
public class SlabByteBufferPool extends ByteBufferPool {

    private static final Logger LOGGER = LogManager.getLogger(SlabByteBufferPool.class);
    private static final Cleaner CLEANER = Cleaner.create();
    /** 64 MB slabs: large enough that glibc mmaps them directly, outside its arenas. */
    static final int SLAB_BYTES = 64 << 20;
    /** How long a read waits for a slot before it may use an overflow buffer. */
    private static final long READ_WAIT_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    /** How long a read waits in total, once overflow is at its cap, before failing. */
    static final long DEMAND_WAIT_NANOS = TimeUnit.SECONDS.toNanos(5);
    /** Live overflow buffers allowed, as a fraction of the pool's slots. */
    static final double MAX_OVERFLOW_FRACTION = 0.10;
    /** Minimum time between two GC requests by the reclaimer. */
    private static final long GC_REQUEST_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);
    private static final int OVERFLOW = -2;
    private static final long WARN_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);

    private final int slotsPerSlab;
    private final int maxSlabs;
    private final List<ByteBuffer> slabs = new ArrayList<>();
    private final int[] freeSlots;
    private int freeCount = 0;
    private int allocatedSlots = 0;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition slotReturned = lock.newCondition();

    private final int maxOverflow;
    private int liveOverflow = 0; // guarded by lock
    private Thread reclaimer; // guarded by lock, started on first need
    private volatile boolean reclaimRequested = false;
    private long lastGCRequestNanos; // reclaimer thread only

    private final AtomicLong overflowAllocations = new AtomicLong();
    private final AtomicLong overflowCapWaits = new AtomicLong();
    private final AtomicLong gcRequests = new AtomicLong();
    private final AtomicLong waits = new AtomicLong();
    private final AtomicLong lastWarnNanos = new AtomicLong();

    public SlabByteBufferPool(long totalMemory, int segmentSize) {
        super(totalMemory, segmentSize);
        if (SLAB_BYTES % segmentSize != 0) {
            throw new IllegalArgumentException("Slab size must be a multiple of segment size");
        }
        this.slotsPerSlab = SLAB_BYTES / segmentSize;
        this.maxSlabs = (maxSegments + slotsPerSlab - 1) / slotsPerSlab;
        this.freeSlots = new int[maxSlabs * slotsPerSlab];
        this.maxOverflow = Math.max(1, (int) (maxSegments * MAX_OVERFLOW_FRACTION));
    }

    @Override
    public RefCountedByteBuffer acquire() throws InterruptedException, IOException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Thread interrupted before pool acquire");
        }
        ensureOpen();
        int slot = takeSlot(DEMAND_WAIT_NANOS, READ_WAIT_NANOS);
        if (slot >= 0) {
            return wrap(slot);
        }
        if (slot == OVERFLOW) {
            overflowAllocations.incrementAndGet();
            warnRateLimited("no free slot after waiting, using a standalone buffer for this block");
            return newOverflowBuffer();
        }
        throw new IOException(
            "No free slot within " + TimeUnit.NANOSECONDS.toMillis(DEMAND_WAIT_NANOS) + " ms and overflow at its cap: " + poolStats()
        );
    }

    @Override
    public RefCountedByteBuffer tryAcquire(long timeout, TimeUnit unit) throws Exception {
        ensureOpen();
        int slot = takeSlot(unit.toNanos(timeout), -1);
        if (slot < 0) {
            throw new IOException("Pool capacity reached (no free slot within " + unit.toMillis(timeout) + " ms)");
        }
        return wrap(slot);
    }

    /**
     * Takes a free slot, growing by one slab if allowed. Otherwise wakes the reclaimer and waits, with
     * the lock released, for a slot to come back. If {@code overflowAfterNanos >= 0}, once that much of
     * the wait has passed it may instead reserve an overflow buffer while fewer than the cap are live,
     * and returns {@link #OVERFLOW}. Returns -1 if neither happened within {@code waitNanos}.
     */
    private int takeSlot(long waitNanos, long overflowAfterNanos) throws InterruptedException {
        lock.lock();
        try {
            if (freeCount == 0 && slabs.size() < maxSlabs) {
                addSlab();
            }
            if (freeCount > 0) {
                return freeSlots[--freeCount];
            }
            waits.incrementAndGet();
            requestReclaim();
            final long start = System.nanoTime();
            boolean countedCapWait = false;
            while (!closed) {
                if (freeCount > 0) {
                    return freeSlots[--freeCount];
                }
                final long elapsed = System.nanoTime() - start;
                final boolean mayOverflow = overflowAfterNanos >= 0 && elapsed >= overflowAfterNanos;
                if (mayOverflow) {
                    if (liveOverflow < maxOverflow) {
                        liveOverflow++;
                        return OVERFLOW;
                    }
                    if (countedCapWait == false) {
                        overflowCapWaits.incrementAndGet();
                        countedCapWait = true;
                    }
                }
                final long remaining = waitNanos - elapsed;
                if (remaining <= 0) {
                    return -1;
                }
                // Woken by a returned slot or a freed overflow buffer; until overflow is allowed, wake then too.
                final long untilOverflow = overflowAfterNanos >= 0 && !mayOverflow ? overflowAfterNanos - elapsed : remaining;
                slotReturned.awaitNanos(Math.min(remaining, untilOverflow));
            }
            return -1;
        } finally {
            lock.unlock();
        }
    }

    /** A standalone buffer for one block, counted against the overflow cap until the GC frees it. */
    private RefCountedByteBuffer newOverflowBuffer() {
        final ByteBuffer direct;
        try {
            direct = ByteBuffer.allocateDirect(segmentSize).order(ByteOrder.LITTLE_ENDIAN);
        } catch (Throwable t) {
            releaseOverflow();
            throw t;
        }
        CLEANER.register(direct, new ReleaseOverflow(this));
        return new RefCountedByteBuffer(direct, segmentSize);
    }

    private void releaseOverflow() {
        lock.lock();
        try {
            liveOverflow--;
            slotReturned.signal();
        } finally {
            lock.unlock();
        }
    }

    /** Must not capture the buffer (that would keep it reachable); holds only the pool. */
    private static final class ReleaseOverflow implements Runnable {
        private final SlabByteBufferPool pool;

        ReleaseOverflow(SlabByteBufferPool pool) {
            this.pool = pool;
        }

        @Override
        public void run() {
            pool.releaseOverflow();
        }
    }

    /**
     * Called with the lock held. Wakes the reclaimer thread, starting it the first time; the GC never
     * runs on the calling (read) thread, and never with the lock held.
     */
    private void requestReclaim() {
        if (reclaimer == null) {
            reclaimer = new Thread(this::reclaimLoop, "crypto-slab-pool-reclaimer");
            reclaimer.setDaemon(true);
            reclaimer.start();
        }
        if (reclaimRequested == false) {
            reclaimRequested = true;
            LockSupport.unpark(reclaimer);
        }
    }

    /**
     * Requests a GC when the pool has run dry, at most once per {@link #GC_REQUEST_INTERVAL_NANOS}:
     * evicted blocks' slots and freed overflow buffers come back only once the GC finds them unreachable.
     */
    private void reclaimLoop() {
        while (closed == false) {
            if (reclaimRequested == false) {
                LockSupport.park(this);
                continue;
            }
            final long sinceLast = System.nanoTime() - lastGCRequestNanos;
            if (gcRequests.get() > 0 && sinceLast < GC_REQUEST_INTERVAL_NANOS) {
                LockSupport.parkNanos(this, GC_REQUEST_INTERVAL_NANOS - sinceLast);
                continue;
            }
            reclaimRequested = false;
            lastGCRequestNanos = System.nanoTime();
            gcRequests.incrementAndGet();
            System.gc();
        }
    }

    /** Called with the lock held. */
    private void addSlab() {
        int base = slabs.size() * slotsPerSlab;
        int slots = Math.min(slotsPerSlab, maxSegments - base);
        slabs.add(ByteBuffer.allocateDirect(slots * segmentSize));
        allocatedSlots += slots;
        for (int i = slots - 1; i >= 0; i--) {
            freeSlots[freeCount++] = base + i;
        }
    }

    private RefCountedByteBuffer wrap(int slot) {
        ByteBuffer slab;
        lock.lock();
        try {
            slab = slabs.get(slot / slotsPerSlab);
        } finally {
            lock.unlock();
        }
        int offset = (slot % slotsPerSlab) * segmentSize;
        ByteBuffer view = slab.slice(offset, segmentSize).order(ByteOrder.LITTLE_ENDIAN);
        CLEANER.register(view, new ReturnSlot(this, slot));
        return new RefCountedByteBuffer(view, segmentSize);
    }

    private void returnSlot(int slot) {
        lock.lock();
        try {
            freeSlots[freeCount++] = slot;
            slotReturned.signal();
        } finally {
            lock.unlock();
        }
    }

    /** Must not capture the view (that would keep it reachable); holds only the pool and the slot index. */
    private static final class ReturnSlot implements Runnable {
        private final SlabByteBufferPool pool;
        private final int slot;

        ReturnSlot(SlabByteBufferPool pool, int slot) {
            this.pool = pool;
            this.slot = slot;
        }

        @Override
        public void run() {
            pool.returnSlot(slot);
        }
    }

    private void warnRateLimited(String msg) {
        long now = System.nanoTime();
        long last = lastWarnNanos.get();
        if (now - last > WARN_INTERVAL_NANOS && lastWarnNanos.compareAndSet(last, now)) {
            LOGGER.warn("SlabByteBufferPool: {} ({})", msg, poolStats());
        }
    }

    /** Slots handed out and not yet returned by the GC (live cache blocks plus evicted, uncollected views). */
    @Override
    public int getBuffersInUse() {
        lock.lock();
        try {
            return allocatedSlots - freeCount;
        } finally {
            lock.unlock();
        }
    }

    public long getOverflowAllocations() {
        return overflowAllocations.get();
    }

    /** Overflow buffers handed out and not yet freed by the GC. */
    public int getLiveOverflowBuffers() {
        lock.lock();
        try {
            return liveOverflow;
        } finally {
            lock.unlock();
        }
    }

    public int getMaxOverflowBuffers() {
        return maxOverflow;
    }

    /** Reads that found overflow at its cap and had to keep waiting. */
    public long getOverflowCapWaits() {
        return overflowCapWaits.get();
    }

    /** GCs requested by the reclaimer. */
    public long getGCRequests() {
        return gcRequests.get();
    }

    @Override
    public long availableMemory() {
        return (long) Math.max(0, maxSegments - getBuffersInUse()) * segmentSize;
    }

    @Override
    public boolean isUnderPressure() {
        return (maxSegments - getBuffersInUse()) < (maxSegments * 0.1);
    }

    @Override
    public void warmUp(long targetSegments) {
        LOGGER.info("Warmup skipped, slabs allocated on demand (max={} slots in {} slabs)", maxSegments, maxSlabs);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        lock.lock();
        try {
            slotReturned.signalAll();
            if (reclaimer != null) {
                LockSupport.unpark(reclaimer);
            }
        } finally {
            lock.unlock();
        }
        LOGGER.info("SlabByteBufferPool closed: {}", poolStats());
    }

    @Override
    public String poolStats() {
        int slabCount;
        int free;
        int overflowLive;
        lock.lock();
        try {
            slabCount = slabs.size();
            free = freeCount;
            overflowLive = liveOverflow;
        } finally {
            lock.unlock();
        }
        return String
            .format(
                Locale.ROOT,
                "SlabByteBufferPool[max=%d, slabs=%d/%d, free=%d, inUse=%d, overflow=%d, overflowLive=%d/%d, overflowCapWaits=%d, "
                    + "waits=%d, gcRequests=%d]",
                maxSegments,
                slabCount,
                maxSlabs,
                free,
                getBuffersInUse(),
                overflowAllocations.get(),
                overflowLive,
                maxOverflow,
                overflowCapWaits.get(),
                waits.get(),
                gcRequests.get()
            );
    }

    @Override
    public void recordStats() {
        int inUse = getBuffersInUse();
        double pct = maxSegments > 0 ? (double) inUse / maxSegments : 0;
        CryptoMetricsService.getInstance().recordPoolStats(SegmentType.PRIMARY, maxSegments, inUse, 0, pct, pct);
    }
}

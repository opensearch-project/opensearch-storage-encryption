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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
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
 * <p>When no slot is free (evicted views not yet collected): request a GC, wait up to a short bound
 * for a slot to come back, then fall back to a standalone direct buffer for that block (counted as
 * overflow) so reads never stall indefinitely. The write path ({@link #tryAcquire}) waits up to its
 * timeout and then fails, as in {@link ByteBufferPool}.
 *
 * @opensearch.internal
 */
public class SlabByteBufferPool extends ByteBufferPool {

    private static final Logger LOGGER = LogManager.getLogger(SlabByteBufferPool.class);
    private static final Cleaner CLEANER = Cleaner.create();
    /** 64 MB slabs: large enough that glibc mmaps them directly, outside its arenas. */
    static final int SLAB_BYTES = 64 << 20;
    private static final long READ_WAIT_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    private static final long GC_REQUEST_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);
    private static final long WARN_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);

    private final int slotsPerSlab;
    private final int maxSlabs;
    private final List<ByteBuffer> slabs = new ArrayList<>();
    private final int[] freeSlots;
    private int freeCount = 0;
    private int allocatedSlots = 0;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition slotReturned = lock.newCondition();

    private final AtomicLong overflowAllocations = new AtomicLong();
    private final AtomicLong waits = new AtomicLong();
    private final AtomicLong lastGCRequestNanos = new AtomicLong();
    private final AtomicLong lastWarnNanos = new AtomicLong();

    public SlabByteBufferPool(long totalMemory, int segmentSize) {
        super(totalMemory, segmentSize);
        if (SLAB_BYTES % segmentSize != 0) {
            throw new IllegalArgumentException("Slab size must be a multiple of segment size");
        }
        this.slotsPerSlab = SLAB_BYTES / segmentSize;
        this.maxSlabs = (maxSegments + slotsPerSlab - 1) / slotsPerSlab;
        this.freeSlots = new int[maxSlabs * slotsPerSlab];
    }

    @Override
    public RefCountedByteBuffer acquire() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Thread interrupted before pool acquire");
        }
        ensureOpen();
        int slot = takeSlot(READ_WAIT_NANOS);
        if (slot >= 0) {
            return wrap(slot);
        }
        // Read path must not stall: serve this block from a standalone buffer, freed by its own cleaner.
        overflowAllocations.incrementAndGet();
        warnRateLimited("no free slot after waiting, using a standalone buffer for this block");
        ByteBuffer direct = ByteBuffer.allocateDirect(segmentSize).order(ByteOrder.LITTLE_ENDIAN);
        return new RefCountedByteBuffer(direct, segmentSize);
    }

    @Override
    public RefCountedByteBuffer tryAcquire(long timeout, TimeUnit unit) throws Exception {
        ensureOpen();
        int slot = takeSlot(unit.toNanos(timeout));
        if (slot < 0) {
            throw new IOException("Pool capacity reached (no free slot within " + unit.toMillis(timeout) + " ms)");
        }
        return wrap(slot);
    }

    /** Takes a free slot, growing by one slab if allowed, otherwise asking for a GC and waiting. -1 on timeout. */
    private int takeSlot(long waitNanos) throws InterruptedException {
        lock.lock();
        try {
            if (freeCount == 0 && slabs.size() < maxSlabs) {
                addSlab();
            }
            if (freeCount == 0) {
                waits.incrementAndGet();
                requestGCIfNeeded();
                long remaining = waitNanos;
                while (freeCount == 0 && remaining > 0 && !closed) {
                    remaining = slotReturned.awaitNanos(remaining);
                }
                if (freeCount == 0) {
                    return -1;
                }
            }
            return freeSlots[--freeCount];
        } finally {
            lock.unlock();
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

    private void requestGCIfNeeded() {
        long now = System.nanoTime();
        long last = lastGCRequestNanos.get();
        if (now - last > GC_REQUEST_INTERVAL_NANOS && lastGCRequestNanos.compareAndSet(last, now)) {
            System.gc();
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
        } finally {
            lock.unlock();
        }
        LOGGER.info("SlabByteBufferPool closed: {}", poolStats());
    }

    @Override
    public String poolStats() {
        int slabCount;
        int free;
        lock.lock();
        try {
            slabCount = slabs.size();
            free = freeCount;
        } finally {
            lock.unlock();
        }
        return String.format(
            "SlabByteBufferPool[max=%d, slabs=%d/%d, free=%d, inUse=%d, overflow=%d, waits=%d]",
            maxSegments,
            slabCount,
            maxSlabs,
            free,
            getBuffersInUse(),
            overflowAllocations.get(),
            waits.get()
        );
    }

    @Override
    public void recordStats() {
        int inUse = getBuffersInUse();
        double pct = maxSegments > 0 ? (double) inUse / maxSegments : 0;
        CryptoMetricsService.getInstance().recordPoolStats(SegmentType.PRIMARY, maxSegments, inUse, 0, pct, pct);
    }
}

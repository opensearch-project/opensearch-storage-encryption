/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.block.RefCountedByteBuffer;
import org.opensearch.index.store.metrics.CryptoMetricsService;

/**
 * GC-managed pool for off-heap memory backed by direct ByteBuffers.
 *
 * <p>Every {@link #acquire()} call allocates a fresh {@link ByteBuffer#allocateDirect(int)}.
 * The JVM's internal {@code Bits.reserveMemory()} handles GC pressure and retry when
 * direct memory is exhausted.
 *
 * <p>A periodic GC debt monitor runs every second. When native direct memory usage
 * exceeds our tracked allocations by more than 10%, it triggers {@code System.gc()}
 * to help the JVM reclaim phantom-reachable DirectByteBuffers faster.
 *
 * @opensearch.internal
 */
@SuppressWarnings("preview")
@SuppressForbidden(reason = "Uses ByteBuffer.allocateDirect for native memory allocation")
public class MemorySegmentPool implements Pool<RefCountedByteBuffer>, AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(MemorySegmentPool.class);
    private static final Cleaner CLEANER = Cleaner.create();

    private final int segmentSize;
    private final int maxSegments;
    private final long totalMemory;
    private final AtomicInteger buffersInUse = new AtomicInteger(0);
    private final LongAdder stallCount = new LongAdder();

    private final Thread gcDebtMonitor;
    private final Runnable cleanerAction = buffersInUse::decrementAndGet;
    private volatile LongSupplier cacheEntriesSupplier = () -> 0;

    /** Register cache size supplier for GC debt monitoring. */
    public void setCacheEntriesSupplier(LongSupplier supplier) {
        this.cacheEntriesSupplier = supplier;
    }

    private volatile boolean closed = false;

    public MemorySegmentPool(long totalMemory, int segmentSize) {
        if (totalMemory % segmentSize != 0) {
            throw new IllegalArgumentException("Total memory must be a multiple of segment size");
        }
        this.totalMemory = totalMemory;
        this.segmentSize = segmentSize;
        this.maxSegments = (int) (totalMemory / segmentSize);
        this.gcDebtMonitor = new Thread(this::gcDebtMonitorLoop, "pool-gc-debt-monitor");
        gcDebtMonitor.setDaemon(true);
        gcDebtMonitor.start();
    }

    @Override
    public RefCountedByteBuffer acquire() throws InterruptedException {
        if (closed)
            throw new IllegalStateException("Pool is closed");
        int current = buffersInUse.incrementAndGet();
        if (current <= maxSegments) {
            ByteBuffer buf = ByteBuffer.allocateDirect(segmentSize).order(ByteOrder.LITTLE_ENDIAN);
            return wrapAndRegister(buf);
        }

        if (closed) {
            throw new IllegalStateException("Pool is closed");
        }
        long allocationStartTime = System.nanoTime();
        stallCount.increment();

        ByteBuffer buf = ByteBuffer.allocateDirect(segmentSize).order(ByteOrder.LITTLE_ENDIAN);
        LOGGER
            .info(
                "Over capacity allocateDirect took {}ms (inUse={}, max={})",
                (System.nanoTime() - allocationStartTime) / 1_000_000,
                current,
                maxSegments
            );
        return wrapAndRegister(buf);
    }

    @Override
    public RefCountedByteBuffer tryAcquire(long timeout, TimeUnit unit) throws Exception {
        return acquire();
    }

    private RefCountedByteBuffer wrapAndRegister(ByteBuffer direct) {
        RefCountedByteBuffer wrapper = new RefCountedByteBuffer(direct, segmentSize);
        CLEANER.register(wrapper, cleanerAction);
        return wrapper;
    }

    private void gcDebtMonitorLoop() {
        while (!closed) {
            try {
                Thread.sleep(1000);
                checkGcDebt();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable t) {
                LOGGER.warn("Error in GC debt monitor", t);
            }
        }
    }

    /**
     * Checks for zombie buffers: buffers that left the cache but whose native memory
     * hasn't been freed by GC yet. Triggers System.gc() when:
     * - Zombie count (buffersInUse - cacheEntries) is significant
     * - Remaining pool capacity is below 10%
     */
    private void checkGcDebt() {
        if (closed)
            return;
        int inUse = buffersInUse.get();
        long cacheEntries = cacheEntriesSupplier.getAsLong();
        long zombies = inUse - cacheEntries;
        int remaining = maxSegments - inUse;

        if (zombies > 0 && remaining < maxSegments / 10) {
            LOGGER
                .info(
                    "GC debt: inUse={}, cacheEntries={}, zombies={}, remaining={}/{} — triggering GC",
                    inUse,
                    cacheEntries,
                    zombies,
                    remaining,
                    maxSegments
                );
            System.gc();
        }
    }

    @Override
    public void release(RefCountedByteBuffer refSegment) {
        // No-op: Cleaner handles lifecycle when wrapper is GC'd.
    }

    public void releaseAll(RefCountedByteBuffer... segments) {
        // No-op
    }

    @Override
    public long totalMemory() {
        return totalMemory;
    }

    @Override
    public long availableMemory() {
        return (long) Math.max(0, maxSegments - buffersInUse.get()) * segmentSize;
    }

    @Override
    public int pooledSegmentSize() {
        return segmentSize;
    }

    public int getBuffersInUse() {
        return buffersInUse.get();
    }

    public long getAllocatedBytes() {
        return (long) buffersInUse.get() * segmentSize;
    }

    @Override
    public boolean isUnderPressure() {
        return buffersInUse.get() >= (int) (maxSegments * 0.95);
    }

    @Override
    public void warmUp(long targetSegments) {}

    @Override
    public void close() {
        if (closed)
            return;
        closed = true;
        gcDebtMonitor.interrupt();
        LOGGER.info("MemorySegmentPool closed");
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public String poolStats() {
        int inUse = buffersInUse.get();
        return String
            .format(
                "PoolStats[max=%d, inUse=%d, utilization=%.1f%%, stalls=%d]",
                maxSegments,
                inUse,
                maxSegments > 0 ? (double) inUse / maxSegments * 100 : 0,
                stallCount.sum()
            );
    }

    public PoolStats getStats() {
        return new PoolStats(maxSegments, buffersInUse.get());
    }

    @SuppressForbidden(reason = "custom string builder")
    public static class PoolStats {
        public final int maxSegments;
        public final int totalProvisioned;
        public final int buffersInUse;
        public final int freeListSize;
        public final double utilizationRatio;

        PoolStats(int maxSegments, int buffersInUse) {
            this.maxSegments = maxSegments;
            this.totalProvisioned = buffersInUse;
            this.buffersInUse = buffersInUse;
            this.freeListSize = 0;
            this.utilizationRatio = maxSegments > 0 ? (double) buffersInUse / maxSegments : 0;
        }

        @Override
        public String toString() {
            return String.format("PoolStats[max=%d, inUse=%d, utilization=%.1f%%]", maxSegments, buffersInUse, utilizationRatio * 100);
        }
    }

    @Override
    public void recordStats() {
        int inUse = buffersInUse.get();
        double pct = maxSegments > 0 ? (double) inUse / maxSegments : 0;
        CryptoMetricsService.getInstance().recordPoolStats(SegmentType.PRIMARY, maxSegments, inUse, 0, pct, pct);
    }
}

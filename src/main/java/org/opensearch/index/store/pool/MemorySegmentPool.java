/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import java.lang.foreign.MemorySegment;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.PanamaNativeAccess;
import org.opensearch.index.store.block.RefCountedMemorySegment;

/**
 * High-performance primary memory pool for off-heap memory segment allocation using
 * pure Panama FFM API (malloc/free) without arenas.
 *
 * <p>This pool manages fixed-size native segments, allocated on-demand
 * via libc malloc() and released via free(). Segments are recycled
 * through a freelist for performance. Optionally, memory is zeroed on release
 * for security.
 *
 * <p>Thread-safe via an internal {@link ReentrantLock}.
 * Cached stats minimize lock contention during monitoring.
 *
 * @opensearch.internal
 */
@SuppressWarnings("preview")
@SuppressForbidden(reason = "Uses Panama FFI for native memory allocation")
public class MemorySegmentPool implements Pool<RefCountedMemorySegment>, AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(MemorySegmentPool.class);

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Deque<RefCountedMemorySegment> freeList = new ArrayDeque<>();

    private final int segmentSize;
    private final int maxSegments;
    private final long totalMemory;
    private final boolean requiresZeroing;

    private volatile boolean closed = false;
    private int allocatedSegments = 0;
    private volatile int cachedFreeListSize = 0;

    /**
     * Creates a pool with lazy allocation and memory zeroing enabled for security.
     */
    public MemorySegmentPool(long totalMemory, int segmentSize) {
        this(totalMemory, segmentSize, true);
    }

    /**
     * Creates a pool with configurable allocation strategy and zeroing behavior.
     */
    public MemorySegmentPool(long totalMemory, int segmentSize, boolean requiresZeroing) {
        if (totalMemory % segmentSize != 0) {
            throw new IllegalArgumentException("Total memory must be a multiple of segment size");
        }
        this.totalMemory = totalMemory;
        this.segmentSize = segmentSize;
        this.maxSegments = (int) (totalMemory / segmentSize);
        this.requiresZeroing = requiresZeroing;
    }

    @Override
    public RefCountedMemorySegment acquire() throws InterruptedException {
        lock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("Pool is closed");
            }

            // Try freelist first
            if (!freeList.isEmpty()) {
                RefCountedMemorySegment refSeg = freeList.removeFirst();
                cachedFreeListSize = freeList.size();
                refSeg.reset();
                return refSeg;
            }

            // Try allocate new segment if under capacity
            if (allocatedSegments < maxSegments) {
                MemorySegment seg = PanamaNativeAccess.malloc(segmentSize);
                RefCountedMemorySegment refSeg = new RefCountedMemorySegment(seg, segmentSize, this::release);
                allocatedSegments++;
                LOGGER.trace("Allocated new native segment, total allocated={}", allocatedSegments);
                return refSeg;
            }

            throw new RuntimeException("Pool limit exhausted, try increasing pool size");
        } finally {
            lock.unlock();
        }
    }

    @Override
    public RefCountedMemorySegment tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        return acquire(); // simple non-blocking version for now
    }

    @Override
    public void release(RefCountedMemorySegment refSegment) {
        lock.lock();
        try {
            if (closed) {
                // Free directly if pool is closed
                PanamaNativeAccess.free(refSegment.segment());
                return;
            }

            if (requiresZeroing) {
                refSegment.segment().fill((byte) 0);
            }

            freeList.addLast(refSegment);
            cachedFreeListSize = freeList.size();
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    /** Release multiple segments efficiently in one lock operation. */
    public void releaseAll(RefCountedMemorySegment... segments) {
        if (segments.length == 0)
            return;
        lock.lock();
        try {
            if (closed) {
                for (RefCountedMemorySegment s : segments) {
                    PanamaNativeAccess.free(s.segment());
                }
                return;
            }

            for (RefCountedMemorySegment s : segments) {
                if (requiresZeroing) {
                    s.segment().fill((byte) 0);
                }
                freeList.addLast(s);
            }
            cachedFreeListSize = freeList.size();
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long totalMemory() {
        return totalMemory;
    }

    @Override
    public long availableMemory() {
        int free = cachedFreeListSize;
        int allocated = allocatedSegments;
        int canAllocate = Math.max(0, maxSegments - allocated);
        return (long) (free + canAllocate) * segmentSize;
    }

    public long availableMemoryAccurate() {
        lock.lock();
        try {
            int free = freeList.size();
            int canAlloc = Math.max(0, maxSegments - allocatedSegments);
            return (long) (free + canAlloc) * segmentSize;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int pooledSegmentSize() {
        return segmentSize;
    }

    public PoolStats getStats() {
        return new PoolStats(maxSegments, allocatedSegments, freeList.size(), maxSegments - allocatedSegments);
    }

    @Override
    public boolean isUnderPressure() {
        int free = cachedFreeListSize;
        int unallocated = maxSegments - allocatedSegments;
        return (free + unallocated) < (maxSegments * 0.1);
    }

    @Override
    public void warmUp(long targetSegments) {
        targetSegments = Math.min(targetSegments, maxSegments);
        lock.lock();
        try {
            while (allocatedSegments < targetSegments) {
                MemorySegment seg = PanamaNativeAccess.malloc(segmentSize);
                RefCountedMemorySegment refSeg = new RefCountedMemorySegment(seg, segmentSize, this::release);
                freeList.addLast(refSeg);
                allocatedSegments++;
            }
            cachedFreeListSize = freeList.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            if (closed)
                return;
            closed = true;
            while (!freeList.isEmpty()) {
                RefCountedMemorySegment seg = freeList.removeFirst();
                PanamaNativeAccess.free(seg.segment());
            }
            cachedFreeListSize = 0;
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public String poolStats() {
        int free = cachedFreeListSize;
        int allocated = allocatedSegments;
        int unalloc = maxSegments - allocated;
        double utilization = (double) (allocated - free) / maxSegments;
        double allocation = (double) allocated / maxSegments;
        return String
            .format(
                "PoolStats[max=%d, allocated=%d, free=%d, unallocated=%d, utilization=%.1f%%, allocation=%.1f%%]",
                maxSegments,
                allocated,
                free,
                unalloc,
                utilization * 100,
                allocation * 100
            );
    }

    /** Monitoring snapshot of pool metrics. */
    @SuppressForbidden(reason = "custom string builder")
    public static class PoolStats {
        public final int maxSegments;
        public final int allocatedSegments;
        public final int freeSegments;
        public final int unallocatedSegments;
        public final double utilizationRatio;
        public final double allocationRatio;

        PoolStats(int maxSegments, int allocatedSegments, int freeSegments, int unallocatedSegments) {
            this.maxSegments = maxSegments;
            this.allocatedSegments = allocatedSegments;
            this.freeSegments = freeSegments;
            this.unallocatedSegments = unallocatedSegments;
            this.utilizationRatio = (double) (allocatedSegments - freeSegments) / maxSegments;
            this.allocationRatio = (double) allocatedSegments / maxSegments;
        }

        @Override
        public String toString() {
            return String
                .format(
                    "PoolStats[max=%d, allocated=%d, free=%d, unallocated=%d, utilization=%.1f%%, allocation=%.1f%%]",
                    maxSegments,
                    allocatedSegments,
                    freeSegments,
                    unallocatedSegments,
                    utilizationRatio * 100,
                    allocationRatio * 100
                );
        }
    }
}

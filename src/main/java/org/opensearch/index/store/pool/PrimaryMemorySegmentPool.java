/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.block.RefCountedMemorySegment;

@SuppressWarnings("preview")
@SuppressForbidden(reason = "uses custom DirectIO")
public class PrimaryMemorySegmentPool implements Pool<RefCountedMemorySegment>, AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(MemorySegmentPool.class);

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Deque<RefCountedMemorySegment> freeList = new ArrayDeque<>();
    private final Arena sharedArena;
    private final int segmentSize;
    private final int maxSegments;
    private final long totalMemory;
    private final boolean requiresZeroing;

    private volatile boolean closed = false;
    private int allocatedSegments = 0;

    // Cached values to reduce lock overhead
    private volatile int cachedFreeListSize = 0;

    /**
     * Creates a pool with lazy allocation and optional zeroing
     */
    public PrimaryMemorySegmentPool(long totalMemory, int segmentSize) {
        this(totalMemory, segmentSize, true);
    }

    /**
     * Creates a pool with configurable allocation strategy and zeroing
     *
     * @param totalMemory total memory to manage
     * @param segmentSize size of each segment
     * @param requiresZeroing if true, zero segments on release; if false, skip
     * for performance
     */
    public PrimaryMemorySegmentPool(long totalMemory, int segmentSize, boolean requiresZeroing) {
        if (totalMemory % segmentSize != 0) {
            throw new IllegalArgumentException("Total memory must be a multiple of segment size");
        }
        this.totalMemory = totalMemory;
        this.segmentSize = segmentSize;
        this.maxSegments = (int) (totalMemory / segmentSize);
        this.sharedArena = Arena.ofShared();
        this.requiresZeroing = requiresZeroing;
    }

    @Override
    public RefCountedMemorySegment acquire() throws InterruptedException {
        lock.lock();
        try {
            // Try to get from free list first
            if (!freeList.isEmpty()) {
                RefCountedMemorySegment refSegment = freeList.removeFirst();
                cachedFreeListSize = freeList.size();
                // Reset to fresh state (refCount=1, retired=false)
                refSegment.reset();
                return refSegment;
            }

            // Try to allocate new segment if under capacity
            if (allocatedSegments < maxSegments) {
                MemorySegment segment = sharedArena.allocate(segmentSize);
                RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, segmentSize, this::release);
                allocatedSegments++;
                LOGGER.trace("Allocated new segment, total allocated: {}", allocatedSegments);
                return refSegment;
            }

            if (closed) {
                throw new IllegalStateException("Pool is closed");
            }

            throw new PrimaryPoolExhaustedException();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public RefCountedMemorySegment tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        return acquire();
    }

    @Override
    public void release(RefCountedMemorySegment refSegment) {
        lock.lock();
        try {
            if (closed) {
                return; // Don't add back to closed pool
            }

            // Optional zeroing for security vs performance trade-off
            if (requiresZeroing) {
                refSegment.segment().fill((byte) 0);
            }

            // Add back to free list (refCount is 0, will be reset to 1 in acquire())
            freeList.addLast(refSegment);
            cachedFreeListSize = freeList.size();
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Bulk release for releasing multiple segments
     */
    public void releaseAll(RefCountedMemorySegment... segments) {
        if (segments.length == 0) {
            return;
        }

        lock.lock();
        try {
            if (closed) {
                return;
            }

            for (RefCountedMemorySegment refSegment : segments) {
                if (requiresZeroing) {
                    refSegment.segment().fill((byte) 0);
                }
                freeList.addLast(refSegment);
            }
            cachedFreeListSize = freeList.size();
            notEmpty.signalAll(); // Wake up multiple waiters
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
        // Fast path using cached value to avoid locking
        int free = cachedFreeListSize;
        int allocated = allocatedSegments; // volatile read
        int canAllocate = Math.max(0, maxSegments - allocated);
        return (long) (free + canAllocate) * segmentSize;
    }

    /**
     * More accurate but slower version requiring lock
     */
    public long availableMemoryAccurate() {
        lock.lock();
        try {
            int free = freeList.size();
            int canAllocate = Math.max(0, maxSegments - allocatedSegments);
            return (long) (free + canAllocate) * segmentSize;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int pooledSegmentSize() {
        return segmentSize;
    }

    /**
     * Get current pool utilization statistics
     */
    public PoolStats getStats() {
        return new PoolStats(maxSegments, allocatedSegments, freeList.size(), maxSegments - allocatedSegments);
    }

    /**
     * Check if pool is under memory pressure.
     * Returns true when available capacity (free + unallocated) is low.
     *
     * This drives the decision to use secondary pool vs primary pool.
     */
    @Override
    public boolean isUnderPressure() {
        // Calculate available segments (can be served immediately without blocking)
        int free = cachedFreeListSize;
        int unallocated = maxSegments - allocatedSegments;
        int available = free + unallocated;

        // Under pressure if less than x% of pool capacity is available
        return available < (maxSegments * 0.1);
    }

    /**
     * Warm up the pool by pre-allocating segments up to target count
     */
    @Override
    public void warmUp(long targetSegments) {
        targetSegments = Math.min(targetSegments, maxSegments);

        if (allocatedSegments >= targetSegments) {
            return;
        }

        lock.lock();
        try {
            while (allocatedSegments < targetSegments) {
                MemorySegment segment = sharedArena.allocate(segmentSize);
                RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, segmentSize, this::release);
                // Segments in free list have refCount=1, ready for next acquisition
                freeList.add(refSegment);
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
            if (closed) {
                return;
            }
            closed = true;
            cachedFreeListSize = 0;
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }

        if (sharedArena.scope().isAlive()) {
            sharedArena.close();
        }
    }

    @Override
    public String poolStats() {
        int free = cachedFreeListSize;
        int allocated = allocatedSegments;
        int unallocated = maxSegments - allocated;
        return new PoolStats(maxSegments, allocated, free, unallocated).toString();
    }

    /**
     * Pool statistics for monitoring
     */
    @SuppressForbidden(reason = "uses custom string builder")
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
            // Utilization: % of pool capacity actively in use (allocated - free) / max
            // This represents segments held by readers/cache
            this.utilizationRatio = (double) (allocatedSegments - freeSegments) / maxSegments;
            // Allocation: % of pool capacity allocated from OS (may be in use or free)
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

    @Override
    public boolean isClosed() {
        return closed;
    }
}

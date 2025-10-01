/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.block.RefCountedMemorySegment;

@SuppressWarnings("preview")
@SuppressForbidden(reason = "uses cleanup for memory pool")
public class SecondaryMemorySegmentPool implements Pool<RefCountedMemorySegment>, AutoCloseable {
    private static final Logger LOGGER = LogManager.getLogger(SecondaryMemorySegmentPool.class);
    private static final long CLEANUP_INTERVAL_MINUTES = 15;

    private volatile boolean closed = false;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Deque<RefCountedMemorySegment> freeList = new ArrayDeque<>();
    private Arena sharedArena;  // Non-final to allow recreation during renewal
    private final int segmentSize;
    private final int maxSegments;
    private final long totalMemory;
    private final boolean requiresZeroing;

    private int allocatedSegments = 0;

    // Cached values to reduce lock overhead
    private volatile int cachedFreeListSize = 0;

    // Background cleanup
    private final ScheduledExecutorService cleanupExecutor;
    private int consecutiveIdleCycles = 0;

    public SecondaryMemorySegmentPool(long totalMemory, int segmentSize) {
        if (totalMemory % segmentSize != 0) {
            throw new IllegalArgumentException("Total memory must be a multiple of segment size");
        }

        this.totalMemory = totalMemory;
        this.segmentSize = segmentSize;
        this.maxSegments = (int) (totalMemory / segmentSize);
        this.requiresZeroing = true;
        this.sharedArena = Arena.ofShared();

        // Start background cleanup task
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "secondary-pool-cleanup");
            t.setDaemon(true);
            return t;
        });

        cleanupExecutor.scheduleWithFixedDelay(this::performCleanup, CLEANUP_INTERVAL_MINUTES, CLEANUP_INTERVAL_MINUTES, TimeUnit.MINUTES);

        LOGGER
            .info(
                "Created secondary pool with cleanup: capacity={}MB ({} segments of {}KB), cleanupInterval={}min",
                totalMemory / (1024 * 1024),
                maxSegments,
                segmentSize / 1024,
                CLEANUP_INTERVAL_MINUTES
            );
    }

    @Override
    public RefCountedMemorySegment acquire() throws InterruptedException {
        lock.lock();
        try {
            // Check pool state - only accept allocations if ACTIVE
            if (closed) {
                throw new IllegalStateException("Secondary pool is closed no new allocations accepted");
            }

            // Try to get from free list first
            if (!freeList.isEmpty()) {
                RefCountedMemorySegment refSegment = freeList.removeFirst();
                cachedFreeListSize = freeList.size();
                // Reset to fresh state (refCount=1, retired=false)
                refSegment.reset();
                LOGGER.trace("Acquired RefCountedMemorySegment from free list - free: {}", cachedFreeListSize);
                return refSegment;
            }

            // Try to allocate new segment if under capacity
            if (allocatedSegments < maxSegments) {
                MemorySegment segment = sharedArena.allocate(segmentSize);
                RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, segmentSize, this::release);
                allocatedSegments++;
                LOGGER.trace("Allocated new RefCountedMemorySegment in secondary pool - allocated: {}", allocatedSegments);
                return refSegment;
            }

            // Pool is full - throw exception
            throw new SecondaryPoolExhaustedException();
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
        if (refSegment == null) {
            return;
        }

        lock.lock();
        try {
            if (closed) {
                return;
            }

            // Optional zeroing for security
            if (requiresZeroing) {
                refSegment.segment().fill((byte) 0);
            }

            // Add back to free list (refCount is 0, will be reset to 1 in acquire())
            freeList.addLast(refSegment);
            cachedFreeListSize = freeList.size();
            notEmpty.signal();

            LOGGER.trace("RefCountedMemorySegment returned to secondary pool - free: {}", cachedFreeListSize);
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
        int allocated = allocatedSegments;
        int canAllocate = Math.max(0, maxSegments - allocated);
        return (long) (free + canAllocate) * segmentSize;
    }

    @Override
    public int pooledSegmentSize() {
        return segmentSize;
    }

    /**
     * Check if pool is under memory pressure.
     * Returns true when available capacity (free + unallocated) is low.
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

    @Override
    public void warmUp(long targetSegments) {
        // Secondary pool does not support warmup - allocates lazily only
        LOGGER.debug("Secondary pool does not support warmup, allocates on demand");
    }

    @Override
    public String poolStats() {
        int free = cachedFreeListSize;
        int allocated = allocatedSegments;
        int unallocated = maxSegments - allocated;
        double utilization = (double) (allocated - free) / maxSegments * 100;
        double allocation = (double) allocated / maxSegments * 100;
        return String
            .format(
                "SecondaryPool[max=%d, allocated=%d, free=%d, unallocated=%d, utilization=%.1f%%, allocation=%.1f%%, cleanup=enabled]",
                maxSegments,
                allocated,
                free,
                unallocated,
                utilization,
                allocation
            );
    }

    /**
     * Periodic cleanup task - runs every 15 minutes on background thread
     */
    private void performCleanup() {
        if (closed) {
            return;
        }

        // Take the same lock that acquire() uses to block new allocations
        lock.lock();
        try {
            if (allocatedSegments == 0 && cachedFreeListSize == 0) {
                consecutiveIdleCycles++;
                if (consecutiveIdleCycles >= 4) {
                    LOGGER
                        .info(
                            "Secondary pool idle for {} cycles ({}min) - closing pool and releasing memory",
                            consecutiveIdleCycles,
                            consecutiveIdleCycles * CLEANUP_INTERVAL_MINUTES
                        );

                    tryFreeArena();
                }
            } else {
                consecutiveIdleCycles = 0;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Close arena to release memory back to OS
     */
    private void tryFreeArena() {
        lock.lock();
        try {
            int active = allocatedSegments - freeList.size();
            if (active > 0) {
                LOGGER.debug("Cannot free arena: {} active segments still in use", active);
                return;
            }

            closed = true;
            freeList.clear();
            cachedFreeListSize = 0;
            notEmpty.signalAll();

            if (sharedArena.scope().isAlive()) {
                sharedArena.close();
                LOGGER.info("Secondary pool memory safely released to OS");
            }
        } catch (Exception e) {
            LOGGER.warn("Error closing secondary pool arena", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        lock.lock();
        try {
            if (closed)
                return;
            closed = true;
            freeList.clear();
            cachedFreeListSize = 0;
            notEmpty.signalAll();
            int active = allocatedSegments - freeList.size();
            if (active > 0) {
                LOGGER
                    .warn(
                        "Closing with {} active segments still in use (allocated={}, free={})",
                        active,
                        allocatedSegments,
                        freeList.size()
                    );
            }
        } finally {
            lock.unlock();
        }

        try {
            if (sharedArena.scope().isAlive()) {
                sharedArena.close();
                LOGGER.info("Secondary pool memory released to OS");
            }
        } catch (Exception e) {
            LOGGER.warn("Error closing secondary pool arena", e);
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}

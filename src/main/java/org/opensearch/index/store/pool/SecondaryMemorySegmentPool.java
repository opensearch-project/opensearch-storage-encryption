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

@SuppressWarnings("preview")
@SuppressForbidden(reason = "uses cleanup for memory pool")
public class SecondaryMemorySegmentPool implements Pool<MemorySegment>, AutoCloseable {
    private static final Logger LOGGER = LogManager.getLogger(SecondaryMemorySegmentPool.class);
    private static final long CLEANUP_INTERVAL_MINUTES = 15;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Deque<MemorySegment> freeList = new ArrayDeque<>();
    private Arena sharedArena;  // Non-final to allow recreation during renewal
    private final int segmentSize;
    private final int maxSegments;
    private final long totalMemory;
    private final boolean requiresZeroing;

    private volatile boolean retired = false;  // No new allocations, but accepts releases
    private volatile boolean closed = false;   // All resources closed, can be renewed
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
    public MemorySegment acquire() throws InterruptedException {
        lock.lock();
        try {
            // Check if pool is retired - no new allocations allowed
            if (retired) {
                throw new IllegalStateException("Secondary pool is retired: no new allocations accepted");
            }

            // Try to get from free list first
            if (!freeList.isEmpty()) {
                MemorySegment segment = freeList.removeFirst();
                cachedFreeListSize = freeList.size();
                return segment;
            }

            // Try to allocate new segment if under capacity
            if (allocatedSegments < maxSegments) {
                MemorySegment segment = sharedArena.allocate(segmentSize);
                allocatedSegments++;
                LOGGER.trace("Allocated new segment in secondary pool, total allocated: {}", allocatedSegments);
                return segment;
            }

            // Pool is full - throw exception instead of waiting
            if (closed) {
                throw new IllegalStateException("Pool is closed");
            }

            throw new SecondaryPoolExhaustedException();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public MemorySegment tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        lock.lock();
        try {
            // Check if pool is retired - no new allocations allowed
            if (retired) {
                throw new IllegalStateException("Secondary pool is retired: no new allocations accepted");
            }

            // Try to get from free list first
            if (!freeList.isEmpty()) {
                MemorySegment segment = freeList.removeFirst();
                cachedFreeListSize = freeList.size();
                return segment;
            }

            // Try to allocate new segment if under capacity
            if (allocatedSegments < maxSegments) {
                MemorySegment segment = sharedArena.allocate(segmentSize);
                allocatedSegments++;
                return segment;
            }

            // Pool is full - throw exception instead of waiting
            if (closed) {
                throw new IllegalStateException("Pool is closed");
            }

            LOGGER
                .debug(
                    "Secondary pool exhausted: no free segments available. Pool stats: allocated={}/{}, free={}",
                    allocatedSegments,
                    maxSegments,
                    freeList.size()
                );
            throw new SecondaryPoolExhaustedException();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void release(MemorySegment segment) {
        if (segment == null) {
            return;
        }

        lock.lock();
        try {
            // Don't accept releases if fully closed
            if (closed) {
                return;
            }

            // Accept releases even if retired (but no new allocations)
            // Optional zeroing for security
            if (requiresZeroing) {
                segment.fill((byte) 0);
            }

            freeList.addLast(segment);
            cachedFreeListSize = freeList.size();
            notEmpty.signal();

            LOGGER.trace("Released segment to secondary pool, free list size: {}", cachedFreeListSize);
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

    @Override
    public boolean isUnderPressure() {
        return allocatedSegments > (maxSegments * 0.9) && cachedFreeListSize < (maxSegments * 0.1);
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
        return String
            .format(
                "SecondaryPool[max=%d, allocated=%d, free=%d, unallocated=%d, utilization=%.1f%%, cleanup=enabled]",
                maxSegments,
                allocated,
                free,
                unallocated,
                (double) allocated / maxSegments * 100
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
            double utilization = (double) allocatedSegments / maxSegments;

            LOGGER
                .debug(
                    "Secondary pool cleanup cycle: utilization={}%, free={}, allocated={}",
                    String.format("%.1f", utilization * 100),
                    cachedFreeListSize,
                    allocatedSegments
                );

            if (allocatedSegments == 0 && cachedFreeListSize == 0) {
                consecutiveIdleCycles++;
                if (consecutiveIdleCycles >= 4) {
                    LOGGER
                        .info(
                            "Secondary pool idle for {} cycles ({}min) - closing pool and releasing memory",
                            consecutiveIdleCycles,
                            consecutiveIdleCycles * CLEANUP_INTERVAL_MINUTES
                        );

                    closeArena();
                    cleanupExecutor.shutdownNow();
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
    private void closeArena() {
        // Mark closed under lock to prevent new acquisitions
        lock.lock();
        try {
            retired = true;  // Transition through retired to closed
            closed = true;
            freeList.clear();
            cachedFreeListSize = 0;
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }

        // Close arena outside lock to avoid blocking
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
            retired = true;  // Transition through retired to closed
            closed = true;
            freeList.clear();
            cachedFreeListSize = 0;
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }

        if (sharedArena.scope().isAlive()) {
            sharedArena.close();
        }

        LOGGER.info("Secondary pool with cleanup closed");
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    /**
     * Retire the pool - no new allocations, but releases are still accepted.
     * This allows existing segments to be returned while preventing new allocations.
     */
    public void retire() {
        lock.lock();
        try {
            if (!closed) {
                retired = true;
                LOGGER.info("Secondary pool retired: no new allocations accepted, releases still accepted");
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check if the pool is in retired state.
     */
    public boolean isRetired() {
        return retired;
    }

    /**
     * Renew a closed pool by creating a new arena and resetting state.
     * Only works if the pool is fully closed.
     */
    public synchronized void renew() {
        if (!closed) {
            LOGGER.debug("Cannot renew pool that is not closed");
            return;
        }

        lock.lock();
        try {
            // Create new arena to replace the closed one
            sharedArena = Arena.ofShared();

            // Reset all state
            retired = false;
            closed = false;
            allocatedSegments = 0;
            freeList.clear();
            cachedFreeListSize = 0;
            consecutiveIdleCycles = 0;

            LOGGER.info("Secondary pool renewed: new arena created, ready for allocations");
        } finally {
            lock.unlock();
        }
    }
}

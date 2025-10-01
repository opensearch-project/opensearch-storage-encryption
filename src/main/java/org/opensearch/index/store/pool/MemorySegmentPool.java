/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.block.RefCountedMemorySegment;

/**
 * Exception hierarchy for memory pool exhaustion
 */

class PoolExhaustedException extends RuntimeException {

    public PoolExhaustedException(String message) {
        super(message);
    }
}

class PrimaryPoolExhaustedException extends PoolExhaustedException {

    public PrimaryPoolExhaustedException() {
        super("Primary pool exhausted: no free segments available");
    }
}

class SecondaryPoolExhaustedException extends PoolExhaustedException {
    public SecondaryPoolExhaustedException() {
        super("Secondary pool exhausted: no free segments available");
    }
}

class SecondaryPoolUnavailableException extends PoolExhaustedException {

    public SecondaryPoolUnavailableException(String reason) {
        super("Secondary pool unavailable: " + reason);
    }
}

class NoOffHeapMemoryException extends PoolExhaustedException {
    public NoOffHeapMemoryException() {
        super("No off-heap memory left: all pool levels exhausted");
    }
}

@SuppressForbidden(reason = "temporary")
public class MemorySegmentPool implements Pool<RefCountedMemorySegment>, AutoCloseable {
    private static final Logger LOGGER = LogManager.getLogger(MemorySegmentPool.class);
    private final Object secondaryLock = new Object();

    private final PrimaryMemorySegmentPool primaryPool;
    private volatile SecondaryMemorySegmentPool secondaryPool;

    private volatile boolean closed = false;

    public MemorySegmentPool(long totalOffHeap, int segmentSize) {
        this.primaryPool = new PrimaryMemorySegmentPool(totalOffHeap, segmentSize);
        this.secondaryPool = null; // Initialize only when needed
    }

    @Override
    public RefCountedMemorySegment acquire() throws InterruptedException {
        // Try primary pool first
        try {
            return primaryPool.acquire();
        } catch (PrimaryPoolExhaustedException e) {
            // Fallback to secondary pool
            try {
                tryAcquireFromSecondary();
                return secondaryPool.acquire();

            } catch (SecondaryPoolExhaustedException | SecondaryPoolUnavailableException se) {
                // Final fallback to ephemeral pool
                try {
                    EphemeralMemorySegmentPool ephemeral = new EphemeralMemorySegmentPool(primaryPool.pooledSegmentSize());
                    return ephemeral.acquire();
                } catch (Exception ee) {
                    throw new NoOffHeapMemoryException();
                }
            }
        }
    }

    @Override
    public RefCountedMemorySegment tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        return acquire();
    }

    private void tryAcquireFromSecondary() throws SecondaryPoolUnavailableException {
        if (secondaryPool == null || secondaryPool.isClosed()) {
            synchronized (secondaryLock) {
                if (secondaryPool == null || secondaryPool.isClosed()) {
                    try {
                        LOGGER.info("Creating fresh secondary pool");
                        long secondaryMemory = primaryPool.totalMemory() / 2;
                        secondaryPool = new SecondaryMemorySegmentPool(secondaryMemory, primaryPool.pooledSegmentSize());
                    } catch (Exception e) {
                        String reason = "Failed to create secondary pool: " + e.getMessage();
                        LOGGER.error(reason, e);
                        throw new SecondaryPoolUnavailableException(reason);
                    }
                }
            }
        }
    }

    @Override
    public void release(RefCountedMemorySegment refSegment) {
        // No-op: segments auto-release to their source pool via callback when refCount hits 0
        // Each segment's callback points to its specific pool (primary/secondary/ephemeral)
        // Users should call decRef() directly instead of pool.release()
    }

    @Override
    public long totalMemory() {
        long total = primaryPool.totalMemory();
        if (secondaryPool != null) {
            total += secondaryPool.totalMemory();
        }
        return total;
    }

    @Override
    public long availableMemory() {
        long available = primaryPool.availableMemory();
        if (secondaryPool != null) {
            available += secondaryPool.availableMemory();
        }
        return available;
    }

    @Override
    public int pooledSegmentSize() {
        return primaryPool.pooledSegmentSize();
    }

    @Override
    public boolean isUnderPressure() {
        return primaryPool.isUnderPressure() && (secondaryPool == null || secondaryPool.isUnderPressure());
    }

    @Override
    public void warmUp(long targetSegments) {
        primaryPool.warmUp(targetSegments);
        // Secondary pool is created lazily, no warmup needed until first use
    }

    @Override
    public void close() {
        try (primaryPool) {
            closed = true;
        }
        if (secondaryPool != null) {
            secondaryPool.close();
        }
    }

    @Override
    public String poolStats() {
        return String
            .format(
                "RoutingPool[\n  primary=%s,\n  secondary=%s,\n  closed=%s\n]",
                primaryPool.poolStats(),
                secondaryPool != null ? secondaryPool.poolStats() : "not-initialized",
                closed
            );
    }

    @Override
    public boolean isClosed() {
        return primaryPool.isClosed() && (secondaryPool == null || secondaryPool.isClosed());
    }
}

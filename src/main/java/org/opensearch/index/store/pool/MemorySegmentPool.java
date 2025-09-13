/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

@SuppressWarnings("preview")
public class MemorySegmentPool implements Pool<MemorySegmentPool.SegmentHandle>, AutoCloseable {
    private static final Logger LOGGER = LogManager.getLogger(MemorySegmentPool.class);
    private final Object secondaryLock = new Object();

    private final PrimaryMemorySegmentPool primaryPool;
    private volatile SecondaryMemorySegmentPool secondaryPool;

    private volatile boolean closed = false;

    public MemorySegmentPool(long totalOffHeap, int segmentSize) {
        this.primaryPool = new PrimaryMemorySegmentPool(totalOffHeap, segmentSize);
        this.secondaryPool = null; // Initialize only when needed
    }

    /**
     * Handle that carries the segment and its owning pool.
     */
    public record SegmentHandle(MemorySegment segment, Pool<MemorySegment> origin) implements AutoCloseable {
        public void release() {
            origin.release(segment);
        }

        @Override
        public void close() {
            release();
        }
    }

    @Override
    public SegmentHandle acquire() throws InterruptedException {
        // Try primary pool first
        try {
            return new SegmentHandle(primaryPool.acquire(), primaryPool);
        } catch (PrimaryPoolExhaustedException e) {
            // Fallback to secondary pool
            try {
                tryAcquireFromSecondary();
                return new SegmentHandle(secondaryPool.acquire(), secondaryPool);
            } catch (SecondaryPoolExhaustedException | SecondaryPoolUnavailableException se) {
                // Final fallback to ephemeral pool
                try {
                    EphemeralMemorySegmentPool ephemeral = new EphemeralMemorySegmentPool(primaryPool.pooledSegmentSize());
                    return new SegmentHandle(ephemeral.acquire(), ephemeral);
                } catch (Exception ee) {
                    throw new NoOffHeapMemoryException();
                }
            }
        }
    }

    @Override
    public SegmentHandle tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        // Try primary pool first
        try {
            MemorySegment seg = primaryPool.tryAcquire(timeout, unit);
            return new SegmentHandle(seg, primaryPool);
        } catch (PrimaryPoolExhaustedException e) {
            // Fallback to secondary pool
            try {
                tryAcquireFromSecondary();
                MemorySegment seg = secondaryPool.tryAcquire(timeout, unit);
                return new SegmentHandle(seg, secondaryPool);
            } catch (SecondaryPoolExhaustedException | SecondaryPoolUnavailableException se) {
                // Final fallback to ephemeral pool
                try {
                    EphemeralMemorySegmentPool ephemeral = new EphemeralMemorySegmentPool(primaryPool.pooledSegmentSize());
                    return new SegmentHandle(ephemeral.acquire(), ephemeral);
                } catch (Exception ee) {
                    throw new NoOffHeapMemoryException();
                }
            }
        }
    }

    private void tryAcquireFromSecondary() throws SecondaryPoolUnavailableException {
        if (secondaryPool == null || secondaryPool.isClosed()) {
            synchronized (secondaryLock) {
                if (secondaryPool == null || secondaryPool.isClosed()) {
                    try {
                        if (secondaryPool != null && secondaryPool.isClosed()) {
                            LOGGER.info("Renewing closed secondary pool");
                            secondaryPool.renew();
                        } else {
                            LOGGER.info("Creating secondary pool on demand");
                            long secondaryMemory = primaryPool.totalMemory() / 2;
                            secondaryPool = new SecondaryMemorySegmentPool(secondaryMemory, primaryPool.pooledSegmentSize());
                        }
                    } catch (Exception e) {
                        String reason = "Failed to create/renew secondary pool: " + e.getMessage();
                        LOGGER.error(reason, e);
                        throw new SecondaryPoolUnavailableException(reason);
                    }
                }
            }
        }
    }

    @Override
    public void release(SegmentHandle handle) {
        if (handle != null) {
            handle.release();
        }
    }

    @Override
    public long totalMemory() {
        return primaryPool.totalMemory() + secondaryPool.totalMemory();
    }

    @Override
    public long availableMemory() {
        return primaryPool.availableMemory() + secondaryPool.availableMemory();
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

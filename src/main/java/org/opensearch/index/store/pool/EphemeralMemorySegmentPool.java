/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.block.RefCountedMemorySegment;

/**
 * An ephemeral memory segment pool that allocates temporary memory segments from a shared arena.
 * 
 * <p>This pool implementation is designed for short-lived memory allocations that don't require
 * sophisticated pooling mechanisms. Each {@link #acquire()} call creates a new memory segment
 * from the shared arena, and segments are automatically released when their reference count
 * reaches zero.
 * 
 * <p>Key characteristics:
 * <ul>
 * <li>Uses a shared {@link Arena} for all allocations</li>
 * <li>No pre-allocation or segment reuse</li>
 * <li>Simple lifecycle - arena is closed when any segment is released</li>
 * <li>Suitable for temporary or testing scenarios</li>
 * <li>Most Pool interface methods are not supported</li>
 * </ul>
 * 
 * <p><strong>Warning:</strong> This implementation closes the arena when any segment is released,
 * making it unsuitable for scenarios where multiple segments need to coexist. It's primarily
 * intended for single-segment use cases or testing.
 * 
 * @opensearch.internal
 */
@SuppressWarnings("preview")
@SuppressForbidden(reason = "allocates standalone arenas per segment")
public class EphemeralMemorySegmentPool implements Pool<RefCountedMemorySegment>, AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(EphemeralMemorySegmentPool.class);
    private final int segmentSize;

    /**
     * Creates a new EphemeralMemorySegmentPool with the specified segment size.
     * 
     * @param segmentSize the size in bytes for each allocated memory segment
     */
    public EphemeralMemorySegmentPool(int segmentSize) {
        this.segmentSize = segmentSize;
    }

    @Override
    public RefCountedMemorySegment acquire() {
        // Each segment gets its own confined arena
        final Arena arena = Arena.ofShared();
        final MemorySegment segment = arena.allocate(segmentSize);

        // Return a refcounted wrapper that closes this arena upon release
        return new RefCountedMemorySegment(segment, segmentSize, _ -> {
            try {
                arena.close(); // Frees native memory immediately
            } catch (Exception e) {
                LOGGER.warn("Failed to close ephemeral arena", e);
            }
        });
    }

    @Override
    public void release(RefCountedMemorySegment refSegment) {
        // no-op, as release is handled in RefCountedMemorySegment’s callback
    }

    @Override
    public RefCountedMemorySegment tryAcquire(long timeout, TimeUnit unit) {
        return acquire();
    }

    @Override
    public void close() {
        // no global arenas, nothing to close
    }

    @Override
    public String poolStats() {
        return "EphemeralMemorySegmentPool[1 arena per segment, size=" + segmentSize + "]";
    }

    @Override
    public long totalMemory() {
        return 0;
    }

    @Override
    public long availableMemory() {
        return 0;
    }

    @Override
    public int pooledSegmentSize() {
        return segmentSize;
    }

    @Override
    public boolean isUnderPressure() {
        return false;
    }

    @Override
    public void warmUp(long numBlocks) {}

    @Override
    public boolean isClosed() {
        return false;
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.TimeUnit;

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
    private final Arena arena;
    private final int segmentSize;

    /**
     * Creates a new EphemeralMemorySegmentPool with the specified segment size.
     * 
     * @param segmentSize the size in bytes for each allocated memory segment
     */
    public EphemeralMemorySegmentPool(int segmentSize) {
        this.segmentSize = segmentSize;
        this.arena = Arena.ofShared();
    }

    @Override
    public RefCountedMemorySegment acquire() {
        MemorySegment segment = arena.allocate(segmentSize);
        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, segmentSize, this::release);
        return refSegment;
    }

    @Override
    public void release(RefCountedMemorySegment refSegment) {
        close();
    }

    @Override
    public void close() {
        arena.close();
    }

    @Override
    public RefCountedMemorySegment tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        return acquire();
    }

    @Override
    public long totalMemory() {
        throw new UnsupportedOperationException("Unimplemented method 'totalMemory'");
    }

    @Override
    public String poolStats() {
        return String.format("EphemeralPool[size=%d]", segmentSize);
    }

    @Override
    public long availableMemory() {
        throw new UnsupportedOperationException("Unimplemented method 'availableMemory'");
    }

    @Override
    public int pooledSegmentSize() {
        throw new UnsupportedOperationException("Unimplemented method 'pooledSegmentSize'");
    }

    @Override
    public boolean isUnderPressure() {
        throw new UnsupportedOperationException("Unimplemented method 'isUnderPressure'");
    }

    @Override
    public void warmUp(long numBlocks) {
        throw new UnsupportedOperationException("Unimplemented method 'warmUp'");
    }

    @Override
    public boolean isClosed() {
        throw new UnsupportedOperationException("Unimplemented method isClosed");
    }
}

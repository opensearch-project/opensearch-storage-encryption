/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import org.opensearch.index.store.block_cache.BlockCacheValue;

/**
 * A GC-managed wrapper around a direct {@link ByteBuffer} that implements {@link BlockCacheValue}.
 *
 * <p>No ref counting or generation tracking. The JVM's GC frees the backing
 * DirectByteBuffer when this object becomes unreachable.
 *
 * <p>{@link #close()} marks the entry as closed so that L1 cache (BlockSlotTinyCache)
 * rejects it on next access via {@link #tryPin()}, forcing a reload from L2/disk.
 *
 * @opensearch.internal
 */
public final class RefCountedByteBuffer implements BlockCacheValue<RefCountedByteBuffer> {

    private final ByteBuffer buffer;
    private final int length;
    private final MemorySegment segment;
    private volatile boolean closed = false;

    public RefCountedByteBuffer(ByteBuffer buffer, int length) {
        this.buffer = buffer;
        this.length = length;
        this.segment = MemorySegment.ofBuffer(buffer);
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    public MemorySegment segment() {
        return segment;
    }

    @Override
    public RefCountedByteBuffer value() {
        return this;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public int getGeneration() {
        return 0;
    }

    @Override
    public boolean tryPin() {
        return !closed;
    }

    @Override
    public void unpin() {}

    @Override
    public void decRef() { closed = true; }

    @Override
    public void close() {
        closed = true;
    }
}

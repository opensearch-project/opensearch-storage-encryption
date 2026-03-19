/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block;

import java.nio.ByteBuffer;

import org.opensearch.index.store.block_cache.BlockCacheValue;

/**
 * A GC-managed wrapper around a direct {@link ByteBuffer} that implements {@link BlockCacheValue}.
 *
 * <p>Unlike {@link RefCountedMemorySegment}, this uses no reference counting or CAS operations.
 * The JVM's garbage collector handles lifecycle: when no thread holds a reference to this
 * object, the backing DirectByteBuffer is eligible for GC, and its Cleaner will free native memory.
 *
 * <p>Pin/unpin are no-ops — there is no pool recycling, so no use-after-recycle risk.
 * Generation is always 0 — there is no recycling, so staleness detection is unnecessary.
 */
public final class RefCountedByteBuffer implements BlockCacheValue<RefCountedByteBuffer> {

    private final ByteBuffer buffer;
    private final int length;
    private volatile boolean closed = false;

    public RefCountedByteBuffer(ByteBuffer buffer, int length) {
        this.buffer = buffer;
        this.length = length;
    }

    @Override
    public RefCountedByteBuffer value() {
        return this;
    }

    @Override
    public int length() {
        return length;
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    @Override
    public boolean tryPin() {
        return !closed;
    }

    @Override
    public void unpin() {
        // no-op
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void decRef() {
        closed = true;
    }

    @Override
    public int getGeneration() {
        return 0;
    }
}

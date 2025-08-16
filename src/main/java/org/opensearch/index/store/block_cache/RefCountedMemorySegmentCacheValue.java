/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A cache value wrapper for {@link RefCountedMemorySegment} that implements {@link BlockCacheValue}.
 *
 * This class represents an entry in the block cache. It handles reference counting semantics
 * to ensure that segments are only released when all readers have completed using them.
 *
 * This value type supports both borrowing (no ref count increment) and pinning (with ref count increment),
 * depending on usage semantics (e.g., shared reads vs ownership transfer).
 */
public final class RefCountedMemorySegmentCacheValue implements BlockCacheValue<RefCountedMemorySegment> {

    private final RefCountedMemorySegment refSegment;
    private final int length;

    /**
     * Creates a new cache value wrapping the given reference-counted memory segment.
     *
     * @param refSegment the reference-counted memory segment to wrap
     * @throws IllegalArgumentException if refSegment is null
     */
    public RefCountedMemorySegmentCacheValue(RefCountedMemorySegment refSegment) {
        if (refSegment == null) {
            throw new IllegalArgumentException("refSegment must not be null");
        }
        this.refSegment = refSegment;
        this.length = refSegment.length();
    }

    /**
     * Returns the wrapped {@link RefCountedMemorySegment}.
     */
    public RefCountedMemorySegment getRefSegment() {
        return refSegment;
    }

    /**
     * Borrows the segment without incrementing the reference count.
     *
     * @return the wrapped segment without touching ref count
     */
    @Override
    public RefCountedMemorySegment borrow() {
        return refSegment;
    }

    @Override
    public boolean tryBorrow() {
        AtomicInteger refCount = refSegment.getRefCount();
        if (refCount == null) {
            return false;
        }

        int current = refCount.get();
        return current > 0 && refCount.compareAndSet(current, current + 1);
    }

    /**
     * Returns the logical length of the block (usually equal to the segment length).
     */
    @Override
    public int length() {
        return length;
    }

    /**
     * Releases the segment by decrementing its reference count.
     * If the ref count reaches zero, the segment is fully released.
     */
    @Override
    public void close() {
        // Reader is done — drop ownership
        refSegment.decRef();
    }
}

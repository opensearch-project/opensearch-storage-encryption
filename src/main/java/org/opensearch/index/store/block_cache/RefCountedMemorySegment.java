/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("preview")
/**
 * A wrapper around a  {@link MemorySegment} that implements reference counting.
 * This allows shared use of the segment (e.g. across multiple readers) while ensuring
 * the underlying resource is released exactly once when no longer in use.
 *
 * The segment is released via a {@link SegmentReleaser} callback when its reference count
 * drops to zero.
 */
public final class RefCountedMemorySegment {

    /** Underlying memory segment holding the actual data. */
    private final MemorySegment segment;

    /** Logical length of the segment; used when returning a sliced view. */
    private final int length;

    /**
     * Reference counter for the segment. Starts at 1 to represent ownership by
     * the creator (or cache). Clients increment the counter via {@link #incRef()}
     * and decrement it via {@link #decRef()}. When the counter reaches zero,
     * the segment is released.
     *
     * Note: We use AtomicInteger for safe updates in concurrent access scenarios.
     */
    private final AtomicInteger refCount = new AtomicInteger(1);

    /**
     * Callback to release the memory segment when the reference count reaches zero.
     * Typically this is responsible for returning the segment to a pool or closing it.
     */
    private final SegmentReleaser onFullyReleased;

    /**
     * Creates a new reference-counted memory segment.
     *
     * @param segment the actual memory segment being tracked
     * @param length the logical length of the data in the segment
     * @param onFullyReleased a callback to invoke when the segment is no longer in use
     */
    public RefCountedMemorySegment(MemorySegment segment, int length, SegmentReleaser onFullyReleased) {
        if (segment == null || onFullyReleased == null) {
            throw new IllegalArgumentException("segment and onFullyReleased must not be null");
        }
        this.segment = segment;
        this.length = length;
        this.onFullyReleased = onFullyReleased;
    }

    /**
     * Increments the reference count.
     * Should be called whenever a new consumer starts using the segment.
     *
     * @throws IllegalStateException if the segment has already been released (refCount <= 0)
     */
    public void incRef() {
        int count = refCount.incrementAndGet();
        if (count <= 1) {
            throw new IllegalStateException("Attempted to revive a released segment (refCount=" + count + ")");
        }
    }

    /**
     * Decrements the reference count.
     * If the reference count reaches zero, the underlying segment is released via the callback.
     *
     * @throws IllegalStateException if refCount underflows (i.e., decremented below zero)
     */
    public void decRef() {
        int prev = refCount.getAndDecrement();
        if (prev == 1) {
            // This thread decremented the last ref, so it's responsible for releasing
            onFullyReleased.release(segment);
        } else if (prev <= 0) {
            throw new IllegalStateException("decRef underflow (refCount=" + (prev - 1) + ')');
        }
    }

    /**
     * Returns the current ref count.
     * This is mainly for diagnostics or metrics.
     */
    public AtomicInteger getRefCount() {
        return refCount;
    }

    /**
     * Returns a sliced view of the segment from offset 0 to `length`.
     * This avoids exposing unused memory region (e.g. for partially filled buffers).
     */
    public MemorySegment segment() {
        return segment.asSlice(0, length);
    }

    /**
     * Returns the logical length of the data inside the segment.
     */
    public int length() {
        return length;
    }
}

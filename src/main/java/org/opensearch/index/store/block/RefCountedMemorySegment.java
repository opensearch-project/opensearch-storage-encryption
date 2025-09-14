/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.index.store.block_cache.BlockCacheValue;

@SuppressWarnings("preview")
/**
 * A wrapper around a  {@link MemorySegment} that implements reference counting and cache semantics.
 * This allows shared use of the segment (e.g. across multiple readers) while ensuring
 * the underlying resource is released exactly once when no longer in use.
 *
 * The segment is released via a {@link BlockReleaser} callback when its reference count
 * drops to zero. Includes cache-specific retirement state for safe eviction.
 */
public final class RefCountedMemorySegment implements BlockCacheValue<RefCountedMemorySegment> {

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
    private final BlockReleaser<MemorySegment> onFullyReleased;

    /**
     * VarHandle for atomic access to the retired field
     */
    private static final VarHandle RETIRED;
    static {
        try {
            RETIRED = MethodHandles.lookup().findVarHandle(RefCountedMemorySegment.class, "retired", boolean.class);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new Error(e);
        }
    }

    /**
     * Cache retirement state - when true, no new pins are allowed
     */
    private volatile boolean retired = false;

    /**
     * Creates a new reference-counted memory segment.
     *
     * @param segment the actual memory segment being tracked
     * @param length the logical length of the data in the segment
     * @param onFullyReleased a callback to invoke when the segment is no longer in use
     */
    public RefCountedMemorySegment(MemorySegment segment, int length, BlockReleaser<MemorySegment> onFullyReleased) {
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
     * @throws IllegalStateException if the segment has already been released
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
    @Override
    public int length() {
        return length;
    }

    // === BlockCacheValue Implementation ===

    /**
     * Attempts to pin this segment for use, incrementing the reference count.
     * Uses CAS loops for thread-safe pinning.
     * 
     * @return true if successfully pinned, false if retired or already released
     */
    @Override
    public boolean tryPin() {
        try {
            while (!this.retired) {
                int r = refCount.get();
                if (r == 0) {
                    return false; // already released
                }

                if (refCount.compareAndSet(r, r + 1)) {
                    return true; // successfully pinned
                }

                Thread.onSpinWait();
            }

            return false; // retired while we were spinning
        } catch (IllegalStateException e) {
            // Race condition occurred - segment was released during pinning attempt
            return false;
        }
    }

    /**
     * Releases a previously acquired pin by decrementing the reference count.
     */
    @Override
    public void unpin() {
        decRef();
    }

    /**
     * Closes this cache value, marking it as retired and dropping the cache's reference.
     * Called exactly once by the cache's removal listener.
     */
    @Override
    public void close() {
        if ((boolean) RETIRED.compareAndSet(this, false, true)) {
            // Drop the cache's ownership reference. If no readers are pinned, this will free now.
            decRef();
        }
    }

    /**
     * Returns this segment instance for cache value access.
     */
    @Override
    public RefCountedMemorySegment value() {
        return this;
    }

    /**
     * Returns true if this value has been retired from the cache.
     */
    @Override
    public boolean isRetired() {
        return this.retired;
    }
}

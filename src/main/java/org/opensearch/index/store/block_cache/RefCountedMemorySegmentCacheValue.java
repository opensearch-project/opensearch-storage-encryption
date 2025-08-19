/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cache value for a fixed-size block backed by a {@link RefCountedMemorySegment}.
 *
 * <p>Contract:
 * <ul>
 *   <li>Cache owns one reference on construction (segment refcount starts >= 1).</li>
 *   <li>Callers must {@link #tryPin()} before accessing {@link #value()} and always {@link #unpin()} in a finally block.</li>
 *   <li>When the cache removes this entry, it invokes {@link #close()} exactly once:
 *       this marks the value as retired (rejecting new pins) and drops the cache’s ref;
 *       the segment is actually freed when the last pin releases.</li>
 * </ul>
 *
 * <p>Thread-safety: All methods are thread-safe. Refcount increments use CAS loops; decrements rely on
 * {@link RefCountedMemorySegment#decRef()} to free exactly once when refcount reaches zero.
 */
public final class RefCountedMemorySegmentCacheValue implements BlockCacheValue<RefCountedMemorySegment> {

    private final RefCountedMemorySegment seg; // must hold an initial ref on construction
    private final int length;
    private final AtomicBoolean retired = new AtomicBoolean(false);

    /**
     * @param seg backing segment with a positive initial refcount (the cache’s hold)
     */
    public RefCountedMemorySegmentCacheValue(RefCountedMemorySegment seg) {
        if (seg == null) {
            throw new IllegalArgumentException("seg must not be null");
        }
        // Defensive check: require a positive refcount so the cache truly owns one reference.
        AtomicInteger rc = seg.getRefCount();
        if (rc == null || rc.get() <= 0) {
            throw new IllegalArgumentException("seg must have a positive initial refcount");
        }
        this.seg = seg;
        this.length = seg.length();
    }

    public boolean tryPin() {
        AtomicInteger rc = this.seg.getRefCount();
        if (rc == null) {
            return false;
        }

        while (!this.retired.get()) {
            int r = rc.get();
            if (r == 0) {
                return false; // already released
            }

            if (rc.compareAndSet(r, r + 1)) {
                return true; // successfully pinned
            }

            Thread.onSpinWait();
        }

        return false; // retired while we were spinning
    }

    /** Releases a previously acquired pin. May free the segment if this was the last reference. */
    @Override
    public void unpin() {
        seg.decRef();
    }

    /**
     * Called exactly once by the cache’s removalListener. This is why we keep removal 
     * listener single threaded. 
     * <p>Marks this value retired and drops the cache’s reference. Actual free happens when the last
     * outstanding pin (if any) releases.
     */
    @Override
    public void close() {
        if (retired.compareAndSet(false, true)) {
            // Drop the cache’s ownership reference. If no readers are pinned, this will free now.
            // the uderlying memory.
            seg.decRef();
        }
    }

    /** Returns the wrapped segment for read-only use while pinned. */
    @Override
    public RefCountedMemorySegment value() {
        return seg;
    }

    /** Logical size (bytes) of this block. */
    @Override
    public int length() {
        return length;
    }

    /** True once this value has been removed from the cache and refuses new pins. */
    @Override
    public boolean isRetired() {
        return retired.get();
    }

    @Override
    public String toString() {
        final AtomicInteger rc = seg.getRefCount();
        final int r = (rc != null) ? rc.get() : -1;
        return "RefCountedMemorySegmentCacheValue{len=" + length + ", retired=" + retired.get() + ", refs=" + r + "}";
    }
}

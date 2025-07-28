/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("preview")
public final class RefCountedMemorySegment {
    private final MemorySegment segment;
    private final int length;
    private final AtomicInteger refCount = new AtomicInteger(1);
    private final SegmentReleaser onFullyReleased;

    public RefCountedMemorySegment(MemorySegment segment, int length, SegmentReleaser onFullyReleased) {
        if (segment == null || onFullyReleased == null) {
            throw new IllegalArgumentException("segment and onFullyReleased must not be null");
        }
        this.segment = segment;
        this.length = length;
        this.onFullyReleased = onFullyReleased;
    }

    public void incRef() {
        int count = refCount.incrementAndGet();
        if (count <= 1) {
            throw new IllegalStateException("Attempted to revive a released segment (refCount=" + count + ")");
        }
    }

    public void decRef() {
        int prev = refCount.getAndDecrement();
        if (prev == 1) {
            onFullyReleased.release(segment);
        } else if (prev <= 0) {
            throw new IllegalStateException("decRef underflow (refCount=" + (prev - 1) + ')');
        }
    }

    public AtomicInteger getRefCount() {
        return refCount;
    }

    public MemorySegment segment() {
        return segment.asSlice(0, length);
    }

    public int length() {
        return length;
    }
}

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
    private SegmentReleaser onFullyReleased;

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
        int count = refCount.decrementAndGet();
        if (count == 0) {
            if (onFullyReleased != null) {
                onFullyReleased.release(segment);
            }
        } else if (count < 0) {
            throw new IllegalStateException("Too many decRef calls (refCount=" + count + ")");
        }
    }

    public void setOnFullyReleased(SegmentReleaser callback) {
        this.onFullyReleased = callback;
    }

    public MemorySegment segment() {
        return segment.asSlice(0, length);
    }

    public int length() {
        return length;
    }
}

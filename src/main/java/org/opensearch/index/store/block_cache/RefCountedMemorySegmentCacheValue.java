/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

public final class RefCountedMemorySegmentCacheValue implements BlockCacheValue<RefCountedMemorySegment> {

    private final RefCountedMemorySegment refSegment;
    private final int length;

    public RefCountedMemorySegmentCacheValue(RefCountedMemorySegment refSegment) {
        if (refSegment == null) {
            throw new IllegalArgumentException("refSegment must not be null");
        }
        this.refSegment = refSegment;
        this.length = refSegment.length();
    }

    public RefCountedMemorySegment getRefSegment() {
        return refSegment;
    }

    @Override
    public RefCountedMemorySegment block() {
        // Reader wants to retain usage so increment the reference for its usage.
        refSegment.incRef();
        return refSegment;
    }

    @Override
    public RefCountedMemorySegment borrowBlock() {
        return refSegment;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public void close() {
        // Reader is done — drop ownership
        refSegment.decRef();
    }
}

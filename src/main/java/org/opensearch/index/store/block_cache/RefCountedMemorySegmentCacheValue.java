/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.lang.foreign.MemorySegment;

@SuppressWarnings("preview")
public final class RefCountedMemorySegmentCacheValue implements BlockCacheValue<MemorySegment> {

    private final RefCountedMemorySegment refSegment;
    private final int length;

    public RefCountedMemorySegmentCacheValue(RefCountedMemorySegment refSegment) {
        if (refSegment == null) {
            throw new IllegalArgumentException("refSegment must not be null");
        }
        this.refSegment = refSegment;
        this.length = refSegment.length();

        // Explicitly claim ownership on behalf of the cache
        // On eviction this reference is decremented.
        this.refSegment.incRef();
    }

    public RefCountedMemorySegment getRefSegment() {
        return refSegment;
    }

    @Override
    public MemorySegment block() {
        // Reader wants to retain usage so increment the reference for its usage.
        refSegment.incRef();
        return refSegment.segment();
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public void close() {
        // Reader or cache is done — drop ownership
        refSegment.decRef();
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.lang.foreign.MemorySegment;

@SuppressWarnings("preview")
public final class MemorySegmentCacheValue implements BlockCacheValue<MemorySegment> {
    private final MemorySegment segment;
    private final Pool<MemorySegment> pool;
    private volatile boolean released = false;

    private final int length;

    public MemorySegmentCacheValue(MemorySegment segment, int length, Pool<MemorySegment> pool) {
        if (segment == null || pool == null) {
            throw new IllegalArgumentException("segment and pool must not be null");
        }
        if (length < 0 || length > segment.byteSize()) {
            throw new IllegalArgumentException("Invalid length: " + length);
        }
        this.segment = segment;
        this.length = length;
        this.pool = pool;
    }

    @Override
    public MemorySegment block() {
        return segment.asSlice(0, length);
    }

    @Override
    public void close() {
        if (!released) {
            released = true;
            pool.release(segment);
        }
    }

    @Override
    public int length() {
        return length;
    }
}

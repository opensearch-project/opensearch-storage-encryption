/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.lang.foreign.MemorySegment;

/**
 * A functional interface that defines a callback to be invoked when a {@link MemorySegment}
 * is no longer in use and can be released.
 *
 * This is typically used in conjunction with {@link RefCountedMemorySegment} to implement
 * custom memory cleanup logic such as returning the segment to a memory pool or closing
 * native resources.
 *
 * Implementations must be idempotent and thread-safe if used concurrently.
 */
@FunctionalInterface
@SuppressWarnings("preview")
public interface SegmentReleaser {

    /**
     * Releases the given {@link MemorySegment}. This method is called when
     * the reference count for a {@link RefCountedMemorySegment} reaches zero.
     *
     * @param segment the memory segment to release
     */
    void release(MemorySegment segment);
}

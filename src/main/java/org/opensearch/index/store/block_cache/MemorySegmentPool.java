/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("preview")
public interface MemorySegmentPool {
    MemorySegment acquire() throws Exception;

    MemorySegment tryAcquire(long timeout, TimeUnit unit) throws InterruptedException;

    void release(MemorySegment segment);

    long totalMemory();

    long availableMemory();

    int pooledSegmentSize();

    void warmUp(int targetSegments);

    void close();
}

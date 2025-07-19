/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.util.concurrent.TimeUnit;

public interface Pool<T> {
    T acquire() throws Exception;

    T tryAcquire(long timeout, TimeUnit unit) throws InterruptedException;

    void release(T pooled);

    long totalMemory();

    long availableMemory();

    int pooledSegmentSize();

    void warmUp(int numBlocks);

    void close();
}

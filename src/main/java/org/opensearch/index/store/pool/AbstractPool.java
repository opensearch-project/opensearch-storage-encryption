/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Base class for memory pools with shared configuration and stats logic.
 *
 * @param <T> the type of pooled resource
 * @opensearch.internal
 */
public abstract class AbstractPool<T> implements Pool<T>, AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(AbstractPool.class);

    protected final int segmentSize;
    protected final int maxSegments;
    protected final long totalMemory;
    protected volatile boolean closed = false;

    protected AbstractPool(long totalMemory, int segmentSize) {
        if (totalMemory % segmentSize != 0) {
            throw new IllegalArgumentException("Total memory must be a multiple of segment size");
        }
        this.totalMemory = totalMemory;
        this.segmentSize = segmentSize;
        this.maxSegments = (int) (totalMemory / segmentSize);
    }

    @Override
    public long totalMemory() {
        return totalMemory;
    }

    @Override
    public int pooledSegmentSize() {
        return segmentSize;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    public int getMaxSegments() {
        return maxSegments;
    }

    protected void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Pool is closed");
        }
    }
}

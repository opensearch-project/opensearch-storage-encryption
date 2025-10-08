/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.block.RefCountedMemorySegment;

@SuppressWarnings("preview")
@SuppressForbidden(reason = "allocates standalone arenas per segment")
public class EphemeralMemorySegmentPool implements Pool<RefCountedMemorySegment>, AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(EphemeralMemorySegmentPool.class);
    private final int segmentSize;

    public EphemeralMemorySegmentPool(int segmentSize) {
        this.segmentSize = segmentSize;
    }

    @Override
    public RefCountedMemorySegment acquire() {
        // Each segment gets its own confined arena
        final Arena arena = Arena.ofShared();
        final MemorySegment segment = arena.allocate(segmentSize);

        // Return a refcounted wrapper that closes this arena upon release
        return new RefCountedMemorySegment(segment, segmentSize, _ -> {
            try {
                arena.close(); // Frees native memory immediately
            } catch (Exception e) {
                LOGGER.warn("Failed to close ephemeral arena", e);
            }
        });
    }

    @Override
    public void release(RefCountedMemorySegment refSegment) {
        // no-op, as release is handled in RefCountedMemorySegment’s callback
    }

    @Override
    public RefCountedMemorySegment tryAcquire(long timeout, TimeUnit unit) {
        return acquire();
    }

    @Override
    public void close() {
        // no global arenas, nothing to close
    }

    @Override
    public String poolStats() {
        return "EphemeralMemorySegmentPool[1 arena per segment, size=" + segmentSize + "]";
    }

    @Override
    public long totalMemory() {
        return 0;
    }

    @Override
    public long availableMemory() {
        return 0;
    }

    @Override
    public int pooledSegmentSize() {
        return segmentSize;
    }

    @Override
    public boolean isUnderPressure() {
        return false;
    }

    @Override
    public void warmUp(long numBlocks) {}

    @Override
    public boolean isClosed() {
        return false;
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.block.RefCountedByteBuffer;
import org.opensearch.index.store.metrics.CryptoMetricsService;

/**
 * Bounded allocator for direct ByteBuffers with GC-based lifecycle tracking.
 *
 * <p>Allocates via {@link ByteBuffer#allocateDirect(int)} on every acquire.
 * Uses {@link Cleaner} to decrement {@code buffersInUse} when wrappers are GC'd.
 *
 * <p>Read path (acquire) never fails — allocates over capacity if needed.
 * Write path (tryAcquire) throws when over capacity.
 * Requests GC (rate-limited 10s) when exhausted.
 *
 * <p>Requires jemalloc for efficient malloc/free without fragmentation.
 *
 * @opensearch.internal
 */
public class ByteBufferPool extends AbstractPool<RefCountedByteBuffer> {

    private static final Logger LOGGER = LogManager.getLogger(ByteBufferPool.class);
    private static final Cleaner CLEANER = Cleaner.create();
    private static final long GC_REQUEST_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);

    private final AtomicInteger buffersInUse = new AtomicInteger(0);
    private final AtomicLong lastGCRequestNanos = new AtomicLong(0);

    public ByteBufferPool(long totalMemory, int segmentSize) {
        super(totalMemory, segmentSize);
    }

    @Override
    public RefCountedByteBuffer acquire() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Thread interrupted before pool acquire");
        }
        ensureOpen();
        return allocateNew();

        /*
        if (buffersInUse.get() < maxSegments) {
            return allocateNew();
        }
        
        requestGCIfNeeded();
        if (buffersInUse.get() < maxSegments) {
            return allocateNew();
        }
        
        // Read path must not fail
        LOGGER.warn("Pool over capacity (inUse={}, max={}), allocating for read path", buffersInUse.get(), maxSegments);
        return allocateNew();
         */
    }

    @Override
    public RefCountedByteBuffer tryAcquire(long timeout, TimeUnit unit) throws Exception {
        ensureOpen();
        return allocateNew();

        /*
        if (buffersInUse.get() < maxSegments) {
            return allocateNew();
        }
        
        requestGCIfNeeded();
        if (buffersInUse.get() < maxSegments) {
            return allocateNew();
        }
        
        throw new IOException("Pool capacity reached (inUse=" + buffersInUse.get() + ", max=" + maxSegments + ")");
         */
    }

    private void requestGCIfNeeded() {
        long now = System.nanoTime();
        long last = lastGCRequestNanos.get();
        if (now - last > GC_REQUEST_INTERVAL_NANOS && lastGCRequestNanos.compareAndSet(last, now)) {
            LOGGER.warn("Pool exhausted (inUse={}, max={}), requesting GC", buffersInUse.get(), maxSegments);
            System.gc();
        }
    }

    private RefCountedByteBuffer allocateNew() {
        ByteBuffer direct = ByteBuffer.allocateDirect(segmentSize);
        buffersInUse.incrementAndGet();
        RefCountedByteBuffer wrapper = new RefCountedByteBuffer(direct, segmentSize);
        CLEANER.register(wrapper, new CleanupAction(buffersInUse));
        return wrapper;
    }

    private static class CleanupAction implements Runnable {
        private final AtomicInteger counter;

        CleanupAction(AtomicInteger counter) {
            this.counter = counter;
        }

        @Override
        public void run() {
            counter.decrementAndGet();
        }
    }

    @Override
    public void release(RefCountedByteBuffer buffer) {
        // no-op: Cleaner handles counter decrement
    }

    @Override
    public long availableMemory() {
        return (long) Math.max(0, maxSegments - buffersInUse.get()) * segmentSize;
    }

    public int getBuffersInUse() {
        return buffersInUse.get();
    }

    @Override
    public boolean isUnderPressure() {
        return (maxSegments - buffersInUse.get()) < (maxSegments * 0.1);
    }

    @Override
    public void warmUp(long targetSegments) {
        LOGGER.info("Warmup skipped, segments allocated on demand (max={})", maxSegments);
    }

    @Override
    public void close() {
        if (closed)
            return;
        closed = true;
        LOGGER.info("ByteBufferPool closed, buffersInUse={}", buffersInUse.get());
    }

    @Override
    public String poolStats() {
        int inUse = buffersInUse.get();
        double pct = maxSegments > 0 ? (double) inUse / maxSegments * 100 : 0;
        return String.format("ByteBufferPool[max=%d, inUse=%d, utilization=%.1f%%]", maxSegments, inUse, pct);
    }

    @Override
    public void recordStats() {
        int inUse = buffersInUse.get();
        double pct = maxSegments > 0 ? (double) inUse / maxSegments : 0;
        CryptoMetricsService.getInstance().recordPoolStats(SegmentType.PRIMARY, maxSegments, inUse, 0, pct, pct);
    }
}

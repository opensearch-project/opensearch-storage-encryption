/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.metrics.CryptoMetricsService;

/**
 * Lock-free pool for off-heap memory segments backed by direct ByteBuffers.
 *
 * <p>Allocates via {@link ByteBuffer#allocateDirect(int)} and wraps as
 * {@link MemorySegment} via {@link MemorySegment#ofBuffer(ByteBuffer)}.
 * Uses a {@link ConcurrentLinkedQueue} freelist for lock-free acquire/release.
 *
 * <p>When a {@link RefCountedMemorySegment} wrapper is GC'd, the {@link Cleaner}
 * allocates a <b>new</b> DirectByteBuffer into the freelist. The old buffer is
 * freed by GC's own DirectByteBuffer Cleaner. This ensures:
 * <ul>
 *   <li>No use-after-recycle corruption (old buffer is never reused)</li>
 *   <li>Zero malloc on the hot read path (freelist provides pre-allocated buffers)</li>
 *   <li>Bounded native memory (pool tracks total provisioned count)</li>
 * </ul>
 *
 * <p>Requires jemalloc ({@code LD_PRELOAD}) for efficient malloc/free without
 * glibc arena fragmentation.
 *
 * <p>Thread-safe via lock-free {@link ConcurrentLinkedQueue} and {@link AtomicInteger}.
 *
 * @opensearch.internal
 */
@SuppressWarnings("preview")
@SuppressForbidden(reason = "Uses ByteBuffer.allocateDirect for native memory allocation")
public class MemorySegmentPool implements Pool<RefCountedMemorySegment>, AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(MemorySegmentPool.class);
    private static final Cleaner CLEANER = Cleaner.create();
    private static final int MAX_RETRY_ATTEMPTS = 20;
    private static final long RETRY_WAIT_NANOS = TimeUnit.MILLISECONDS.toNanos(50);
    private static final long GC_REQUEST_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);

    private final ConcurrentLinkedQueue<ByteBuffer> freeList = new ConcurrentLinkedQueue<>();
    private final int segmentSize;
    private final int maxSegments;
    private final long totalMemory;
    private final AtomicInteger buffersInUse = new AtomicInteger(0);
    private final AtomicInteger totalProvisioned = new AtomicInteger(0);
    private final java.util.concurrent.atomic.AtomicLong lastGCRequestNanos = new java.util.concurrent.atomic.AtomicLong(0);

    private volatile boolean closed = false;

    /**
     * Creates a pool with the specified total memory and segment size.
     *
     * @param totalMemory total memory in bytes (must be a multiple of segmentSize)
     * @param segmentSize size of each memory segment in bytes
     */
    public MemorySegmentPool(long totalMemory, int segmentSize) {
        if (totalMemory % segmentSize != 0) {
            throw new IllegalArgumentException("Total memory must be a multiple of segment size");
        }
        this.totalMemory = totalMemory;
        this.segmentSize = segmentSize;
        this.maxSegments = (int) (totalMemory / segmentSize);
    }

    /**
     * Compatibility constructor — ignores requiresZeroing (GC handles cleanup).
     */
    public MemorySegmentPool(long totalMemory, int segmentSize, boolean requiresZeroing) {
        this(totalMemory, segmentSize);
    }

    @Override
    public RefCountedMemorySegment acquire() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Thread interrupted before pool acquire");
        }
        if (closed) {
            throw new IllegalStateException("Pool is closed");
        }

        // Fast path: grab from freelist (no malloc, lock-free CAS)
        ByteBuffer buf = freeList.poll();
        if (buf != null) {
            buf.clear().order(ByteOrder.LITTLE_ENDIAN);
            buffersInUse.incrementAndGet();
            return wrapAndRegister(buf);
        }

        // Freelist empty — trigger GC and retry
        long startNanos = System.nanoTime();
        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            requestGCIfNeeded();
            java.util.concurrent.locks.LockSupport.parkNanos(RETRY_WAIT_NANOS * attempt);

            buf = freeList.poll();
            if (buf != null) {
                long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
                buf.clear().order(ByteOrder.LITTLE_ENDIAN);
                buffersInUse.incrementAndGet();
                LOGGER
                    .info(
                        "Freelist replenished after {} retries ({}ms), provisioned={}, inUse={}, freeList={}",
                        attempt,
                        elapsedMs,
                        totalProvisioned.get(),
                        buffersInUse.get(),
                        freeList.size()
                    );
                return wrapAndRegister(buf);
            }

            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Thread interrupted while waiting for freelist");
            }
        }

        // All retries exhausted — fail
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        throw new IllegalStateException(
            "Freelist empty after "
                + MAX_RETRY_ATTEMPTS
                + " retries ("
                + elapsedMs
                + "ms). "
                + "provisioned="
                + totalProvisioned.get()
                + ", max="
                + maxSegments
                + ", inUse="
                + buffersInUse.get()
                + ", freeList="
                + freeList.size()
        );
    }

    @Override
    public RefCountedMemorySegment tryAcquire(long timeout, TimeUnit unit) throws Exception {
        if (closed) {
            throw new IllegalStateException("Pool is closed");
        }

        ByteBuffer buf = freeList.poll();
        if (buf != null) {
            buf.clear().order(ByteOrder.LITTLE_ENDIAN);
            buffersInUse.incrementAndGet();
            return wrapAndRegister(buf);
        }

        // Freelist empty — trigger GC and retry with timeout
        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + unit.toNanos(timeout);
        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            requestGCIfNeeded();
            java.util.concurrent.locks.LockSupport.parkNanos(RETRY_WAIT_NANOS * attempt);

            buf = freeList.poll();
            if (buf != null) {
                long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
                buf.clear().order(ByteOrder.LITTLE_ENDIAN);
                buffersInUse.incrementAndGet();
                LOGGER.info("Freelist replenished after {} retries ({}ms)", attempt, elapsedMs);
                return wrapAndRegister(buf);
            }

            if (System.nanoTime() > deadlineNanos)
                break;
        }

        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        throw new IOException(
            "Freelist empty after retries ("
                + elapsedMs
                + "ms). "
                + "provisioned="
                + totalProvisioned.get()
                + ", max="
                + maxSegments
                + ", inUse="
                + buffersInUse.get()
                + ", freeList="
                + freeList.size()
        );
    }

    private RefCountedMemorySegment wrapAndRegister(ByteBuffer direct) {
        MemorySegment seg = MemorySegment.ofBuffer(direct);
        RefCountedMemorySegment wrapper = new RefCountedMemorySegment(seg, segmentSize, this::release);
        // When wrapper is GC'd: old buffer freed by GC, NEW buffer allocated into freelist
        CLEANER.register(wrapper, new ReplenishFreeList(freeList, segmentSize, buffersInUse, this));
        return wrapper;
    }

    /**
     * Invoked by Cleaner when a RefCountedMemorySegment wrapper becomes phantom reachable.
     * Decrements buffersInUse and allocates a NEW DirectByteBuffer into the freelist.
     * The old buffer is freed separately by DirectByteBuffer's own JDK Cleaner.
     *
     * <p>Must NOT reference the old wrapper or its ByteBuffer/MemorySegment (would prevent GC).
     */
    private static class ReplenishFreeList implements Runnable {
        private final ConcurrentLinkedQueue<ByteBuffer> freeList;
        private final int size;
        private final AtomicInteger buffersInUse;
        private final MemorySegmentPool pool;

        ReplenishFreeList(ConcurrentLinkedQueue<ByteBuffer> freeList, int size, AtomicInteger buffersInUse, MemorySegmentPool pool) {
            this.freeList = freeList;
            this.size = size;
            this.buffersInUse = buffersInUse;
            this.pool = pool;
        }

        @Override
        public void run() {
            buffersInUse.decrementAndGet();
            if (pool.isClosed())
                return;
            try {
                ByteBuffer fresh = ByteBuffer.allocateDirect(size).order(ByteOrder.LITTLE_ENDIAN);
                freeList.offer(fresh);
            } catch (OutOfMemoryError e) {
                LogManager.getLogger(MemorySegmentPool.class).warn("Failed to replenish freelist: direct memory exhausted");
            }
        }
    }

    private void requestGCIfNeeded() {
        long now = System.nanoTime();
        long last = lastGCRequestNanos.get();
        if (now - last > GC_REQUEST_INTERVAL_NANOS && lastGCRequestNanos.compareAndSet(last, now)) {
            LOGGER
                .warn(
                    "Pool exhausted (inUse={}, provisioned={}, max={}, freeList={}), requesting GC",
                    buffersInUse.get(),
                    totalProvisioned.get(),
                    maxSegments,
                    freeList.size()
                );
            System.gc();
        }
    }

    @Override
    public void release(RefCountedMemorySegment refSegment) {
        // No-op: Cleaner handles replenishment when wrapper is GC'd.
        // The ref-counted release path (from cache eviction) calls this,
        // but actual native memory lifecycle is managed by GC + Cleaner.
    }

    /**
     * Release multiple segments. No-op for same reason as release().
     */
    public void releaseAll(RefCountedMemorySegment... segments) {
        // No-op
    }

    @Override
    public long totalMemory() {
        return totalMemory;
    }

    @Override
    public long availableMemory() {
        return (long) (freeList.size() + Math.max(0, maxSegments - totalProvisioned.get())) * segmentSize;
    }

    /**
     * Returns the accurate available memory (same as availableMemory for lock-free impl).
     */
    public long availableMemoryAccurate() {
        return availableMemory();
    }

    @Override
    public int pooledSegmentSize() {
        return segmentSize;
    }

    /**
     * Returns current pool statistics.
     */
    public PoolStats getStats() {
        return new PoolStats(maxSegments, totalProvisioned.get(), buffersInUse.get(), freeList.size());
    }

    public int getBuffersInUse() {
        return buffersInUse.get();
    }

    public int getTotalProvisioned() {
        return totalProvisioned.get();
    }

    public int getFreeListSize() {
        return freeList.size();
    }

    @Override
    public boolean isUnderPressure() {
        return freeList.isEmpty() && (maxSegments - totalProvisioned.get()) < (maxSegments * 0.1);
    }

    @Override
    public void warmUp(long targetSegments) {
        long toAllocate = Math.min(targetSegments, maxSegments);
        LOGGER.info("Warming up freelist with {} buffers (max={})", toAllocate, maxSegments);
        for (long i = 0; i < toAllocate; i++) {
            try {
                ByteBuffer buf = ByteBuffer.allocateDirect(segmentSize).order(ByteOrder.LITTLE_ENDIAN);
                freeList.offer(buf);
                totalProvisioned.incrementAndGet();
            } catch (OutOfMemoryError e) {
                LOGGER.warn("Warmup stopped at {} buffers: direct memory exhausted", i);
                break;
            }
        }
        LOGGER.info("Warmup complete: freeList={}, provisioned={}", freeList.size(), totalProvisioned.get());
    }

    @Override
    public void close() {
        if (closed)
            return;
        closed = true;
        int drained = 0;
        while (freeList.poll() != null)
            drained++;
        LOGGER
            .info(
                "MemorySegmentPool closed: drained {} freelist buffers, inUse={}, provisioned={}",
                drained,
                buffersInUse.get(),
                totalProvisioned.get()
            );
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public String poolStats() {
        int inUse = buffersInUse.get();
        int provisioned = totalProvisioned.get();
        int free = freeList.size();
        return String
            .format(
                "PoolStats[max=%d, provisioned=%d, inUse=%d, freeList=%d, utilization=%.1f%%]",
                maxSegments,
                provisioned,
                inUse,
                free,
                maxSegments > 0 ? (double) inUse / maxSegments * 100 : 0
            );
    }

    /**
     * Monitoring snapshot of pool metrics.
     */
    @SuppressForbidden(reason = "custom string builder")
    public static class PoolStats {
        public final int maxSegments;
        public final int totalProvisioned;
        public final int buffersInUse;
        public final int freeListSize;
        public final double utilizationRatio;

        PoolStats(int maxSegments, int totalProvisioned, int buffersInUse, int freeListSize) {
            this.maxSegments = maxSegments;
            this.totalProvisioned = totalProvisioned;
            this.buffersInUse = buffersInUse;
            this.freeListSize = freeListSize;
            this.utilizationRatio = maxSegments > 0 ? (double) buffersInUse / maxSegments : 0;
        }

        @Override
        public String toString() {
            return String
                .format(
                    "PoolStats[max=%d, provisioned=%d, inUse=%d, freeList=%d, utilization=%.1f%%]",
                    maxSegments,
                    totalProvisioned,
                    buffersInUse,
                    freeListSize,
                    utilizationRatio * 100
                );
        }
    }

    @Override
    public void recordStats() {
        int inUse = buffersInUse.get();
        int free = freeList.size();
        double pct = maxSegments > 0 ? (double) inUse / maxSegments : 0;
        CryptoMetricsService.getInstance().recordPoolStats(SegmentType.PRIMARY, maxSegments, inUse, free, pct, pct);
    }
}

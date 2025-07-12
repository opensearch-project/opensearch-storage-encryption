/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings("preview")
public class TieredDirectIOBufferPool {
    private static final Logger LOGGER = LogManager.getLogger(TieredDirectIOBufferPool.class);

    private static final Arena PROCESS_ARENA = Arena.ofShared();

    private static final Map<Long, Integer> BUFFER_CLASSES = Map
        .of(512L * 1024, 128, 1L * 1024 * 1024, 64, 2L * 1024 * 1024, 32, 4L * 1024 * 1024, 32, 8L * 1024 * 1024, 32);

    private final Map<Long, BufferClassPool> pools = new ConcurrentHashMap<>();

    public TieredDirectIOBufferPool() {

        for (Map.Entry<Long, Integer> entry : BUFFER_CLASSES.entrySet()) {
            long size = entry.getKey();
            int maxBuffers = entry.getValue();
            pools.put(size, new BufferClassPool(size, maxBuffers));
        }

        startStatsLogger();
    }

    /**
     * Acquire a buffer of at least `minSize` (rounded up to nearest class).
     */

    public MemorySegment acquire(long requestedSize) {
        long sizeClass = getNearestBufferClass(requestedSize);
        BufferClassPool pool = pools.get(sizeClass);
        if (pool == null) {
            throw new IllegalStateException("No buffer class pool for size: " + sizeClass);
        }
        return pool.acquire();
    }

    /**
     * Release a buffer back to the appropriate class pool.
     */
    public void release(MemorySegment buffer) {
        long size = buffer.byteSize();
        BufferClassPool pool = pools.get(size);
        if (pool == null) {
            LOGGER.warn("Dropping buffer of unexpected size: {}", size);
            return;
        }
        pool.release(buffer);
    }

    /**
     * Finds the nearest buffer class >= requested size.
     */
    private long getNearestBufferClass(long size) {
        return BUFFER_CLASSES
            .keySet()
            .stream()
            .sorted()
            .filter(klass -> klass >= size)
            .findFirst()
            .orElseGet(() -> BUFFER_CLASSES.keySet().stream().max(Long::compare).orElseThrow());
    }

    /**
     * Internal buffer class pool (per size tier).
     */
    private static class BufferClassPool {
        private final long bufferSize;
        private final int maxBuffers;

        private static final long MAX_TOTAL_BYTES = 4L * 1024 * 1024 * 1024; // 4 GiB
        private final AtomicLong totalBytesAllocated = new AtomicLong(0);

        private final ConcurrentLinkedQueue<MemorySegment> queue = new ConcurrentLinkedQueue<>();
        private final AtomicInteger allocated = new AtomicInteger(0);
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition notEmpty = lock.newCondition();

        BufferClassPool(long bufferSize, int maxBuffers) {
            this.bufferSize = bufferSize;
            this.maxBuffers = maxBuffers;
        }

        MemorySegment acquire() {
            MemorySegment buffer = queue.poll();
            if (buffer != null)
                return buffer;

            lock.lock();
            try {
                buffer = queue.poll();
                if (buffer != null)
                    return buffer;

                long currentTotal = totalBytesAllocated.get();
                if (allocated.get() < maxBuffers && currentTotal + bufferSize <= MAX_TOTAL_BYTES) {
                    MemorySegment seg = PROCESS_ARENA.allocate(bufferSize, DirectIoConstants.DIRECT_IO_ALIGNMENT);
                    allocated.incrementAndGet();
                    totalBytesAllocated.addAndGet(bufferSize);
                    LOGGER.debug("Allocated {}B buffer (allocated={}B)", bufferSize, totalBytesAllocated.get());
                    return seg;
                }

                while (queue.isEmpty()) {
                    try {
                        notEmpty.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while waiting for buffer", e);
                    }
                }

                return queue.poll();
            } finally {
                lock.unlock();
            }
        }

        void release(MemorySegment buffer) {
            queue.offer(buffer);
            lock.lock();
            try {
                notEmpty.signal();
            } finally {
                lock.unlock();
            }
        }
    }

    private void startStatsLogger() {
        Thread loggerThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(30_000); // 30 seconds

                    long totalMem = 0;
                    StringBuilder sb = new StringBuilder("DirectIO Buffer Pool Stats:\n");

                    for (Map.Entry<Long, BufferClassPool> entry : pools.entrySet()) {
                        long sizeClass = entry.getKey();
                        BufferClassPool pool = entry.getValue();

                        int allocated = pool.allocated.get();
                        int free = pool.queue.size();
                        long tierTotal = (long) allocated * sizeClass;

                        totalMem += tierTotal;

                        sb
                            .append(
                                String
                                    .format(
                                        "  [%6s KB] Allocated: %3d  | Free: %3d | Used: %3d\n",
                                        sizeClass / 1024,
                                        allocated,
                                        free,
                                        allocated - free
                                    )
                            );
                    }

                    sb.append(String.format("  Total Allocated Memory: %.2f MB\n", totalMem / 1048576.0));
                    LOGGER.info(sb.toString());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Throwable t) {
                    LOGGER.warn("Error in buffer pool stats logger", t);
                }
            }
        });

        loggerThread.setDaemon(true);
        loggerThread.setName("DirectIOBufferPoolStatsLogger");
        loggerThread.start();
    }
}

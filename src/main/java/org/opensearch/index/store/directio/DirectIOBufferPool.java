/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A thread-safe buffer pool for DirectIO operations that manages reusable aligned memory buffers.
 * <p>
 * This pool uses a single process-wide Arena to allocate aligned memory buffers that can be reused
 * across multiple DirectIO operations. The pool has a configurable maximum number of buffers to
 * prevent unbounded memory growth.
 * </p>
 * <p>
 * Memory allocated by this pool is never freed until the process terminates, trading memory 
 * flexibility for performance and predictable memory usage.
 * </p>
 */
@SuppressWarnings("preview")
public class DirectIOBufferPool {
    private static final Logger LOGGER = LogManager.getLogger(DirectIOBufferPool.class);

    private static final Arena PROCESS_ARENA = Arena.ofShared();
    private static final int BUFFER_SIZE = 1 << DirectIoUtils.MAX_CHUNK_SIZE; // 16MB
    private static final int DEFAULT_MAX_BUFFERS = 64; // ~1GB total

    private final ConcurrentLinkedQueue<MemorySegment> freeBuffers = new ConcurrentLinkedQueue<>();
    private final AtomicInteger allocatedCount = new AtomicInteger(0);
    private final AtomicInteger maxBuffers = new AtomicInteger(DEFAULT_MAX_BUFFERS);

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition bufferAvailable = lock.newCondition();

    /**
     * Acquires a buffer from the pool. Blocks until a buffer becomes available.
     * 
     * @return A reusable MemorySegment buffer
     * @throws DirectIOBufferException if buffer cannot be acquired
     */
    public MemorySegment acquireBuffer(int lenght) {
        // Fast path: try to get a free buffer without locking
        MemorySegment buffer = freeBuffers.poll();
        if (buffer != null) {
            return buffer;
        }

        lock.lock();
        try {
            // Double-check after acquiring lock to avoid race condition
            buffer = freeBuffers.poll();
            if (buffer != null) {
                return buffer;
            }

            // Try to allocate a new buffer if under limit
            if (allocatedCount.get() < maxBuffers.get()) {
                try {
                    MemorySegment newBuffer = PROCESS_ARENA.allocate(BUFFER_SIZE, DirectIoUtils.DIRECT_IO_ALIGNMENT);
                    int currentCount = allocatedCount.incrementAndGet();
                    LOGGER
                        .debug(
                            "Allocated new DirectIO buffer. Total buffers: {}/{} ({}MB)",
                            currentCount,
                            maxBuffers.get(),
                            (currentCount * BUFFER_SIZE) / (1024 * 1024)
                        );
                    return newBuffer;
                } catch (Exception e) {
                    throw new DirectIOBufferException("Failed to allocate DirectIO buffer", e);
                }
            }

            // Wait for a buffer to be released (proper producer-consumer pattern)
            while (freeBuffers.isEmpty() && allocatedCount.get() >= maxBuffers.get()) {
                try {
                    bufferAvailable.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new DirectIOBufferException("Interrupted while waiting for DirectIO buffer", e);
                }
            }

            buffer = freeBuffers.poll();
            if (buffer != null) {
                return buffer;
            }

            if (allocatedCount.get() < maxBuffers.get()) {
                try {
                    MemorySegment newBuffer = PROCESS_ARENA.allocate(BUFFER_SIZE, DirectIoUtils.DIRECT_IO_ALIGNMENT);
                    int currentCount = allocatedCount.incrementAndGet();
                    LOGGER
                        .debug(
                            "Allocated new DirectIO buffer after wait. Total buffers: {}/{} ({}MB)",
                            currentCount,
                            maxBuffers.get(),
                            (currentCount * BUFFER_SIZE) / (1024 * 1024)
                        );
                    return newBuffer;
                } catch (Exception e) {
                    throw new DirectIOBufferException("Failed to allocate DirectIO buffer after wait", e);
                }
            }

            throw new DirectIOBufferException("Unexpected state: unable to acquire buffer");

        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns a buffer to the pool for reuse.
     * 
     * @param buffer The buffer to return to the pool
     * @throws IllegalArgumentException if buffer is null
     */
    public void releaseBuffer(MemorySegment buffer) {
        if (buffer == null) {
            throw new IllegalArgumentException("Buffer cannot be null");
        }

        freeBuffers.offer(buffer);

        // Signal waiting threads that a buffer is available
        lock.lock();
        try {
            bufferAvailable.signal(); // Wake up one waiting thread
        } finally {
            lock.unlock();
        }
    }

    /**
     * Gets the size of each buffer in bytes.
     * 
     * @return Buffer size in bytes
     */
    public static int getBufferSize() {
        return BUFFER_SIZE;
    }

    /**
     * Gets the total number of allocated buffers.
     * 
     * @return Number of allocated buffers
     */
    public int getAllocatedCount() {
        return allocatedCount.get();
    }

    /**
     * Gets the number of free buffers currently in the pool.
     * 
     * @return Number of free buffers
     */
    public int getFreeBufferCount() {
        return freeBuffers.size();
    }

    /**
     * Gets the maximum number of buffers allowed.
     * 
     * @return Maximum buffer count
     */
    public int getMaxBuffers() {
        return maxBuffers.get();
    }

    /**
     * Sets the maximum number of buffers allowed.
     * 
     * @param maxBuffers Maximum buffer count (must be positive)
     * @throws IllegalArgumentException if maxBuffers is not positive
     */
    public void setMaxBuffers(int maxBuffers) {
        if (maxBuffers <= 0) {
            throw new IllegalArgumentException("maxBuffers must be positive");
        }
        this.maxBuffers.set(maxBuffers);
        LOGGER.info("DirectIO buffer pool max buffers set to: {} ({}MB max)", maxBuffers, (maxBuffers * BUFFER_SIZE) / (1024 * 1024));
    }

    /**
     * Gets the number of buffers currently in use.
     * 
     * @return Number of buffers in use
     */
    public int getInUseCount() {
        return allocatedCount.get() - freeBuffers.size();
    }

    /**
     * Gets the total memory allocated by this pool in bytes.
     * 
     * @return Total allocated memory in bytes
     */
    public long getTotalAllocatedMemory() {
        return (long) allocatedCount.get() * BUFFER_SIZE;
    }

    /**
     * Exception thrown when buffer pool operations fail.
     */
    public static class DirectIOBufferException extends RuntimeException {
        public DirectIOBufferException(String message) {
            super(message);
        }

        public DirectIOBufferException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

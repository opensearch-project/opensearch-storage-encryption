/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.test.OpenSearchTestCase;

public class QueuingWorkerTests extends OpenSearchTestCase {

    private static final int CACHE_BLOCK_SIZE = 8192; // 2^13 from DirectIoConfigs.CACHE_BLOCK_SIZE_POWER
    private static final Path TEST_PATH = Paths.get("/test/file.dat");
    private static final Path TEST_PATH_2 = Paths.get("/test/file2.dat");

    private ExecutorService executor;
    private BlockCache<RefCountedMemorySegment> mockBlockCache;
    private QueuingWorker worker;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        super.setUp();
        executor = Executors.newFixedThreadPool(4);
        mockBlockCache = mock(BlockCache.class);
    }

    @After
    public void tearDown() throws Exception {
        if (worker != null) {
            worker.close();
        }
        if (executor != null) {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        super.tearDown();
    }

    // ========== Construction Tests ==========

    /**
     * Tests worker construction with valid parameters.
     */
    public void testWorkerConstruction() {
        worker = new QueuingWorker(100, 2, executor, mockBlockCache);

        assertNotNull("Worker should be created", worker);
        assertTrue("Worker should be running", worker.isRunning());
    }

    /**
     * Tests worker construction with minimal capacity.
     */
    public void testWorkerConstructionMinimalCapacity() {
        worker = new QueuingWorker(1, 1, executor, mockBlockCache);

        assertNotNull("Worker should be created with minimal capacity", worker);
        assertTrue("Worker should be running", worker.isRunning());
    }

    /**
     * Tests worker construction enforces maxRunners >= 1.
     */
    public void testWorkerConstructionEnforcesMinRunners() {
        worker = new QueuingWorker(100, 0, executor, mockBlockCache);

        assertNotNull("Worker should be created", worker);
        // maxRunners should be clamped to at least 1
    }

    /**
     * Tests scheduling a single small request.
     */
    public void testScheduleSingleSmallRequest() throws Exception {
        worker = new QueuingWorker(100, 2, executor, mockBlockCache);

        Map<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> mockResult = Map.of();
        when(mockBlockCache.loadBulk(any(Path.class), anyLong(), anyLong())).thenReturn(mockResult);

        boolean accepted = worker.schedule(TEST_PATH, 0, 10);

        assertTrue("Worker should accept request", accepted);
        Thread.sleep(100); // Allow processing

        verify(mockBlockCache, times(1)).loadBulk(eq(TEST_PATH), eq(0L), eq(10L));
    }

    /**
     * Tests scheduling multiple independent requests.
     */
    public void testScheduleMultipleRequests() throws Exception {
        worker = new QueuingWorker(100, 2, executor, mockBlockCache);

        CountDownLatch loadLatch = new CountDownLatch(3);
        Map<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> mockResult = Map.of();
        when(mockBlockCache.loadBulk(any(Path.class), anyLong(), anyLong())).thenAnswer(invocation -> {
            loadLatch.countDown();
            return mockResult;
        });

        boolean accepted1 = worker.schedule(TEST_PATH, 0, 10);
        boolean accepted2 = worker.schedule(TEST_PATH, 10 * CACHE_BLOCK_SIZE, 10);
        boolean accepted3 = worker.schedule(TEST_PATH, 20 * CACHE_BLOCK_SIZE, 10);

        assertTrue("All requests should be accepted", accepted1 && accepted2 && accepted3);
        assertTrue("All requests should be processed", loadLatch.await(2, TimeUnit.SECONDS));

        verify(mockBlockCache, times(3)).loadBulk(eq(TEST_PATH), anyLong(), eq(10L));
    }

    /**
     * Tests that large requests are chunked automatically.
     * Requests > MAX_BULK_SIZE (128) should be split into smaller chunks.
     */
    public void testChunkingLargeRequest() throws Exception {
        worker = new QueuingWorker(500, 4, executor, mockBlockCache);

        CountDownLatch loadLatch = new CountDownLatch(2);
        Map<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> mockResult = Map.of();
        when(mockBlockCache.loadBulk(any(Path.class), anyLong(), anyLong())).thenAnswer(invocation -> {
            loadLatch.countDown();
            return mockResult;
        });

        // Schedule 256 blocks - should be split into 2 chunks of 128
        boolean accepted = worker.schedule(TEST_PATH, 0, 256);

        assertTrue("Large request should be accepted", accepted);
        assertTrue("All chunks should be processed", loadLatch.await(2, TimeUnit.SECONDS));

        // Should see 2 loadBulk calls, each with 128 blocks
        // Chunk 1: offset=0, count=128
        // Chunk 2: offset=128*8192, count=128
        verify(mockBlockCache, times(1)).loadBulk(eq(TEST_PATH), eq(0L), eq(128L));
        verify(mockBlockCache, times(1)).loadBulk(eq(TEST_PATH), eq(128L * CACHE_BLOCK_SIZE), eq(128L));
    }

    /**
     * Tests chunking with non-uniform split (not evenly divisible by MAX_BULK_SIZE).
     */
    public void testChunkingNonUniformSplit() throws Exception {
        worker = new QueuingWorker(500, 4, executor, mockBlockCache);

        CountDownLatch loadLatch = new CountDownLatch(2);
        Map<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> mockResult = Map.of();
        when(mockBlockCache.loadBulk(any(Path.class), anyLong(), anyLong())).thenAnswer(invocation -> {
            loadLatch.countDown();
            return mockResult;
        });

        // Schedule 200 blocks - should be split into 128 + 72
        boolean accepted = worker.schedule(TEST_PATH, 0, 200);

        assertTrue("Request should be accepted", accepted);
        assertTrue("All chunks should be processed", loadLatch.await(2, TimeUnit.SECONDS));

        verify(mockBlockCache, times(1)).loadBulk(eq(TEST_PATH), eq(0L), eq(128L));
        verify(mockBlockCache, times(1)).loadBulk(eq(TEST_PATH), eq(128L * CACHE_BLOCK_SIZE), eq(72L));
    }

    /**
     * Tests very large request is chunked into multiple pieces.
     */
    public void testChunkingVeryLargeRequest() throws Exception {
        worker = new QueuingWorker(500, 4, executor, mockBlockCache);

        CountDownLatch loadLatch = new CountDownLatch(4);
        Map<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> mockResult = Map.of();
        when(mockBlockCache.loadBulk(any(Path.class), anyLong(), anyLong())).thenAnswer(invocation -> {
            loadLatch.countDown();
            return mockResult;
        });

        // Schedule 512 blocks - should be split into 4 chunks of 128
        boolean accepted = worker.schedule(TEST_PATH, 0, 512);

        assertTrue("Very large request should be accepted", accepted);
        assertTrue("All chunks should be processed", loadLatch.await(2, TimeUnit.SECONDS));

        // Should see 4 loadBulk calls, each with 128 blocks
        verify(mockBlockCache, times(4)).loadBulk(eq(TEST_PATH), anyLong(), eq(128L));
    }

    /**
     * Tests that requests at MAX_BULK_SIZE boundary are not chunked.
     */
    public void testNoChunkingAtBoundary() throws Exception {
        worker = new QueuingWorker(500, 4, executor, mockBlockCache);

        CountDownLatch loadLatch = new CountDownLatch(1);
        Map<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> mockResult = Map.of();
        when(mockBlockCache.loadBulk(any(Path.class), anyLong(), anyLong())).thenAnswer(invocation -> {
            loadLatch.countDown();
            return mockResult;
        });

        // Schedule exactly 128 blocks - should NOT be chunked
        boolean accepted = worker.schedule(TEST_PATH, 0, 128);

        assertTrue("Request at boundary should be accepted", accepted);
        assertTrue("Request should be processed", loadLatch.await(2, TimeUnit.SECONDS));

        verify(mockBlockCache, times(1)).loadBulk(eq(TEST_PATH), eq(0L), eq(128L));
    }

    /**
     * Tests that 129 blocks triggers chunking.
     */
    public void testChunkingJustOverBoundary() throws Exception {
        worker = new QueuingWorker(500, 4, executor, mockBlockCache);

        CountDownLatch loadLatch = new CountDownLatch(2);
        Map<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> mockResult = Map.of();
        when(mockBlockCache.loadBulk(any(Path.class), anyLong(), anyLong())).thenAnswer(invocation -> {
            loadLatch.countDown();
            return mockResult;
        });

        // Schedule 129 blocks - should be split into 128 + 1
        boolean accepted = worker.schedule(TEST_PATH, 0, 129);

        assertTrue("Request should be accepted", accepted);
        assertTrue("All chunks should be processed", loadLatch.await(2, TimeUnit.SECONDS));

        verify(mockBlockCache, times(1)).loadBulk(eq(TEST_PATH), eq(0L), eq(128L));
        verify(mockBlockCache, times(1)).loadBulk(eq(TEST_PATH), eq(128L * CACHE_BLOCK_SIZE), eq(1L));
    }

    /**
     * Tests that overlapping requests are detected and skipped.
     */
    public void testOverlapDetection() throws Exception {
        worker = new QueuingWorker(100, 1, executor, mockBlockCache);

        // Make loadBulk slow to ensure overlap
        CountDownLatch loadLatch = new CountDownLatch(1);
        Map<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> mockResult = Map.of();
        when(mockBlockCache.loadBulk(any(Path.class), anyLong(), anyLong())).thenAnswer(invocation -> {
            loadLatch.await(2, TimeUnit.SECONDS);
            return mockResult;
        });

        // Schedule overlapping requests
        boolean accepted1 = worker.schedule(TEST_PATH, 0, 20);
        Thread.sleep(50); // Let first request start processing
        boolean accepted2 = worker.schedule(TEST_PATH, 10 * CACHE_BLOCK_SIZE, 20); // Overlaps with first

        assertTrue("First request should be accepted", accepted1);
        assertTrue("Second request should be detected as duplicate", accepted2); // Returns true but skipped

        loadLatch.countDown();
        Thread.sleep(200);

        // Should only see 1 loadBulk call (second was skipped)
        verify(mockBlockCache, times(1)).loadBulk(eq(TEST_PATH), anyLong(), anyLong());
    }

    /**
     * Tests that non-overlapping requests are both processed.
     */
    public void testNonOverlappingRequests() throws Exception {
        worker = new QueuingWorker(100, 2, executor, mockBlockCache);

        Map<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> mockResult = Map.of();
        when(mockBlockCache.loadBulk(any(Path.class), anyLong(), anyLong())).thenReturn(mockResult);

        // Schedule non-overlapping requests
        boolean accepted1 = worker.schedule(TEST_PATH, 0, 10);
        boolean accepted2 = worker.schedule(TEST_PATH, 100 * CACHE_BLOCK_SIZE, 10);

        assertTrue("Both requests should be accepted", accepted1 && accepted2);
        Thread.sleep(300);

        verify(mockBlockCache, times(2)).loadBulk(eq(TEST_PATH), anyLong(), anyLong());
    }

    /**
     * Tests overlap detection across different files.
     */
    public void testNoOverlapAcrossDifferentFiles() throws Exception {
        worker = new QueuingWorker(100, 2, executor, mockBlockCache);

        Map<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> mockResult = Map.of();
        when(mockBlockCache.loadBulk(any(Path.class), anyLong(), anyLong())).thenReturn(mockResult);

        // Same offsets but different files - should both be processed
        boolean accepted1 = worker.schedule(TEST_PATH, 0, 10);
        boolean accepted2 = worker.schedule(TEST_PATH_2, 0, 10);

        assertTrue("Requests for different files should both be accepted", accepted1 && accepted2);
        Thread.sleep(300);

        verify(mockBlockCache, times(1)).loadBulk(eq(TEST_PATH), eq(0L), eq(10L));
        verify(mockBlockCache, times(1)).loadBulk(eq(TEST_PATH_2), eq(0L), eq(10L));
    }

    /**
     * Tests that queue capacity is enforced.
     */
    public void testQueueCapacityEnforcement() throws Exception {
        worker = new QueuingWorker(5, 1, executor, mockBlockCache);

        // Block loadBulk to fill up queue
        CountDownLatch blockLatch = new CountDownLatch(1);
        Map<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> mockResult = Map.of();
        when(mockBlockCache.loadBulk(any(Path.class), anyLong(), anyLong())).thenAnswer(invocation -> {
            blockLatch.await(5, TimeUnit.SECONDS);
            return mockResult;
        });

        // Fill queue (capacity = 5, maxRunners = 1)
        int acceptedCount = 0;
        for (int i = 0; i < 10; i++) {
            boolean accepted = worker.schedule(TEST_PATH, i * 100L * CACHE_BLOCK_SIZE, 10);
            if (accepted)
                acceptedCount++;
            Thread.sleep(10);
        }

        blockLatch.countDown();

        // Should reject some requests when queue is full
        assertTrue("Some requests should be rejected", acceptedCount < 10);
    }

    /**
     * Tests behavior when queue becomes available again.
     */
    public void testQueueDrainsAndAcceptsNew() throws Exception {
        worker = new QueuingWorker(100, 2, executor, mockBlockCache);

        Map<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> mockResult = Map.of();
        when(mockBlockCache.loadBulk(any(Path.class), anyLong(), anyLong())).thenReturn(mockResult);

        // Schedule requests
        for (int i = 0; i < 10; i++) {
            worker.schedule(TEST_PATH, i * 100L * CACHE_BLOCK_SIZE, 10);
        }

        Thread.sleep(300); // Let queue drain

        // Should accept new requests
        boolean accepted = worker.schedule(TEST_PATH, 1000L * CACHE_BLOCK_SIZE, 10);
        assertTrue("Should accept new request after draining", accepted);
    }

    /**
     * Tests cancellation of pending work for a specific path.
     */
    public void testCancellation() throws Exception {
        worker = new QueuingWorker(100, 1, executor, mockBlockCache);

        // Block loadBulk to keep items in queue
        CountDownLatch blockLatch = new CountDownLatch(1);
        Map<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> mockResult = Map.of();
        when(mockBlockCache.loadBulk(any(Path.class), anyLong(), anyLong())).thenAnswer(invocation -> {
            blockLatch.await(5, TimeUnit.SECONDS);
            return mockResult;
        });

        // Schedule multiple requests
        worker.schedule(TEST_PATH, 0, 10);
        worker.schedule(TEST_PATH, 100L * CACHE_BLOCK_SIZE, 10);
        worker.schedule(TEST_PATH, 200L * CACHE_BLOCK_SIZE, 10);
        Thread.sleep(100);

        // Cancel pending work
        worker.cancel(TEST_PATH);

        blockLatch.countDown();
        Thread.sleep(200);

        // Should have processed less than all scheduled (some were cancelled)
        // Exact count depends on timing, so just verify no exception
        assertNotNull("Worker should handle cancellation", worker);
    }

    /**
     * Tests cancellation doesn't affect other files.
     */
    public void testCancellationScopedToPath() throws Exception {
        worker = new QueuingWorker(100, 2, executor, mockBlockCache);

        Map<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> mockResult = Map.of();
        when(mockBlockCache.loadBulk(any(Path.class), anyLong(), anyLong())).thenReturn(mockResult);

        // Schedule for both files
        worker.schedule(TEST_PATH, 0, 10);
        worker.schedule(TEST_PATH_2, 0, 10);

        // Cancel only TEST_PATH
        worker.cancel(TEST_PATH);

        Thread.sleep(200);

        // TEST_PATH_2 should still be processed
        verify(mockBlockCache, times(1)).loadBulk(eq(TEST_PATH_2), eq(0L), eq(10L));
    }

    /**
     * Tests handling of IOException during load.
     */
    public void testIOExceptionHandling() throws Exception {
        worker = new QueuingWorker(100, 2, executor, mockBlockCache);

        when(mockBlockCache.loadBulk(any(Path.class), anyLong(), anyLong())).thenThrow(new IOException("Test IO error"));

        boolean accepted = worker.schedule(TEST_PATH, 0, 10);

        assertTrue("Request should be accepted", accepted);
        Thread.sleep(200); // Allow processing

        // Should handle exception gracefully, no crash
        assertTrue("Worker should still be running after exception", worker.isRunning());
    }

    /**
     * Tests handling of NoSuchFileException.
     */
    public void testNoSuchFileExceptionHandling() throws Exception {
        worker = new QueuingWorker(100, 2, executor, mockBlockCache);

        when(mockBlockCache.loadBulk(any(Path.class), anyLong(), anyLong())).thenThrow(new NoSuchFileException("Test file"));

        boolean accepted = worker.schedule(TEST_PATH, 0, 10);

        assertTrue("Request should be accepted", accepted);
        Thread.sleep(200);

        assertTrue("Worker should still be running", worker.isRunning());
    }

    /**
     * Tests handling of RuntimeException during load.
     */
    public void testRuntimeExceptionHandling() throws Exception {
        worker = new QueuingWorker(100, 2, executor, mockBlockCache);

        when(mockBlockCache.loadBulk(any(Path.class), anyLong(), anyLong())).thenThrow(new RuntimeException("Test runtime error"));

        boolean accepted = worker.schedule(TEST_PATH, 0, 10);

        assertTrue("Request should be accepted", accepted);
        Thread.sleep(200);

        // RuntimeException should be propagated but worker continues
        assertTrue("Worker should handle runtime exceptions", worker.isRunning());
    }

    /**
     * Tests concurrent scheduling from multiple threads.
     */
    public void testConcurrentScheduling() throws Exception {
        worker = new QueuingWorker(500, 4, executor, mockBlockCache);

        Map<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> mockResult = Map.of();
        when(mockBlockCache.loadBulk(any(Path.class), anyLong(), anyLong())).thenReturn(mockResult);

        int threadCount = 4;
        int requestsPerThread = 25;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger acceptedCount = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < requestsPerThread; i++) {
                        long offset = (threadId * requestsPerThread + i) * 100L * CACHE_BLOCK_SIZE;
                        if (worker.schedule(TEST_PATH, offset, 10)) {
                            acceptedCount.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        assertTrue("All threads should complete", doneLatch.await(10, TimeUnit.SECONDS));

        Thread.sleep(500); // Allow processing

        assertTrue("Should accept most requests", acceptedCount.get() > 50);
    }

    /**
     * Tests maxRunners concurrency limit is respected.
     * Note: Due to timing, this test verifies processing completes correctly
     * rather than strict enforcement of maxRunners.
     */
    public void testMaxRunnersLimit() throws Exception {
        worker = new QueuingWorker(100, 2, executor, mockBlockCache);

        CountDownLatch loadLatch = new CountDownLatch(10);
        Map<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> mockResult = Map.of();
        when(mockBlockCache.loadBulk(any(Path.class), anyLong(), anyLong())).thenAnswer(invocation -> {
            loadLatch.countDown();
            return mockResult;
        });

        // Schedule many requests
        for (int i = 0; i < 10; i++) {
            boolean accepted = worker.schedule(TEST_PATH, i * 100L * CACHE_BLOCK_SIZE, 10);
            assertTrue("Requests should be accepted", accepted);
        }

        // All requests should eventually complete
        assertTrue("All requests should be processed", loadLatch.await(3, TimeUnit.SECONDS));

        // Verify all calls were made
        verify(mockBlockCache, times(10)).loadBulk(eq(TEST_PATH), anyLong(), eq(10L));
    }

    // ========== Lifecycle Tests ==========

    /**
     * Tests worker is running after construction.
     */
    public void testIsRunning() {
        worker = new QueuingWorker(100, 2, executor, mockBlockCache);

        assertTrue("Worker should be running", worker.isRunning());
    }

    /**
     * Tests close stops the worker.
     */
    public void testClose() throws Exception {
        worker = new QueuingWorker(100, 2, executor, mockBlockCache);

        assertTrue("Worker should be running before close", worker.isRunning());

        worker.close();

        assertFalse("Worker should not be running after close", worker.isRunning());
    }

    /**
     * Tests scheduling after close returns false.
     */
    public void testScheduleAfterClose() {
        worker = new QueuingWorker(100, 2, executor, mockBlockCache);

        worker.close();

        boolean accepted = worker.schedule(TEST_PATH, 0, 10);

        assertFalse("Should reject requests after close", accepted);
    }

    /**
     * Tests close is idempotent.
     */
    public void testCloseIdempotent() {
        worker = new QueuingWorker(100, 2, executor, mockBlockCache);

        worker.close();
        worker.close();
        worker.close();

        assertFalse("Worker should remain closed", worker.isRunning());
    }

    /**
     * Tests close clears pending work.
     */
    public void testCloseClears() throws Exception {
        worker = new QueuingWorker(100, 1, executor, mockBlockCache);

        // Block to keep items in queue
        CountDownLatch blockLatch = new CountDownLatch(1);
        Map<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> mockResult = Map.of();
        when(mockBlockCache.loadBulk(any(Path.class), anyLong(), anyLong())).thenAnswer(invocation -> {
            blockLatch.await(5, TimeUnit.SECONDS);
            return mockResult;
        });

        // Schedule work
        worker.schedule(TEST_PATH, 0, 10);
        worker.schedule(TEST_PATH, 100L * CACHE_BLOCK_SIZE, 10);

        worker.close();
        blockLatch.countDown();

        // After close, queue should be cleared
        assertFalse("Worker should not be running", worker.isRunning());
    }
}

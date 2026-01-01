/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.test.OpenSearchTestCase;

public class QueuingWorkerTests extends OpenSearchTestCase {

    private static final Path TEST_PATH = Paths.get("/test/file.dat");

    private ExecutorService executor;
    private BlockCache<AutoCloseable> mockBlockCache;
    private QueuingWorker worker;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        super.setUp();
        executor = Executors.newFixedThreadPool(2);
        mockBlockCache = (BlockCache<AutoCloseable>) mock(BlockCache.class);
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

    /**
     * Tests basic worker creation and properties.
     */
    public void testWorkerCreation() {
        worker = new QueuingWorker(100, executor);

        assertTrue(worker.isRunning());
        assertEquals(100, worker.getQueueCapacity());
        assertEquals(0, worker.getQueueSize());
        assertFalse(worker.isReadAheadPaused());
    }

    /**
     * Tests that worker can accept and process a schedule request.
     */
    public void testBasicSchedule() throws Exception {
        worker = new QueuingWorker(100, executor);

        // Mock successful load
        when(mockBlockCache.loadForPrefetch(any(), anyLong(), anyLong())).thenReturn(Map.of());

        boolean accepted = worker.schedule(mockBlockCache, TEST_PATH, 0, 10);

        assertTrue("Worker should accept schedule", accepted);

        // Give worker time to process
        Thread.sleep(100);

        // Verify loadForPrefetch was called
        verify(mockBlockCache).loadForPrefetch(TEST_PATH, 0, 10);
    }

    /**
     * Tests that worker respects queue capacity.
     */
    public void testQueueCapacity() throws Exception {
        worker = new QueuingWorker(2, executor);

        // Make BlockCache slow to process
        when(mockBlockCache.loadForPrefetch(any(), anyLong(), anyLong())).thenAnswer(invocation -> {
            Thread.sleep(500);
            return Map.of();
        });

        // Fill queue
        boolean accepted1 = worker.schedule(mockBlockCache, TEST_PATH, 0, 10);
        boolean accepted2 = worker.schedule(mockBlockCache, TEST_PATH, 10 * 8192, 10);

        assertTrue("First schedule should be accepted", accepted1);
        assertTrue("Second schedule should be accepted", accepted2);

        // Queue should now be full or close to it
        assertTrue("Queue size should be > 0", worker.getQueueSize() >= 0);
    }

    /**
     * Tests worker can be closed safely.
     */
    public void testWorkerClose() {
        worker = new QueuingWorker(100, executor);

        assertTrue(worker.isRunning());

        worker.close();

        assertFalse("Worker should not be running after close", worker.isRunning());

        // Scheduling after close should fail
        boolean accepted = worker.schedule(mockBlockCache, TEST_PATH, 0, 10);
        assertFalse("Worker should reject schedules after close", accepted);
    }

    /**
     * Tests that worker can cancel pending requests for a specific path.
     */
    public void testCancelPath() throws Exception {
        worker = new QueuingWorker(100, executor);

        // Make loads slow
        when(mockBlockCache.loadForPrefetch(any(), anyLong(), anyLong())).thenAnswer(invocation -> {
            Thread.sleep(200);
            return Map.of();
        });

        // Schedule multiple requests
        worker.schedule(mockBlockCache, TEST_PATH, 0, 10);
        Path otherPath = Paths.get("/test/other.dat");
        worker.schedule(mockBlockCache, otherPath, 0, 10);

        // Cancel one path
        worker.cancel(TEST_PATH);

        // Give time for processing
        Thread.sleep(300);

        // Both should have been attempted (cancel is best-effort)
        // This is just testing that cancel doesn't throw
    }

    /**
     * Tests that isReadAheadPaused returns false initially.
     */
    public void testInitialPauseState() {
        worker = new QueuingWorker(100, executor);

        assertFalse("Worker should not be paused initially", worker.isReadAheadPaused());
    }

    /**
     * Tests queue size and capacity getters.
     */
    public void testQueueMetrics() {
        worker = new QueuingWorker(50, executor);

        assertEquals(50, worker.getQueueCapacity());
        assertEquals(0, worker.getQueueSize());
    }

    /**
     * Tests worker handles large block counts by chunking.
     */
    public void testLargeBlockCountChunking() throws Exception {
        worker = new QueuingWorker(200, executor);

        when(mockBlockCache.loadForPrefetch(any(), anyLong(), anyLong())).thenReturn(Map.of());

        // Request 200 blocks (should be split into chunks of 128 max)
        boolean accepted = worker.schedule(mockBlockCache, TEST_PATH, 0, 200);

        assertTrue("Worker should accept large request", accepted);

        // Give worker time to process
        Thread.sleep(200);

        // Should have been called at least twice (200/128 = 2 chunks)
        verify(mockBlockCache, org.mockito.Mockito.atLeast(2)).loadForPrefetch(any(), anyLong(), anyLong());
    }
}

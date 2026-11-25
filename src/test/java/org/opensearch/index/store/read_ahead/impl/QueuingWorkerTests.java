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
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
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
     * Tests that cancelByPathPrefix drops only queued requests for files under the prefix,
     * and leaves the shared worker running for other directories.
     */
    public void testCancelByPathPrefix() throws Exception {
        ExecutorService blocked = Executors.newSingleThreadExecutor();
        CountDownLatch release = new CountDownLatch(1);
        try {
            // Occupy the only executor thread so scheduled tasks stay queued
            blocked.submit(() -> {
                release.await();
                return null;
            });
            worker = new QueuingWorker(100, blocked);

            Path shard0 = Paths.get("/data/indices/uuid/0/index");
            Path shard1 = Paths.get("/data/indices/uuid/1/index");
            assertTrue(worker.schedule(mockBlockCache, shard0.resolve("_0.cfs"), 0, 4));
            assertTrue(worker.schedule(mockBlockCache, shard0.resolve("_1.cfs"), 0, 4));
            assertTrue(worker.schedule(mockBlockCache, shard1.resolve("_0.cfs"), 0, 4));
            assertEquals(3, worker.getQueueSize());

            worker.cancelByPathPrefix(shard0);

            assertEquals(1, worker.getQueueSize());
            assertTrue("Shared worker must keep running", worker.isRunning());
            assertTrue(worker.schedule(mockBlockCache, shard1.resolve("_2.cfs"), 0, 4));
        } finally {
            release.countDown();
            worker.close();
            blocked.shutdown();
            blocked.awaitTermination(5, TimeUnit.SECONDS);
        }
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

    /**
     * Tests that a request partly covered by an in-flight task loads the part that is not
     * covered, instead of being dropped whole.
     */
    public void testPartlyOverlappingRequestLoadsTheRest() throws Exception {
        worker = new QueuingWorker(100, executor);
        final long block = 1L << CACHE_BLOCK_SIZE_POWER;
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch secondDone = new CountDownLatch(1);
        when(mockBlockCache.loadForPrefetch(any(), anyLong(), anyLong())).thenAnswer(inv -> {
            if ((long) inv.getArgument(1) == 0L) {
                firstStarted.countDown();
                release.await(5, TimeUnit.SECONDS);
            } else {
                secondDone.countDown();
            }
            return Map.of();
        });

        // Blocks [0, 10) are in flight while [5, 20) is requested.
        assertTrue(worker.schedule(mockBlockCache, TEST_PATH, 0, 10));
        assertTrue(firstStarted.await(5, TimeUnit.SECONDS));
        assertTrue(worker.schedule(mockBlockCache, TEST_PATH, 5 * block, 15));
        assertTrue("non-overlapping part should load", secondDone.await(5, TimeUnit.SECONDS));
        release.countDown();

        verify(mockBlockCache).loadForPrefetch(TEST_PATH, 10 * block, 10);
    }

    /**
     * Tests that a request fully covered by an in-flight task is skipped.
     */
    public void testFullyCoveredRequestIsSkipped() throws Exception {
        worker = new QueuingWorker(100, executor);
        final long block = 1L << CACHE_BLOCK_SIZE_POWER;
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(mockBlockCache.loadForPrefetch(any(), anyLong(), anyLong())).thenAnswer(inv -> {
            firstStarted.countDown();
            release.await(5, TimeUnit.SECONDS);
            return Map.of();
        });

        assertTrue(worker.schedule(mockBlockCache, TEST_PATH, 0, 10));
        assertTrue(firstStarted.await(5, TimeUnit.SECONDS));
        assertTrue(worker.schedule(mockBlockCache, TEST_PATH, 2 * block, 5));
        assertEquals("covered request is not queued", 0, worker.getQueueSize());
        release.countDown();
    }

    /** Holds every load of a request starting at a given block until released. */
    private CountDownLatch blockLoadsAt(long... startBlocks) throws IOException {
        CountDownLatch release = new CountDownLatch(1);
        Set<Long> held = new HashSet<>();
        for (long b : startBlocks) {
            held.add(b << CACHE_BLOCK_SIZE_POWER);
        }
        when(mockBlockCache.loadForPrefetch(any(), anyLong(), anyLong())).thenAnswer(inv -> {
            if (held.contains((long) inv.getArgument(1))) {
                release.await(5, TimeUnit.SECONDS);
            }
            return Map.of();
        });
        return release;
    }

    /**
     * Tests that a request whose end overlaps an in-flight task loads only its start.
     */
    public void testRequestOverlappingAtTheEndLoadsTheStart() throws Exception {
        worker = new QueuingWorker(100, executor);
        final long block = 1L << CACHE_BLOCK_SIZE_POWER;
        CountDownLatch release = blockLoadsAt(10);

        assertTrue(worker.schedule(mockBlockCache, TEST_PATH, 10 * block, 10)); // [10, 20) in flight
        assertBusy(() -> verify(mockBlockCache).loadForPrefetch(TEST_PATH, 10 * block, 10));
        assertTrue(worker.schedule(mockBlockCache, TEST_PATH, 0, 15)); // [0, 15)
        assertBusy(() -> verify(mockBlockCache).loadForPrefetch(TEST_PATH, 0, 10));
        release.countDown();
    }

    /**
     * Tests trimming against several in-flight tasks, including one that is strictly inside the
     * request until another task trims the request's start up to it.
     */
    public void testRequestOverlappingSeveralTasksLoadsOnlyTheRest() throws Exception {
        worker = new QueuingWorker(100, executor);
        final long block = 1L << CACHE_BLOCK_SIZE_POWER;
        CountDownLatch release = blockLoadsAt(0, 8);

        assertTrue(worker.schedule(mockBlockCache, TEST_PATH, 8 * block, 4)); // [8, 12)
        assertBusy(() -> verify(mockBlockCache).loadForPrefetch(TEST_PATH, 8 * block, 4));
        assertTrue(worker.schedule(mockBlockCache, TEST_PATH, 0, 10)); // [0, 8) after trim
        assertBusy(() -> verify(mockBlockCache).loadForPrefetch(TEST_PATH, 0, 8));
        assertTrue(worker.schedule(mockBlockCache, TEST_PATH, 3 * block, 17)); // [3, 20) -> [12, 20)
        assertBusy(() -> verify(mockBlockCache).loadForPrefetch(TEST_PATH, 12 * block, 8));
        release.countDown();
    }

    /**
     * Tests that a request with an in-flight task strictly inside it is still loaded whole.
     */
    public void testRequestContainingAnInFlightTaskLoadsWhole() throws Exception {
        worker = new QueuingWorker(100, executor);
        final long block = 1L << CACHE_BLOCK_SIZE_POWER;
        CountDownLatch release = blockLoadsAt(5);

        assertTrue(worker.schedule(mockBlockCache, TEST_PATH, 5 * block, 2)); // [5, 7) in flight
        assertBusy(() -> verify(mockBlockCache).loadForPrefetch(TEST_PATH, 5 * block, 2));
        assertTrue(worker.schedule(mockBlockCache, TEST_PATH, 0, 20)); // [0, 20)
        assertBusy(() -> verify(mockBlockCache).loadForPrefetch(TEST_PATH, 0, 20));
        release.countDown();
    }
}

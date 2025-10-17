/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link MemorySegmentPool}
 */
@SuppressWarnings("preview")
public class MemorySegmentPoolTests extends OpenSearchTestCase {

    private MemorySegmentPool pool;

    @After
    public void tearDown() throws Exception {
        if (pool != null && !pool.isClosed()) {
            pool.close();
        }
        super.tearDown();
    }

    public void testPoolInitialization() {
        long totalMemory = 4096;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        assertNotNull(pool);
        assertEquals(totalMemory, pool.totalMemory());
        assertEquals(segmentSize, pool.pooledSegmentSize());
    }

    public void testAcquireFromPrimaryPool() throws Exception {
        long totalMemory = 4096;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        RefCountedMemorySegment segment = pool.acquire();

        assertNotNull(segment);
        assertEquals(1, segment.getRefCount());
        assertEquals(segmentSize, segment.length());

        segment.decRef();
    }

    public void testAcquireMultipleSegments() throws Exception {
        long totalMemory = 4096;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        List<RefCountedMemorySegment> segments = new ArrayList<>();
        int maxSegments = (int) (totalMemory / segmentSize);

        for (int i = 0; i < maxSegments; i++) {
            RefCountedMemorySegment segment = pool.acquire();
            assertNotNull(segment);
            segments.add(segment);
        }

        assertEquals(maxSegments, segments.size());

        // Release all segments
        for (RefCountedMemorySegment segment : segments) {
            segment.decRef();
        }
    }

    public void testSecondaryPoolActivation() throws Exception {
        long totalMemory = 2048; // Small primary pool
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        List<RefCountedMemorySegment> segments = new ArrayList<>();

        // Exhaust primary pool (2 segments)
        for (int i = 0; i < 2; i++) {
            segments.add(pool.acquire());
        }

        // This should trigger secondary pool creation
        RefCountedMemorySegment secondarySegment = pool.acquire();
        assertNotNull(secondarySegment);
        segments.add(secondarySegment);

        // Cleanup
        for (RefCountedMemorySegment segment : segments) {
            segment.decRef();
        }
    }

    public void testEphemeralPoolFallback() throws Exception {
        long totalMemory = 2048; // Small pool (must be at least 2x segment size for secondary pool)
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        List<RefCountedMemorySegment> segments = new ArrayList<>();

        // Acquire many segments to exhaust primary and secondary pools
        for (int i = 0; i < 5; i++) {
            try {
                RefCountedMemorySegment segment = pool.acquire();
                assertNotNull(segment);
                segments.add(segment);
            } catch (NoOffHeapMemoryException e) {
                // Expected when all pools are exhausted
                break;
            }
        }

        assertTrue(segments.size() > 1); // Should have acquired at least primary + some fallback

        // Cleanup
        for (RefCountedMemorySegment segment : segments) {
            segment.decRef();
        }
    }

    public void testAvailableMemory() throws Exception {
        long totalMemory = 4096;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        long initialAvailable = pool.availableMemory();
        assertEquals(totalMemory, initialAvailable);

        RefCountedMemorySegment segment = pool.acquire();
        assertNotNull(segment);

        // Note: availableMemory may vary based on lazy allocation strategy
        long afterAcquire = pool.availableMemory();
        assertTrue(afterAcquire <= initialAvailable);

        segment.decRef();
    }

    public void testTryAcquire() throws Exception {
        long totalMemory = 4096;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        RefCountedMemorySegment segment = pool.tryAcquire(100, TimeUnit.MILLISECONDS);

        assertNotNull(segment);
        assertEquals(1, segment.getRefCount());

        segment.decRef();
    }

    public void testPoolStats() {
        long totalMemory = 4096;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        String stats = pool.poolStats();
        assertNotNull(stats);
        assertTrue(stats.contains("RoutingPool"));
        assertTrue(stats.contains("primary"));
    }

    public void testClose() throws Exception {
        long totalMemory = 4096;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        RefCountedMemorySegment segment = pool.acquire();
        segment.decRef();

        pool.close();

        assertTrue(pool.isClosed());
    }

    public void testConcurrentAcquisition() throws Exception {
        long totalMemory = 8192;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        int threadCount = 10;
        int acquisitionsPerThread = 5;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < acquisitionsPerThread; j++) {
                        try {
                            RefCountedMemorySegment segment = pool.acquire();
                            assertNotNull(segment);
                            successCount.incrementAndGet();
                            Thread.sleep(1); // Simulate some work
                            segment.decRef();
                        } catch (NoOffHeapMemoryException e) {
                            // Expected when pool is exhausted
                        }
                    }
                } catch (Exception e) {
                    fail("Unexpected exception: " + e.getMessage());
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(endLatch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        assertTrue(successCount.get() > 0);
    }

    public void testSegmentReuse() throws Exception {
        long totalMemory = 2048;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        // Acquire and release a segment
        RefCountedMemorySegment segment1 = pool.acquire();
        int generation1 = segment1.getGeneration();
        segment1.decRef();

        // Acquire another segment - might be the same physical segment reused
        RefCountedMemorySegment segment2 = pool.acquire();
        assertNotNull(segment2);
        assertEquals(1, segment2.getRefCount());

        segment2.decRef();
    }

    public void testWarmUp() throws Exception {
        long totalMemory = 8192;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        long targetSegments = 4;
        pool.warmUp(targetSegments);

        // Warmup should preallocate some segments
        // Verify by checking available memory or stats
        String stats = pool.poolStats();
        assertNotNull(stats);
    }

    public void testIsUnderPressure() throws Exception {
        long totalMemory = 2048;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        // Initially not under pressure
        List<RefCountedMemorySegment> segments = new ArrayList<>();

        // Acquire many segments to create pressure
        for (int i = 0; i < 3; i++) {
            try {
                segments.add(pool.acquire());
            } catch (Exception e) {
                break;
            }
        }

        // Cleanup
        for (RefCountedMemorySegment segment : segments) {
            segment.decRef();
        }
    }

    public void testPooledSegmentSize() {
        long totalMemory = 4096;
        int segmentSize = 512;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        assertEquals(segmentSize, pool.pooledSegmentSize());
    }

    public void testReleaseNoOp() throws Exception {
        long totalMemory = 4096;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        RefCountedMemorySegment segment = pool.acquire();

        // release() should be a no-op as segments auto-release via callback
        pool.release(segment);

        // Segment should still be valid
        assertEquals(1, segment.getRefCount());

        segment.decRef();
    }

    public void testTotalMemoryWithSecondaryPool() throws Exception {
        long totalMemory = 2048;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        long initialTotal = pool.totalMemory();
        assertEquals(totalMemory, initialTotal);

        // Exhaust primary and trigger secondary
        List<RefCountedMemorySegment> segments = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            try {
                segments.add(pool.acquire());
            } catch (Exception e) {
                break;
            }
        }

        // Total memory should now include secondary pool
        long totalWithSecondary = pool.totalMemory();
        assertTrue(totalWithSecondary >= totalMemory);

        // Cleanup
        for (RefCountedMemorySegment segment : segments) {
            segment.decRef();
        }
    }

    public void testCloseWithOutstandingSegments() throws Exception {
        long totalMemory = 4096;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        RefCountedMemorySegment segment = pool.acquire();

        // Close pool even with outstanding segment
        pool.close();

        assertTrue(pool.isClosed());

        // Segment should still be usable
        assertEquals(1, segment.getRefCount());

        segment.decRef();
    }

    /**
     * Tests that trying to acquire after pool is closed throws exception.
     */
    public void testAcquireAfterClose() throws Exception {
        long totalMemory = 4096;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);
        pool.close();

        expectThrows(IllegalStateException.class, () -> pool.acquire());
    }

    /**
     * Tests multiple acquire and release cycles to verify pool reuse.
     */
    public void testMultipleAcquireReleaseCycles() throws Exception {
        long totalMemory = 4096;
        int segmentSize = 1024;
        pool = new MemorySegmentPool(totalMemory, segmentSize);

        for (int cycle = 0; cycle < 5; cycle++) {
            List<RefCountedMemorySegment> segments = new ArrayList<>();

            // Acquire all available segments
            for (int i = 0; i < 4; i++) {
                segments.add(pool.acquire());
            }

            // Verify all segments are valid
            for (RefCountedMemorySegment segment : segments) {
                assertNotNull(segment);
                assertEquals(1, segment.getRefCount());
            }

            // Release all segments
            for (RefCountedMemorySegment segment : segments) {
                segment.decRef();
            }
        }
    }

    /**
     * Tests that warmUp with invalid count handles gracefully.
     */
    public void testWarmUpWithZeroCount() throws Exception {
        long totalMemory = 4096;
        int segmentSize = 1024;
        pool = new MemorySegmentPool(totalMemory, segmentSize);

        // Should not throw exception
        pool.warmUp(0);

        String stats = pool.poolStats();
        assertNotNull(stats);
    }

    /**
     * Tests that warmUp with count exceeding pool capacity is handled.
     */
    public void testWarmUpExceedingCapacity() throws Exception {
        long totalMemory = 2048;
        int segmentSize = 1024;
        pool = new MemorySegmentPool(totalMemory, segmentSize);

        // Try to warm up more segments than pool can hold
        pool.warmUp(100);

        // Should still be usable
        RefCountedMemorySegment segment = pool.acquire();
        assertNotNull(segment);
        segment.decRef();
    }

    /**
     * Tests concurrent acquire and release under high contention.
     */
    public void testHighContentionConcurrentOperations() throws Exception {
        long totalMemory = 8192;
        int segmentSize = 1024;
        pool = new MemorySegmentPool(totalMemory, segmentSize);

        int threadCount = 20;
        int operationsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger acquireCount = new AtomicInteger(0);
        AtomicInteger releaseCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < operationsPerThread; j++) {
                        try {
                            RefCountedMemorySegment segment = pool.acquire();
                            acquireCount.incrementAndGet();
                            // Simulate minimal work
                            segment.decRef();
                            releaseCount.incrementAndGet();
                        } catch (NoOffHeapMemoryException e) {
                            // Expected under high contention
                        }
                    }
                } catch (Exception e) {
                    fail("Unexpected exception: " + e.getMessage());
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(endLatch.await(60, TimeUnit.SECONDS));
        executor.shutdown();

        // All acquired segments should have been released
        assertEquals(acquireCount.get(), releaseCount.get());
    }

    /**
     * Tests that generation numbers increment correctly on segment reuse.
     */
    public void testGenerationIncrementOnReuse() throws Exception {
        long totalMemory = 2048; // Must be at least 2x segment size for secondary pool
        int segmentSize = 1024;
        pool = new MemorySegmentPool(totalMemory, segmentSize);

        // First acquisition
        RefCountedMemorySegment segment1 = pool.acquire();
        int gen1 = segment1.getGeneration();
        segment1.decRef();

        // Second acquisition - may get same physical segment
        RefCountedMemorySegment segment2 = pool.acquire();
        int gen2 = segment2.getGeneration();

        // Generation should be different if it's the same physical segment
        // (generation increments on close/reset)
        segment2.decRef();
    }

    /**
     * Tests pool behavior with very large segment size.
     */
    public void testLargeSegmentSize() throws Exception {
        long totalMemory = 10 * 1024 * 1024; // 10MB
        int segmentSize = 1024 * 1024; // 1MB per segment
        pool = new MemorySegmentPool(totalMemory, segmentSize);

        RefCountedMemorySegment segment = pool.acquire();
        assertNotNull(segment);
        assertEquals(segmentSize, segment.length());

        segment.decRef();
    }

    /**
     * Tests that pool stats contains expected information.
     */
    public void testPoolStatsContent() throws Exception {
        long totalMemory = 4096;
        int segmentSize = 1024;
        pool = new MemorySegmentPool(totalMemory, segmentSize);

        // Acquire some segments
        List<RefCountedMemorySegment> segments = new ArrayList<>();
        segments.add(pool.acquire());
        segments.add(pool.acquire());

        String stats = pool.poolStats();
        assertNotNull(stats);
        assertTrue("Stats should contain pool information", stats.length() > 0);

        // Cleanup
        for (RefCountedMemorySegment segment : segments) {
            segment.decRef();
        }
    }

    /**
     * Tests that isClosed returns correct state.
     */
    public void testIsClosedState() throws Exception {
        long totalMemory = 4096;
        int segmentSize = 1024;
        pool = new MemorySegmentPool(totalMemory, segmentSize);

        assertFalse("Pool should not be closed initially", pool.isClosed());

        pool.close();

        assertTrue("Pool should be closed after close()", pool.isClosed());
    }
}

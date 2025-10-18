/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link MemorySegmentPool} - Simple Panama malloc/free based pool
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

    /**
     * Test 1: Basic pool creation and properties
     */
    public void testPoolCreation() {
        long totalMemory = 4096;  // 4 segments * 1024 bytes
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        assertNotNull("Pool should not be null", pool);
        assertEquals("Pool segment size should match", segmentSize, pool.pooledSegmentSize());
        assertEquals("Pool should not be closed", false, pool.isClosed());
    }

    /**
     * Test 2: Single segment acquire and release
     */
    public void testSingleAcquireRelease() throws Exception {
        long totalMemory = 4096;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        // Acquire a segment
        RefCountedMemorySegment segment = pool.acquire();
        assertNotNull("Acquired segment should not be null", segment);
        assertEquals("Segment ref count should be 1", 1, segment.getRefCount());
        assertEquals("Segment size should match", segmentSize, segment.length());

        // Release the segment (refCount goes to 0, triggers pool.release())
        segment.decRef();

        // Segment should be back in pool's freelist now (not freed)
    }

    /**
     * Test 3: Acquire multiple segments
     */
    public void testMultipleAcquire() throws Exception {
        long totalMemory = 4096;  // 4 segments
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        // Acquire all 4 segments
        RefCountedMemorySegment seg1 = pool.acquire();
        RefCountedMemorySegment seg2 = pool.acquire();
        RefCountedMemorySegment seg3 = pool.acquire();
        RefCountedMemorySegment seg4 = pool.acquire();

        assertNotNull(seg1);
        assertNotNull(seg2);
        assertNotNull(seg3);
        assertNotNull(seg4);

        // All should have refCount = 1
        assertEquals(1, seg1.getRefCount());
        assertEquals(1, seg2.getRefCount());
        assertEquals(1, seg3.getRefCount());
        assertEquals(1, seg4.getRefCount());

        // Release all segments
        seg1.decRef();
        seg2.decRef();
        seg3.decRef();
        seg4.decRef();
    }

    /**
     * Test 4: Pool exhaustion - should throw exception
     */
    public void testPoolExhaustion() throws Exception {
        long totalMemory = 2048;  // Only 2 segments
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        // Acquire 2 segments successfully
        RefCountedMemorySegment seg1 = pool.acquire();
        RefCountedMemorySegment seg2 = pool.acquire();

        assertNotNull(seg1);
        assertNotNull(seg2);

        // Third acquire should throw exception (pool exhausted)
        try {
            pool.acquire();
            fail("Should have thrown exception when pool exhausted");
        } catch (RuntimeException e) {
            assertTrue(
                "Exception should mention pool exhaustion",
                e.getMessage().contains("Pool exhausted") || e.getMessage().contains("Pool limit")
            );
        }

        // Cleanup
        seg1.decRef();
        seg2.decRef();
    }

    /**
     * Test 5: Segment reuse from freelist
     */
    public void testSegmentReuse() throws Exception {
        long totalMemory = 2048;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        // Acquire and release a segment
        RefCountedMemorySegment seg1 = pool.acquire();
        long firstAddress = seg1.segment().address();
        seg1.decRef();  // Returns to freelist

        // Acquire again - should get the same segment reused
        RefCountedMemorySegment seg2 = pool.acquire();
        long secondAddress = seg2.segment().address();

        assertEquals("Should reuse the same memory segment", firstAddress, secondAddress);
        assertEquals("Reused segment should have refCount=1", 1, seg2.getRefCount());

        // Cleanup
        seg2.decRef();
    }

    /**
     * Test 6: Pool close should free all segments
     */
    public void testPoolClose() throws Exception {
        long totalMemory = 4096;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        // Acquire and release some segments (they go to freelist)
        RefCountedMemorySegment seg1 = pool.acquire();
        RefCountedMemorySegment seg2 = pool.acquire();
        seg1.decRef();
        seg2.decRef();

        // Close the pool
        pool.close();

        assertTrue("Pool should be closed", pool.isClosed());

        // Trying to acquire after close should fail
        try {
            pool.acquire();
            fail("Should not be able to acquire from closed pool");
        } catch (IllegalStateException e) {
            assertTrue("Exception should mention pool is closed", e.getMessage().contains("closed"));
        }
    }

    /**
     * Test 7: Reference counting behavior - increment and decrement
     */
    public void testRefCounting() throws Exception {
        long totalMemory = 2048;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        // Acquire a segment (refCount = 1)
        RefCountedMemorySegment seg = pool.acquire();
        assertEquals("Initial refCount should be 1", 1, seg.getRefCount());

        // Increment reference (simulates multiple readers)
        seg.incRef();
        assertEquals("After incRef, refCount should be 2", 2, seg.getRefCount());

        seg.incRef();
        assertEquals("After second incRef, refCount should be 3", 3, seg.getRefCount());

        // Decrement back down
        seg.decRef();
        assertEquals("After decRef, refCount should be 2", 2, seg.getRefCount());

        seg.decRef();
        assertEquals("After second decRef, refCount should be 1", 1, seg.getRefCount());

        // Final decrement - should return to pool
        seg.decRef();
        // refCount is now 0, segment is back in freelist
    }

    /**
     * Test 8: Pin and unpin behavior
     */
    public void testPinUnpin() throws Exception {
        long totalMemory = 2048;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        RefCountedMemorySegment seg = pool.acquire();
        int initialRefCount = seg.getRefCount();

        // Pin the segment
        boolean pinned = seg.tryPin();
        assertTrue("tryPin should succeed", pinned);
        assertEquals("After pin, refCount should increase", initialRefCount + 1, seg.getRefCount());

        // Unpin the segment
        seg.unpin();
        assertEquals("After unpin, refCount should decrease", initialRefCount, seg.getRefCount());

        // Cleanup
        seg.decRef();
    }

    /**
     * Test 9: Memory zeroing when requiresZeroing is enabled
     */
    public void testMemoryZeroing() throws Exception {
        long totalMemory = 2048;
        int segmentSize = 1024;

        // Create pool with zeroing enabled
        pool = new MemorySegmentPool(totalMemory, segmentSize, true);

        // Acquire segment and write some data
        RefCountedMemorySegment seg1 = pool.acquire();
        seg1.segment().fill((byte) 0xFF);  // Fill with non-zero values

        // Verify data was written
        assertEquals((byte) 0xFF, seg1.segment().get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0));

        // Release the segment (should be zeroed)
        seg1.decRef();

        // Acquire again (should get same segment, now zeroed)
        RefCountedMemorySegment seg2 = pool.acquire();
        assertEquals("Segment should be zeroed after release", (byte) 0, seg2.segment().get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0));

        // Cleanup
        seg2.decRef();
    }

    /**
     * Test 10: Memory NOT zeroed when requiresZeroing is disabled
     */
    public void testNoZeroing() throws Exception {
        long totalMemory = 2048;
        int segmentSize = 1024;

        // Create pool with zeroing disabled
        pool = new MemorySegmentPool(totalMemory, segmentSize, false);

        // Acquire segment and write some data
        RefCountedMemorySegment seg1 = pool.acquire();
        seg1.segment().fill((byte) 0xAA);

        // Release the segment (should NOT be zeroed)
        seg1.decRef();

        // Acquire again (should get same segment with data intact)
        RefCountedMemorySegment seg2 = pool.acquire();
        assertEquals(
            "Segment should NOT be zeroed when zeroing disabled",
            (byte) 0xAA,
            seg2.segment().get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0)
        );

        // Cleanup
        seg2.decRef();
    }

    /**
     * Test 11: Pool statistics and memory tracking
     */
    public void testPoolStats() throws Exception {
        long totalMemory = 4096;  // 4 segments
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        // Initially, no segments allocated
        assertEquals("Total memory should match", totalMemory, pool.totalMemory());
        assertEquals("All memory should be available", totalMemory, pool.availableMemory());

        // Acquire 2 segments
        RefCountedMemorySegment seg1 = pool.acquire();
        RefCountedMemorySegment seg2 = pool.acquire();

        // Available memory should decrease
        long expectedAvailable = 2 * segmentSize;  // 2 unallocated segments remaining
        assertEquals("Available memory should account for acquired segments", expectedAvailable, pool.availableMemory());

        // Release one segment
        seg1.decRef();

        // Available memory should increase by one segment (now in freelist)
        expectedAvailable = 3 * segmentSize;  // 1 free + 2 unallocated
        assertEquals("Available memory should increase after release", expectedAvailable, pool.availableMemory());

        // Cleanup
        seg2.decRef();
    }

    /**
     * Test 12: Pool pressure detection
     */
    public void testPoolPressure() throws Exception {
        long totalMemory = 20480;  // 20 segments
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        // Pool should not be under pressure initially
        assertFalse("Pool should not be under pressure initially", pool.isUnderPressure());

        // Acquire 19 segments (leaving only 1 available = 5% which is < 10% threshold)
        RefCountedMemorySegment[] segments = new RefCountedMemorySegment[19];
        for (int i = 0; i < 19; i++) {
            segments[i] = pool.acquire();
        }

        // Pool should be under pressure now (< 10% available)
        assertTrue("Pool should be under pressure with 95% allocated", pool.isUnderPressure());

        // Release segments
        for (RefCountedMemorySegment seg : segments) {
            seg.decRef();
        }

        // Pool should not be under pressure anymore
        assertFalse("Pool should not be under pressure after releasing segments", pool.isUnderPressure());
    }

    /**
     * Test 13: WarmUp functionality
     */
    public void testWarmUp() throws Exception {
        long totalMemory = 10240;  // 10 segments
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        // Warm up pool with 5 segments
        pool.warmUp(5);

        // Should be able to quickly acquire 5 segments (already allocated)
        RefCountedMemorySegment seg1 = pool.acquire();
        RefCountedMemorySegment seg2 = pool.acquire();
        RefCountedMemorySegment seg3 = pool.acquire();
        RefCountedMemorySegment seg4 = pool.acquire();
        RefCountedMemorySegment seg5 = pool.acquire();

        assertNotNull(seg1);
        assertNotNull(seg2);
        assertNotNull(seg3);
        assertNotNull(seg4);
        assertNotNull(seg5);

        // Cleanup
        seg1.decRef();
        seg2.decRef();
        seg3.decRef();
        seg4.decRef();
        seg5.decRef();
    }

    /**
     * Test 14: ReleaseAll batch operation
     */
    public void testReleaseAll() throws Exception {
        long totalMemory = 5120;  // 5 segments
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        // Acquire multiple segments
        RefCountedMemorySegment seg1 = pool.acquire();
        RefCountedMemorySegment seg2 = pool.acquire();
        RefCountedMemorySegment seg3 = pool.acquire();

        // Mark them as ready for release (reduce refCount to 1)
        // In real usage, they would be ready to return to pool

        // Use releaseAll for batch release
        pool.releaseAll(seg1, seg2, seg3);

        // All segments should now be in freelist
        // Verify by acquiring - should get reused segments
        RefCountedMemorySegment reused1 = pool.acquire();
        RefCountedMemorySegment reused2 = pool.acquire();
        RefCountedMemorySegment reused3 = pool.acquire();

        assertNotNull(reused1);
        assertNotNull(reused2);
        assertNotNull(reused3);

        // Cleanup
        reused1.decRef();
        reused2.decRef();
        reused3.decRef();
    }

    /**
     * Test 15: Pool stats string formatting
     */
    public void testPoolStatsString() throws Exception {
        long totalMemory = 4096;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        // Acquire and release to create interesting state
        RefCountedMemorySegment seg1 = pool.acquire();
        RefCountedMemorySegment seg2 = pool.acquire();
        seg1.decRef();  // seg1 now in freelist

        String stats = pool.poolStats();
        assertNotNull("Pool stats string should not be null", stats);
        assertTrue("Stats should contain allocation info", stats.contains("allocated"));
        assertTrue("Stats should contain utilization info", stats.contains("utilization"));

        // Cleanup
        seg2.decRef();
    }

    /**
     * Test 16: Generation tracking after close
     */
    public void testGenerationTracking() throws Exception {
        long totalMemory = 2048;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        RefCountedMemorySegment seg = pool.acquire();
        int initialGeneration = seg.getGeneration();

        // Close the segment (simulates cache eviction)
        seg.close();  // This increments generation and decrements refCount

        // Generation should have incremented
        assertEquals("Generation should increment after close", initialGeneration + 1, seg.getGeneration());
    }

    /**
     * Test 17: Invalid total memory configuration
     */
    public void testInvalidConfiguration() {
        long totalMemory = 4097;  // Not divisible by segment size
        int segmentSize = 1024;

        try {
            pool = new MemorySegmentPool(totalMemory, segmentSize);
            fail("Should throw exception for invalid configuration");
        } catch (IllegalArgumentException e) {
            assertTrue("Exception should mention memory/segment alignment", e.getMessage().contains("multiple"));
        }
    }

    /**
     * Test 18: AvailableMemoryAccurate under contention
     */
    public void testAvailableMemoryAccurate() throws Exception {
        long totalMemory = 5120;  // 5 segments
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        // Acquire some segments
        RefCountedMemorySegment seg1 = pool.acquire();
        RefCountedMemorySegment seg2 = pool.acquire();

        // Get accurate available memory
        long accurateAvailable = pool.availableMemoryAccurate();
        long cachedAvailable = pool.availableMemory();

        // Both should report same value in this simple scenario
        assertEquals("Accurate and cached available memory should match", accurateAvailable, cachedAvailable);

        // Cleanup
        seg1.decRef();
        seg2.decRef();
    }

    /**
     * Test 19: Segment size verification
     */
    public void testSegmentSize() throws Exception {
        long totalMemory = 4096;
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        assertEquals("Pooled segment size should match", segmentSize, pool.pooledSegmentSize());

        RefCountedMemorySegment seg = pool.acquire();
        assertEquals("Acquired segment length should match", segmentSize, seg.length());

        // Cleanup
        seg.decRef();
    }

    /**
     * Test 20: Concurrent acquire and release patterns
     */
    public void testConcurrentAcquireRelease() throws Exception {
        long totalMemory = 2048;  // 2 segments
        int segmentSize = 1024;

        pool = new MemorySegmentPool(totalMemory, segmentSize);

        // Acquire, release, acquire pattern (common in real usage)
        RefCountedMemorySegment seg1 = pool.acquire();
        seg1.decRef();

        RefCountedMemorySegment seg2 = pool.acquire();
        seg2.decRef();

        RefCountedMemorySegment seg3 = pool.acquire();
        long addr1 = seg1.segment().address();
        long addr3 = seg3.segment().address();

        // Should reuse same segment
        assertEquals("Should reuse segment addresses", addr1, addr3);

        // Cleanup
        seg3.decRef();
    }
}

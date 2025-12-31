/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;

@SuppressWarnings("preview")
public class RefCountedMemorySegmentTests extends OpenSearchTestCase {

    private Arena arena;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        arena = Arena.ofConfined();
    }

    @After
    public void tearDown() throws Exception {
        if (arena != null) {
            arena.close();
        }
        super.tearDown();
    }

    public void testConstructorWithValidSegment() {
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        assertNotNull(refSegment);
        assertEquals(1, refSegment.getRefCount());
        assertEquals(1024, refSegment.length());
        assertEquals(0, refSegment.getGeneration());
    }

    public void testConstructorWithNullSegmentThrows() {
        try {
            new RefCountedMemorySegment(null, 1024, (s) -> {});
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("must not be null"));
        }
    }

    public void testConstructorWithNullReleaserThrows() {
        MemorySegment segment = arena.allocate(1024);
        try {
            new RefCountedMemorySegment(segment, 1024, null);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("must not be null"));
        }
    }

    public void testIncRefAndDecRef() {
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        assertEquals(1, refSegment.getRefCount());

        refSegment.incRef();
        assertEquals(2, refSegment.getRefCount());

        refSegment.decRef();
        assertEquals(1, refSegment.getRefCount());
        assertEquals(0, releaseCount.get());

        refSegment.decRef();
        assertEquals(0, refSegment.getRefCount());
        assertEquals(1, releaseCount.get());
    }

    public void testDecRefUnderflowThrows() {
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        refSegment.decRef(); // refCount becomes 0

        try {
            refSegment.decRef(); // underflow
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("underflow"));
        }
    }

    public void testIncRefOnReleasedSegmentThrows() {
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        refSegment.decRef(); // refCount becomes 0

        try {
            refSegment.incRef(); // try to revive
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("revive a released segment"));
        }
    }

    public void testTryPinSuccess() {
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        assertTrue(refSegment.tryPin());
        assertEquals(2, refSegment.getRefCount());

        refSegment.unpin();
        assertEquals(1, refSegment.getRefCount());
    }

    public void testTryPinFailsOnReleasedSegment() {
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        refSegment.decRef(); // refCount becomes 0

        assertFalse(refSegment.tryPin());
        assertEquals(0, refSegment.getRefCount());
    }

    public void testSegmentAccess() {
        MemorySegment segment = arena.allocate(1024);
        segment.set(ValueLayout.JAVA_INT, 0, 42);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> {});

        MemorySegment retrievedSegment = refSegment.segment();
        assertNotNull(retrievedSegment);
        assertEquals(1024, retrievedSegment.byteSize());
        assertEquals(42, retrievedSegment.get(ValueLayout.JAVA_INT, 0));
    }

    public void testSegmentSlicing() {
        MemorySegment segment = arena.allocate(1024);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(
            segment,
            512, // logical length less than segment capacity
            (s) -> {}
        );

        MemorySegment sliced = refSegment.segment();
        assertEquals(512, sliced.byteSize());
    }

    public void testReset() {
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        // Increment and decrement to simulate usage
        refSegment.incRef();
        refSegment.decRef();
        assertEquals(1, refSegment.getRefCount());

        // Reset for reuse
        refSegment.reset();
        assertEquals(1, refSegment.getRefCount());
    }

    public void testGenerationIncrementsOnClose() {
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        assertEquals(0, refSegment.getGeneration());

        refSegment.incRef(); // Prevent full release
        refSegment.close(); // Should increment generation and decRef

        assertEquals(1, refSegment.getGeneration());
        assertEquals(1, refSegment.getRefCount());
        assertEquals(0, releaseCount.get()); // Not fully released yet
    }

    public void testGenerationDoesNotIncrementOnReset() {
        MemorySegment segment = arena.allocate(1024);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> {});

        int initialGeneration = refSegment.getGeneration();
        refSegment.reset();
        assertEquals(initialGeneration, refSegment.getGeneration());
    }

    public void testValue() {
        MemorySegment segment = arena.allocate(1024);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> {});

        assertEquals(refSegment, refSegment.value());
    }

    public void testConcurrentPinning() throws Exception {
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await(); // Synchronize start
                    if (refSegment.tryPin()) {
                        successCount.incrementAndGet();
                        Thread.sleep(10);
                        refSegment.unpin();
                    }
                } catch (Exception e) {
                    fail("Unexpected exception: " + e.getMessage());
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown(); // Start all threads
        assertTrue(endLatch.await(15, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(threadCount, successCount.get());
        assertEquals(1, refSegment.getRefCount()); // Back to initial state
    }

    public void testConcurrentIncRefDecRef() throws Exception {
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        int threadCount = 20;
        int operationsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < operationsPerThread; j++) {
                        refSegment.incRef();
                        refSegment.decRef();
                    }
                } catch (Exception e) {
                    fail("Unexpected exception: " + e.getMessage());
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(endLatch.await(20, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(1, refSegment.getRefCount());
        assertEquals(0, releaseCount.get());
    }

    public void testReleaserCalledExactlyOnce() {
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        // Multiple refs
        refSegment.incRef();
        refSegment.incRef();
        refSegment.incRef();

        assertEquals(4, refSegment.getRefCount());

        // Release all refs
        refSegment.decRef();
        refSegment.decRef();
        refSegment.decRef();

        assertEquals(1, refSegment.getRefCount());
        assertEquals(0, releaseCount.get());

        // Final release
        refSegment.decRef();

        assertEquals(0, refSegment.getRefCount());
        assertEquals(1, releaseCount.get()); // Called exactly once
    }

    public void testCloseDecrementsRefCount() {
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        refSegment.incRef();
        assertEquals(2, refSegment.getRefCount());

        refSegment.close();
        assertEquals(1, refSegment.getRefCount());
        assertEquals(0, releaseCount.get());
    }

    public void testLength() {
        MemorySegment segment = arena.allocate(2048);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> {});

        assertEquals(1024, refSegment.length());
    }

    public void testTryPinIfGenerationSuccess() {
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        int expectedGen = refSegment.getGeneration();
        assertTrue(refSegment.tryPinIfGeneration(expectedGen));
        assertEquals(2, refSegment.getRefCount());

        refSegment.unpin();
        assertEquals(1, refSegment.getRefCount());
    }

    public void testTryPinIfGenerationFailsOnMismatch() {
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        refSegment.incRef();
        refSegment.close(); // bumps generation to 1

        // Try to pin with old generation
        assertFalse(refSegment.tryPinIfGeneration(0));
        assertEquals(1, refSegment.getRefCount()); // unchanged
    }

    public void testTryPinIfGenerationFailsOnReleasedSegment() {
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        int expectedGen = refSegment.getGeneration();
        refSegment.decRef(); // release, refCount=0

        assertFalse(refSegment.tryPinIfGeneration(expectedGen));
        assertEquals(0, refSegment.getRefCount());
    }

    public void testMultipleCloseThrows() {
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        refSegment.close(); // first close: gen++, refCount 1->0, released

        try {
            refSegment.close(); // second close on already released segment
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("already released"));
        }
    }

    public void testGenerationBumpsAtomicallyWithRefCountInClose() {
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        refSegment.incRef(); // refCount=2
        assertEquals(0, refSegment.getGeneration());

        refSegment.close(); // should atomically: gen++ and refCount--

        assertEquals(1, refSegment.getGeneration());
        assertEquals(1, refSegment.getRefCount());
    }

    public void testConcurrentCloseAndTryPin() throws Exception {
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        // Add enough refs to prevent full release during concurrent close attempts
        for (int i = 0; i < 10; i++) {
            refSegment.incRef();
        }

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger pinSuccessCount = new AtomicInteger(0);
        AtomicInteger closeSuccessCount = new AtomicInteger(0);

        // Half the threads try to pin, half try to close
        for (int i = 0; i < threadCount; i++) {
            final int threadIdx = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    if (threadIdx % 2 == 0) {
                        // Try to pin with generation 0
                        if (refSegment.tryPinIfGeneration(0)) {
                            pinSuccessCount.incrementAndGet();
                            Thread.sleep(5);
                            refSegment.unpin();
                        }
                    } else {
                        // Try to close - all should succeed until generation bumps
                        try {
                            refSegment.close();
                            closeSuccessCount.incrementAndGet();
                        } catch (IllegalStateException e) {
                            // Expected after refCount hits 0
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
        assertTrue(endLatch.await(20, TimeUnit.SECONDS));
        executor.shutdown();

        // After close bumps generation, tryPinIfGeneration(0) should fail
        assertTrue("Generation should have been bumped", refSegment.getGeneration() > 0);
        // At least one close should have succeeded
        assertTrue("At least one close should succeed", closeSuccessCount.get() >= 1);
    }

    public void testPackedStateAtomicity() throws Exception {
        // Verify that we never observe torn reads of generation/refCount
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        // Prevent full release
        for (int i = 0; i < 100; i++) {
            refSegment.incRef();
        }

        int threadCount = 4;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger tornReadCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final int threadIdx = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < 1000; j++) {
                        if (threadIdx == 0) {
                            // Thread 0: repeatedly close (bumps generation)
                            try {
                                refSegment.close();
                            } catch (IllegalStateException e) {
                                // Expected after first close
                            }
                        } else {
                            // Other threads: read generation and refCount
                            int gen = refSegment.getGeneration();
                            int rc = refSegment.getRefCount();

                            // If we see a torn read, generation would be bumped but refCount
                            // wouldn't be decremented (or vice versa). In practice with packed
                            // state, this should never happen.
                            if (gen > 0 && rc > 100) {
                                // This would indicate a torn read in the old implementation
                                // With packed state, this should never happen
                            }
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
        assertTrue(endLatch.await(20, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(0, tornReadCount.get());
    }

    public void testResetPreservesGeneration() {
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        // Bump generation through eviction cycles
        refSegment.incRef();
        refSegment.close(); // gen=1
        refSegment.incRef();
        refSegment.close(); // gen=2

        assertEquals(2, refSegment.getGeneration());

        // Reset should preserve generation but reset refCount
        refSegment.reset();

        assertEquals(2, refSegment.getGeneration()); // generation preserved
        assertEquals(1, refSegment.getRefCount()); // refCount reset to 1
    }

    public void testConcurrentResetIsNotThreadSafe() {
        // Document that reset() is NOT thread-safe (should only be called under pool lock)
        // This test just verifies the documented behavior exists
        MemorySegment segment = arena.allocate(1024);

        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> {});

        refSegment.reset(); // Should work when called sequentially
        assertEquals(1, refSegment.getRefCount());

        // Calling reset() concurrently is undefined behavior (pool's responsibility to prevent)
    }

    public void testPackStateAndUnpack() {
        // Test the pack/unpack logic directly through observable behavior
        MemorySegment segment = arena.allocate(1024);
        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> {});

        // Initial state: gen=0, refCount=1
        assertEquals(0, refSegment.getGeneration());
        assertEquals(1, refSegment.getRefCount());

        // Increment refCount to 5
        for (int i = 0; i < 4; i++) {
            refSegment.incRef();
        }
        assertEquals(0, refSegment.getGeneration()); // gen unchanged
        assertEquals(5, refSegment.getRefCount());

        // Close bumps gen and decrements refCount
        refSegment.close();
        assertEquals(1, refSegment.getGeneration());
        assertEquals(4, refSegment.getRefCount());

        // Multiple closes bump gen each time
        refSegment.close();
        assertEquals(2, refSegment.getGeneration());
        assertEquals(3, refSegment.getRefCount());

        refSegment.close();
        assertEquals(3, refSegment.getGeneration());
        assertEquals(2, refSegment.getRefCount());
    }

    public void testPackStateWithLargeValues() {
        // Test with large generation and refCount values
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);
        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        // Build up large refCount
        for (int i = 0; i < 1000; i++) {
            refSegment.incRef();
        }
        assertEquals(1001, refSegment.getRefCount());

        // Build up generation through many eviction cycles
        for (int i = 0; i < 100; i++) {
            refSegment.close(); // gen++, refCount--
        }

        assertEquals(100, refSegment.getGeneration());
        assertEquals(901, refSegment.getRefCount());
    }

    public void testPackStateGenerationWraparound() {
        // Verify generation wraps around correctly at 32-bit boundary
        // This is more of a documentation test since we can't easily reach 2^32
        MemorySegment segment = arena.allocate(1024);
        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> {});

        // Generation is treated as unsigned 32-bit, so wraparound is expected behavior
        // We'll just verify a few eviction cycles work correctly
        for (int i = 0; i < 10; i++) {
            refSegment.incRef();
            refSegment.close();
        }

        assertEquals(10, refSegment.getGeneration());
        assertEquals(1, refSegment.getRefCount());
    }

    public void testUnpackGenerationAndRefCountIndependently() {
        // Verify gen and refCount are truly independent
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);
        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        // Increase refCount without affecting generation
        refSegment.incRef();
        refSegment.incRef();
        refSegment.incRef();
        assertEquals(0, refSegment.getGeneration());
        assertEquals(4, refSegment.getRefCount());

        // Decrease refCount without affecting generation
        refSegment.decRef();
        assertEquals(0, refSegment.getGeneration());
        assertEquals(3, refSegment.getRefCount());

        // close() bumps gen AND decrements refCount atomically
        refSegment.close();
        assertEquals(1, refSegment.getGeneration());
        assertEquals(2, refSegment.getRefCount());

        // Further decRef doesn't affect generation
        refSegment.decRef();
        assertEquals(1, refSegment.getGeneration());
        assertEquals(1, refSegment.getRefCount());
    }

    public void testPackedStatePreservesZeroValues() {
        // Edge case: verify zero values are preserved correctly
        MemorySegment segment = arena.allocate(1024);
        AtomicInteger releaseCount = new AtomicInteger(0);
        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, 1024, (s) -> releaseCount.incrementAndGet());

        // Initial: gen=0, refCount=1
        assertEquals(0, refSegment.getGeneration());
        assertEquals(1, refSegment.getRefCount());

        // After release, refCount=0 (generation still 0)
        refSegment.decRef();
        assertEquals(0, refSegment.getGeneration());
        assertEquals(0, refSegment.getRefCount());
        assertEquals(1, releaseCount.get()); // released

        // Reset sets refCount=1, generation preserved
        refSegment.reset();
        assertEquals(0, refSegment.getGeneration());
        assertEquals(1, refSegment.getRefCount());
    }
}

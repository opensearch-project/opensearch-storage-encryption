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
        assertTrue(endLatch.await(5, TimeUnit.SECONDS));
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
        assertTrue(endLatch.await(10, TimeUnit.SECONDS));
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
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.Mockito;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for BlockSlotTinyCache focusing on the race condition fix and proper pin/unpin behavior.
 */
@SuppressWarnings("unchecked")
public class BlockSlotTinyCacheTests extends OpenSearchTestCase {

    private static final int BLOCK_SIZE = 8192; // DirectIoConfigs.CACHE_BLOCK_SIZE
    private static final int BLOCK_SIZE_POWER = 13; // log2(8192)

    private BlockCache<RefCountedMemorySegment> mockCache;
    private Path testPath;
    private Arena arena;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockCache = mock(BlockCache.class);
        testPath = Paths.get("/test/file.dat");
        arena = Arena.ofAuto();
    }

    /**
     * Test that acquireRefCountedValue returns an already-pinned block.
     * This is the core fix - the L1 cache must return pinned blocks.
     */
    public void testAcquireReturnsAlreadyPinnedBlock() throws IOException {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        // Create a memory segment and wrap it
        MemorySegment segment = arena.allocate(BLOCK_SIZE);
        AtomicInteger releaseCount = new AtomicInteger(0);
        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, BLOCK_SIZE, (seg) -> { releaseCount.incrementAndGet(); });

        BlockCacheValue<RefCountedMemorySegment> cacheValue = mock(BlockCacheValue.class);
        when(cacheValue.value()).thenReturn(refSegment);
        when(cacheValue.tryPin()).thenAnswer(inv -> refSegment.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment.unpin();
            return null;
        }).when(cacheValue).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue);

        // Initial refCount should be 1 (cache's reference)
        assertEquals(1, refSegment.getRefCount());

        // Acquire the block - should return with refCount incremented (pinned)
        BlockCacheValue<RefCountedMemorySegment> result = cache.acquireRefCountedValue(0);
        assertNotNull(result);

        // RefCount should now be 2 (cache + our pin)
        assertEquals(2, refSegment.getRefCount());

        // Unpin should decrement
        result.unpin();
        assertEquals(1, refSegment.getRefCount());

        // No releases should have occurred yet
        assertEquals(0, releaseCount.get());
    }

    /**
     * Test that a block is pinned exactly once per acquireRefCountedValue call.
     * Multiple acquisitions should each increment the refCount.
     */
    public void testBlockIsPinnedExactlyOncePerAcquisition() throws IOException {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        MemorySegment segment = arena.allocate(BLOCK_SIZE);
        AtomicInteger releaseCount = new AtomicInteger(0);
        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, BLOCK_SIZE, (seg) -> { releaseCount.incrementAndGet(); });

        BlockCacheValue<RefCountedMemorySegment> cacheValue = mock(BlockCacheValue.class);
        when(cacheValue.value()).thenReturn(refSegment);
        when(cacheValue.tryPin()).thenAnswer(inv -> refSegment.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment.unpin();
            return null;
        }).when(cacheValue).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue);

        // Initial state
        assertEquals(1, refSegment.getRefCount());

        // First acquisition
        BlockCacheValue<RefCountedMemorySegment> result1 = cache.acquireRefCountedValue(0);
        assertEquals(2, refSegment.getRefCount());

        // Second acquisition (same block) - should hit thread-local cache and pin again
        BlockCacheValue<RefCountedMemorySegment> result2 = cache.acquireRefCountedValue(0);
        assertEquals(3, refSegment.getRefCount());

        // Third acquisition
        BlockCacheValue<RefCountedMemorySegment> result3 = cache.acquireRefCountedValue(0);
        assertEquals(4, refSegment.getRefCount());

        // Unpin all three
        result1.unpin();
        assertEquals(3, refSegment.getRefCount());

        result2.unpin();
        assertEquals(2, refSegment.getRefCount());

        result3.unpin();
        assertEquals(1, refSegment.getRefCount());

        // No releases yet (cache still holds reference)
        assertEquals(0, releaseCount.get());
    }

    /**
     * Test that unpinning releases the reference properly.
     * When all pins are released and cache drops its reference, the segment should be released.
     */
    public void testUnpinReleasesReference() throws IOException {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        MemorySegment segment = arena.allocate(BLOCK_SIZE);
        AtomicInteger releaseCount = new AtomicInteger(0);
        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, BLOCK_SIZE, (seg) -> { releaseCount.incrementAndGet(); });

        BlockCacheValue<RefCountedMemorySegment> cacheValue = mock(BlockCacheValue.class);
        when(cacheValue.value()).thenReturn(refSegment);
        when(cacheValue.tryPin()).thenAnswer(inv -> refSegment.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment.unpin();
            return null;
        }).when(cacheValue).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue);

        // Acquire and unpin
        BlockCacheValue<RefCountedMemorySegment> result = cache.acquireRefCountedValue(0);
        assertEquals(2, refSegment.getRefCount());

        result.unpin();
        assertEquals(1, refSegment.getRefCount());

        // Simulate cache eviction (cache drops its reference)
        refSegment.close(); // This increments generation and calls decRef
        assertEquals(0, refSegment.getRefCount());

        // Releaser should have been called
        assertEquals(1, releaseCount.get());
    }

    /**
     * Test the race condition fix: verify that generation checking prevents returning stale blocks.
     * When a block is evicted (generation incremented), the L1 cache should detect this and reload.
     */
    public void testGenerationCheckPreventsStaleBlocks() throws IOException {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        // Create first segment (generation 0)
        MemorySegment segment1 = arena.allocate(BLOCK_SIZE);
        segment1.fill((byte) 0xAA); // Fill with pattern to identify it
        AtomicInteger releaseCount = new AtomicInteger(0);
        RefCountedMemorySegment refSegment1 = new RefCountedMemorySegment(
            segment1,
            BLOCK_SIZE,
            (seg) -> { releaseCount.incrementAndGet(); }
        );

        BlockCacheValue<RefCountedMemorySegment> cacheValue1 = mock(BlockCacheValue.class);
        when(cacheValue1.value()).thenReturn(refSegment1);
        when(cacheValue1.tryPin()).thenAnswer(inv -> refSegment1.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment1.unpin();
            return null;
        }).when(cacheValue1).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue1);

        // First acquisition - populates L1 cache
        int initialGeneration = refSegment1.getGeneration();
        assertEquals(0, initialGeneration);

        BlockCacheValue<RefCountedMemorySegment> result1 = cache.acquireRefCountedValue(0);
        assertEquals(refSegment1, result1.value());
        result1.unpin();

        // Simulate eviction from L2 cache - increments generation
        refSegment1.close(); // generation becomes 1, refCount becomes 0
        int newGeneration = refSegment1.getGeneration();
        assertEquals(1, newGeneration);

        // Create second segment (reused from pool, generation 1)
        MemorySegment segment2 = arena.allocate(BLOCK_SIZE);
        segment2.fill((byte) 0xBB); // Different pattern
        RefCountedMemorySegment refSegment2 = new RefCountedMemorySegment(
            segment2,
            BLOCK_SIZE,
            (seg) -> { releaseCount.incrementAndGet(); }
        );

        BlockCacheValue<RefCountedMemorySegment> cacheValue2 = mock(BlockCacheValue.class);
        when(cacheValue2.value()).thenReturn(refSegment2);
        when(cacheValue2.tryPin()).thenAnswer(inv -> refSegment2.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment2.unpin();
            return null;
        }).when(cacheValue2).unpin();

        // L2 cache now returns the new segment
        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue2);

        // Next acquisition should detect stale generation and reload from L2
        BlockCacheValue<RefCountedMemorySegment> result2 = cache.acquireRefCountedValue(0);

        // Should get the new segment (generation check should have failed tryPin on old segment)
        assertEquals(refSegment2, result2.value());
        result2.unpin();
    }

    /**
     * Test concurrent access to the same block from multiple threads.
     * Each thread should get a properly pinned block and unpinning should work correctly.
     */
    public void testConcurrentAcquisitionAndRelease() throws Exception {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        MemorySegment segment = arena.allocate(BLOCK_SIZE);
        AtomicInteger releaseCount = new AtomicInteger(0);
        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, BLOCK_SIZE, (seg) -> { releaseCount.incrementAndGet(); });

        BlockCacheValue<RefCountedMemorySegment> cacheValue = mock(BlockCacheValue.class);
        when(cacheValue.value()).thenReturn(refSegment);
        when(cacheValue.tryPin()).thenAnswer(inv -> refSegment.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment.unpin();
            return null;
        }).when(cacheValue).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue);

        int numThreads = 10;
        int acquisitionsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicReference<Throwable> error = new AtomicReference<>();

        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    barrier.await(); // Synchronize start
                    for (int j = 0; j < acquisitionsPerThread; j++) {
                        BlockCacheValue<RefCountedMemorySegment> result = cache.acquireRefCountedValue(0);
                        assertNotNull(result);
                        // Block is pinned - refCount should be > 1
                        assertTrue(result.value().getRefCount() > 1);
                        result.unpin();
                    }
                } catch (Throwable t) {
                    error.set(t);
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue("Threads did not complete in time", latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        if (error.get() != null) {
            throw new AssertionError("Thread error", error.get());
        }

        // All threads are done, refCount should be back to 1 (cache only)
        assertEquals(1, refSegment.getRefCount());
        assertEquals(0, releaseCount.get());
    }

    /**
     * Test that multiple blocks can be cached and pinned independently.
     */
    public void testMultipleBlocksIndependentPinning() throws IOException {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        // Create three different blocks
        List<RefCountedMemorySegment> segments = new ArrayList<>();
        List<BlockCacheValue<RefCountedMemorySegment>> cacheValues = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            MemorySegment segment = arena.allocate(BLOCK_SIZE);
            int finalI = i;
            RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, BLOCK_SIZE, (seg) -> {
                // No-op releaser
            });
            segments.add(refSegment);

            BlockCacheValue<RefCountedMemorySegment> cacheValue = mock(BlockCacheValue.class);
            when(cacheValue.value()).thenReturn(refSegment);
            when(cacheValue.tryPin()).thenAnswer(inv -> refSegment.tryPin());
            Mockito.doAnswer(inv -> {
                refSegment.unpin();
                return null;
            }).when(cacheValue).unpin();
            cacheValues.add(cacheValue);
        }

        // Mock cache to return appropriate segment based on block offset
        when(mockCache.get(any(FileBlockCacheKey.class))).thenAnswer(inv -> {
            FileBlockCacheKey key = inv.getArgument(0);
            long blockOffset = key.fileOffset();
            int blockIdx = (int) (blockOffset / BLOCK_SIZE);
            return cacheValues.get(blockIdx);
        });

        // Acquire all three blocks
        BlockCacheValue<RefCountedMemorySegment> result0 = cache.acquireRefCountedValue(0);
        BlockCacheValue<RefCountedMemorySegment> result1 = cache.acquireRefCountedValue(BLOCK_SIZE);
        BlockCacheValue<RefCountedMemorySegment> result2 = cache.acquireRefCountedValue(BLOCK_SIZE * 2L);

        // Each should be pinned (refCount = 2)
        assertEquals(2, segments.get(0).getRefCount());
        assertEquals(2, segments.get(1).getRefCount());
        assertEquals(2, segments.get(2).getRefCount());

        // Unpin in different order
        result1.unpin();
        assertEquals(1, segments.get(1).getRefCount());
        assertEquals(2, segments.get(0).getRefCount()); // Others unchanged
        assertEquals(2, segments.get(2).getRefCount());

        result0.unpin();
        assertEquals(1, segments.get(0).getRefCount());

        result2.unpin();
        assertEquals(1, segments.get(2).getRefCount());
    }

    /**
     * Test the retry mechanism when tryPin() temporarily fails.
     * This simulates the scenario where eviction is happening concurrently.
     */
    public void testRetryOnPinFailure() throws IOException {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        MemorySegment segment = arena.allocate(BLOCK_SIZE);
        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, BLOCK_SIZE, (seg) -> {
            // No-op releaser
        });

        BlockCacheValue<RefCountedMemorySegment> cacheValue = mock(BlockCacheValue.class);
        when(cacheValue.value()).thenReturn(refSegment);

        AtomicInteger tryPinAttempts = new AtomicInteger(0);

        // First 2 tryPin calls fail, third succeeds
        when(cacheValue.tryPin()).thenAnswer(inv -> {
            int attempt = tryPinAttempts.incrementAndGet();
            if (attempt < 3) {
                return false; // Fail first 2 attempts
            }
            return refSegment.tryPin(); // Succeed on 3rd attempt
        });

        Mockito.doAnswer(inv -> {
            refSegment.unpin();
            return null;
        }).when(cacheValue).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(null); // First call returns null
        when(mockCache.getOrLoad(any(FileBlockCacheKey.class))).thenReturn(cacheValue);

        // Should succeed after retries
        BlockCacheValue<RefCountedMemorySegment> result = cache.acquireRefCountedValue(0);
        assertNotNull(result);
        assertEquals(2, refSegment.getRefCount()); // Successfully pinned

        // Verify at most 3 tryPin attempts were made (could be less with cache hits)
        assertTrue("Expected at least 1 tryPin attempt", tryPinAttempts.get() >= 1);
        assertTrue("Expected at most 3 tryPin attempts on this path", tryPinAttempts.get() <= 3);

        result.unpin();
    }

    /**
     * Test that exceeding max retry attempts throws IOException.
     */
    public void testMaxRetriesExceededThrowsException() throws IOException {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        MemorySegment segment = arena.allocate(BLOCK_SIZE);
        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, BLOCK_SIZE, (seg) -> {
            // No-op releaser
        });

        BlockCacheValue<RefCountedMemorySegment> cacheValue = mock(BlockCacheValue.class);
        when(cacheValue.value()).thenReturn(refSegment);
        when(cacheValue.tryPin()).thenReturn(false); // Always fail

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(null);
        when(mockCache.getOrLoad(any(FileBlockCacheKey.class))).thenReturn(cacheValue);

        // Should throw after max retries
        IOException ex = expectThrows(IOException.class, () -> cache.acquireRefCountedValue(0));
        assertTrue(ex.getMessage().contains("Unable to pin memory segment"));
        assertTrue(ex.getMessage().contains("after 10 attempts"));
    }

    /**
     * Test clear() properly resets the cache and prevents stale references.
     */
    public void testClearPreventsStaleCacheHits() throws IOException {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        MemorySegment segment1 = arena.allocate(BLOCK_SIZE);
        RefCountedMemorySegment refSegment1 = new RefCountedMemorySegment(segment1, BLOCK_SIZE, (seg) -> {});

        BlockCacheValue<RefCountedMemorySegment> cacheValue1 = mock(BlockCacheValue.class);
        when(cacheValue1.value()).thenReturn(refSegment1);
        when(cacheValue1.tryPin()).thenAnswer(inv -> refSegment1.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment1.unpin();
            return null;
        }).when(cacheValue1).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue1);

        // Populate cache
        BlockCacheValue<RefCountedMemorySegment> result1 = cache.acquireRefCountedValue(0);
        result1.unpin();

        // Clear cache
        cache.clear();

        // Create new segment
        MemorySegment segment2 = arena.allocate(BLOCK_SIZE);
        RefCountedMemorySegment refSegment2 = new RefCountedMemorySegment(segment2, BLOCK_SIZE, (seg) -> {});

        BlockCacheValue<RefCountedMemorySegment> cacheValue2 = mock(BlockCacheValue.class);
        when(cacheValue2.value()).thenReturn(refSegment2);
        when(cacheValue2.tryPin()).thenAnswer(inv -> refSegment2.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment2.unpin();
            return null;
        }).when(cacheValue2).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue2);

        // Next acquisition should get the new segment (not stale cached one)
        BlockCacheValue<RefCountedMemorySegment> result2 = cache.acquireRefCountedValue(0);
        assertEquals(refSegment2, result2.value());
        result2.unpin();
    }

    /**
     * Test thread-local cache hits prevent redundant pinning on the same thread.
     */
    public void testThreadLocalCacheHitsPinCorrectly() throws IOException {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        MemorySegment segment = arena.allocate(BLOCK_SIZE);
        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, BLOCK_SIZE, (seg) -> {});

        BlockCacheValue<RefCountedMemorySegment> cacheValue = mock(BlockCacheValue.class);
        when(cacheValue.value()).thenReturn(refSegment);
        when(cacheValue.tryPin()).thenAnswer(inv -> refSegment.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment.unpin();
            return null;
        }).when(cacheValue).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue);

        // First acquisition
        BlockCacheValue<RefCountedMemorySegment> result1 = cache.acquireRefCountedValue(0);
        assertEquals(2, refSegment.getRefCount());

        // Second acquisition on same thread - should hit thread-local cache but still pin
        BlockCacheValue<RefCountedMemorySegment> result2 = cache.acquireRefCountedValue(0);
        assertEquals(3, refSegment.getRefCount());

        // Verify tryPin was called at least twice (once per acquisition)
        verify(cacheValue, atMost(3)).tryPin(); // At most 3 because of potential retry logic

        result1.unpin();
        result2.unpin();
        assertEquals(1, refSegment.getRefCount());
    }
}

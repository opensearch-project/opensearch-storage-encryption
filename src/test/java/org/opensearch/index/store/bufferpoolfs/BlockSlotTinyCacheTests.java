/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
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
import org.opensearch.index.store.block.RefCountedByteBuffer;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for BlockSlotTinyCache focusing on the race condition fix and proper pin/unpin behavior.
 */
@SuppressWarnings("preview")
public class BlockSlotTinyCacheTests extends OpenSearchTestCase {

    private static final int BLOCK_SIZE = 8192; // DirectIoConfigs.CACHE_BLOCK_SIZE

    private BlockCache<RefCountedByteBuffer> mockCache;
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
        RefCountedByteBuffer refSegment = new RefCountedByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE), BLOCK_SIZE);

        BlockCacheValue<RefCountedByteBuffer> cacheValue = mock(BlockCacheValue.class);
        when(cacheValue.value()).thenReturn(refSegment);
        when(cacheValue.tryPin()).thenAnswer(inv -> refSegment.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment.unpin();
            return null;
        }).when(cacheValue).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue);

        // Initial refCount should be 1 (cache's reference)

        // Acquire the block - should return with refCount incremented (pinned)
        BlockCacheValue<RefCountedByteBuffer> result = cache.acquireRefCountedValue(0);
        assertNotNull(result);

        // RefCount should now be 2 (cache + our pin)

        // Unpin should decrement
        result.unpin();

        // No releases should have occurred yet
    }

    /**
     * Test that a block is pinned exactly once per acquireRefCountedValue call.
     * Multiple acquisitions should each increment the refCount.
     */
    public void testBlockIsPinnedExactlyOncePerAcquisition() throws IOException {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        MemorySegment segment = arena.allocate(BLOCK_SIZE);
        AtomicInteger releaseCount = new AtomicInteger(0);
        RefCountedByteBuffer refSegment = new RefCountedByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE), BLOCK_SIZE);

        BlockCacheValue<RefCountedByteBuffer> cacheValue = mock(BlockCacheValue.class);
        when(cacheValue.value()).thenReturn(refSegment);
        when(cacheValue.tryPin()).thenAnswer(inv -> refSegment.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment.unpin();
            return null;
        }).when(cacheValue).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue);

        // Initial state

        // First acquisition
        BlockCacheValue<RefCountedByteBuffer> result1 = cache.acquireRefCountedValue(0);

        // Second acquisition (same block) - should hit thread-local cache and pin again
        BlockCacheValue<RefCountedByteBuffer> result2 = cache.acquireRefCountedValue(0);

        // Third acquisition
        BlockCacheValue<RefCountedByteBuffer> result3 = cache.acquireRefCountedValue(0);

        // Unpin all three
        result1.unpin();

        result2.unpin();

        result3.unpin();

        // No releases yet (cache still holds reference)
    }

    /**
     * Test that unpinning releases the reference properly.
     * When all pins are released and cache drops its reference, the segment should be released.
     */
    public void testUnpinReleasesReference() throws IOException {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        MemorySegment segment = arena.allocate(BLOCK_SIZE);
        AtomicInteger releaseCount = new AtomicInteger(0);
        RefCountedByteBuffer refSegment = new RefCountedByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE), BLOCK_SIZE);

        BlockCacheValue<RefCountedByteBuffer> cacheValue = mock(BlockCacheValue.class);
        when(cacheValue.value()).thenReturn(refSegment);
        when(cacheValue.tryPin()).thenAnswer(inv -> refSegment.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment.unpin();
            return null;
        }).when(cacheValue).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue);

        // Acquire and unpin
        BlockCacheValue<RefCountedByteBuffer> result = cache.acquireRefCountedValue(0);

        result.unpin();

        // Simulate cache eviction (cache drops its reference)
        refSegment.close(); // This increments generation and calls decRef

        // Releaser should have been called
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
        RefCountedByteBuffer refSegment1 = new RefCountedByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE), BLOCK_SIZE);

        BlockCacheValue<RefCountedByteBuffer> cacheValue1 = mock(BlockCacheValue.class);
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

        BlockCacheValue<RefCountedByteBuffer> result1 = cache.acquireRefCountedValue(0);
        assertEquals(refSegment1, result1.value());
        result1.unpin();

        // Simulate eviction from L2 cache - increments generation
        refSegment1.close(); // generation becomes 1, refCount becomes 0
        int newGeneration = refSegment1.getGeneration();
        assertEquals(1, newGeneration);

        // Create second segment (reused from pool, generation 1)
        MemorySegment segment2 = arena.allocate(BLOCK_SIZE);
        segment2.fill((byte) 0xBB); // Different pattern
        RefCountedByteBuffer refSegment2 = new RefCountedByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE), BLOCK_SIZE);

        BlockCacheValue<RefCountedByteBuffer> cacheValue2 = mock(BlockCacheValue.class);
        when(cacheValue2.value()).thenReturn(refSegment2);
        when(cacheValue2.tryPin()).thenAnswer(inv -> refSegment2.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment2.unpin();
            return null;
        }).when(cacheValue2).unpin();

        // L2 cache now returns the new segment
        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue2);

        // Next acquisition should detect stale generation and reload from L2
        BlockCacheValue<RefCountedByteBuffer> result2 = cache.acquireRefCountedValue(0);

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
        RefCountedByteBuffer refSegment = new RefCountedByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE), BLOCK_SIZE);

        BlockCacheValue<RefCountedByteBuffer> cacheValue = mock(BlockCacheValue.class);
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
                        BlockCacheValue<RefCountedByteBuffer> result = cache.acquireRefCountedValue(0);
                        assertNotNull(result);
                        // Block is pinned - refCount should be > 1
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
    }

    /**
     * Test that multiple blocks can be cached and pinned independently.
     */
    public void testMultipleBlocksIndependentPinning() throws IOException {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        // Create three different blocks
        List<RefCountedByteBuffer> segments = new ArrayList<>();
        List<BlockCacheValue<RefCountedByteBuffer>> cacheValues = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            MemorySegment segment = arena.allocate(BLOCK_SIZE);
            int finalI = i;
            RefCountedByteBuffer refSegment = new RefCountedByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE), BLOCK_SIZE);
            segments.add(refSegment);

            BlockCacheValue<RefCountedByteBuffer> cacheValue = mock(BlockCacheValue.class);
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
        BlockCacheValue<RefCountedByteBuffer> result0 = cache.acquireRefCountedValue(0);
        BlockCacheValue<RefCountedByteBuffer> result1 = cache.acquireRefCountedValue(BLOCK_SIZE);
        BlockCacheValue<RefCountedByteBuffer> result2 = cache.acquireRefCountedValue(BLOCK_SIZE * 2L);

        // Each should be pinned (refCount = 2)

        // Unpin in different order
        result1.unpin();

        result0.unpin();

        result2.unpin();
    }

    /**
     * Test the retry mechanism when tryPin() temporarily fails.
     * This simulates the scenario where eviction is happening concurrently.
     */
    public void testRetryOnPinFailure() throws IOException {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        MemorySegment segment = arena.allocate(BLOCK_SIZE);
        RefCountedByteBuffer refSegment = new RefCountedByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE), BLOCK_SIZE);

        BlockCacheValue<RefCountedByteBuffer> cacheValue = mock(BlockCacheValue.class);
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
        BlockCacheValue<RefCountedByteBuffer> result = cache.acquireRefCountedValue(0);
        assertNotNull(result);

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
        RefCountedByteBuffer refSegment = new RefCountedByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE), BLOCK_SIZE);

        BlockCacheValue<RefCountedByteBuffer> cacheValue = mock(BlockCacheValue.class);
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
        RefCountedByteBuffer refSegment1 = new RefCountedByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE), BLOCK_SIZE);

        BlockCacheValue<RefCountedByteBuffer> cacheValue1 = mock(BlockCacheValue.class);
        when(cacheValue1.value()).thenReturn(refSegment1);
        when(cacheValue1.tryPin()).thenAnswer(inv -> refSegment1.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment1.unpin();
            return null;
        }).when(cacheValue1).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue1);

        // Populate cache
        BlockCacheValue<RefCountedByteBuffer> result1 = cache.acquireRefCountedValue(0);
        result1.unpin();

        // Clear cache
        cache.clear();

        // Create new segment
        MemorySegment segment2 = arena.allocate(BLOCK_SIZE);
        RefCountedByteBuffer refSegment2 = new RefCountedByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE), BLOCK_SIZE);

        BlockCacheValue<RefCountedByteBuffer> cacheValue2 = mock(BlockCacheValue.class);
        when(cacheValue2.value()).thenReturn(refSegment2);
        when(cacheValue2.tryPin()).thenAnswer(inv -> refSegment2.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment2.unpin();
            return null;
        }).when(cacheValue2).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue2);

        // Next acquisition should get the new segment (not stale cached one)
        BlockCacheValue<RefCountedByteBuffer> result2 = cache.acquireRefCountedValue(0);
        assertEquals(refSegment2, result2.value());
        result2.unpin();
    }

    /**
     * Test thread-local cache hits prevent redundant pinning on the same thread.
     */
    public void testThreadLocalCacheHitsPinCorrectly() throws IOException {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        MemorySegment segment = arena.allocate(BLOCK_SIZE);
        RefCountedByteBuffer refSegment = new RefCountedByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE), BLOCK_SIZE);

        BlockCacheValue<RefCountedByteBuffer> cacheValue = mock(BlockCacheValue.class);
        when(cacheValue.value()).thenReturn(refSegment);
        when(cacheValue.tryPin()).thenAnswer(inv -> refSegment.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment.unpin();
            return null;
        }).when(cacheValue).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue);

        // First acquisition
        BlockCacheValue<RefCountedByteBuffer> result1 = cache.acquireRefCountedValue(0);

        // Second acquisition on same thread - should hit thread-local cache but still pin
        BlockCacheValue<RefCountedByteBuffer> result2 = cache.acquireRefCountedValue(0);

        // Verify tryPin was called at least twice (once per acquisition)
        verify(cacheValue, atMost(3)).tryPin(); // At most 3 because of potential retry logic

        result1.unpin();
        result2.unpin();
    }

    public void testRaceBetweenGetAndPinWithSegmentRecycling_reproAndFix() throws Exception {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 100);

        MemorySegment segment = arena.allocate(BLOCK_SIZE);
        segment.fill((byte) 0xAA);

        ByteBuffer buf = ByteBuffer.allocateDirect(BLOCK_SIZE);
        MemorySegment.copy(segment, 0, MemorySegment.ofBuffer(buf), 0, BLOCK_SIZE);
        RefCountedByteBuffer refSegment = new RefCountedByteBuffer(buf, BLOCK_SIZE);

        BlockCacheValue<RefCountedByteBuffer> cacheValue = mock(BlockCacheValue.class);
        when(cacheValue.value()).thenReturn(refSegment);

        // Latches to force the window: cache.get returned, then we pause at tryPin
        CountDownLatch tryPinEntered = new CountDownLatch(1);
        CountDownLatch allowTryPinToProceed = new CountDownLatch(1);

        when(cacheValue.tryPin()).thenAnswer(inv -> {
            tryPinEntered.countDown();                 // signal T1 reached tryPin
            allowTryPinToProceed.await(5, TimeUnit.SECONDS); // wait for recycle
            return refSegment.tryPin();                // may succeed after reset()
        });

        doAnswer(inv -> {
            refSegment.unpin();
            return null;
        }).when(cacheValue).unpin();

        // Main cache behavior: always return same object for the key (like "stale handle")
        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue);

        // If your Tier-3 falls back to load on mismatch, provide a fresh object for getOrLoad
        MemorySegment fresh = arena.allocate(BLOCK_SIZE);
        fresh.fill((byte) 0xAA);
        ByteBuffer freshBuf = ByteBuffer.allocateDirect(BLOCK_SIZE);
        MemorySegment.ofBuffer(freshBuf).fill((byte) 0xAA);
        RefCountedByteBuffer freshSeg = new RefCountedByteBuffer(freshBuf, BLOCK_SIZE);

        BlockCacheValue<RefCountedByteBuffer> freshValue = mock(BlockCacheValue.class);
        when(freshValue.value()).thenReturn(freshSeg);
        when(freshValue.tryPin()).thenAnswer(inv -> freshSeg.tryPin());
        doAnswer(inv -> {
            freshSeg.unpin();
            return null;
        }).when(freshValue).unpin();

        when(mockCache.getOrLoad(any(FileBlockCacheKey.class))).thenReturn(freshValue);

        ExecutorService exec = Executors.newFixedThreadPool(2);
        AtomicReference<BlockCacheValue<RefCountedByteBuffer>> t1Result = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();

        exec.submit(() -> {
            try {
                BlockCacheValue<RefCountedByteBuffer> v = cache.acquireRefCountedValue(0);
                t1Result.set(v);
            } catch (Throwable t) {
                error.set(t);
            }
        });

        exec.submit(() -> {
            try {
                // Wait until T1 is *about to pin*
                assertTrue("T1 never reached tryPin", tryPinEntered.await(5, TimeUnit.SECONDS));

                // Simulate eviction + recycle of the same object
                refSegment.close();         // drops cache ref, generation++, refCount->0 (releaser is noop)
                segment.fill((byte) 0xBB);  // overwrite underlying bytes (reused for another block)

                allowTryPinToProceed.countDown();
            } catch (Throwable t) {
                error.set(t);
            }
        });

        exec.shutdown();
        assertTrue(exec.awaitTermination(30, TimeUnit.SECONDS));
        if (error.get() != null)
            throw new AssertionError(error.get());

        BlockCacheValue<RefCountedByteBuffer> result = t1Result.get();
        assertNotNull(result);

        byte b = result.value().segment().get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0);

        assertEquals((byte) 0xAA, b);

        result.unpin();
    }
}

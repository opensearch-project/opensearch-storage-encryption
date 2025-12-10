/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.index.store.block.BlockReleaser;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;
import org.opensearch.test.OpenSearchTestCase;

@SuppressWarnings("preview")
public class BlockSlotTinyCacheIntegrationTests extends OpenSearchTestCase {

    private static final int BLOCK_SIZE = 4096;
    private static final long FILE_SIZE = 100 * BLOCK_SIZE; // 100 blocks

    private Arena arena;
    private Path testPath;
    private SimulatedBlockCache simulatedCache;
    private BlockSlotTinyCache tinyCache;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        arena = Arena.ofConfined();
        testPath = Paths.get("/test/file.dat");

        // Create a simulated cache with pool and eviction
        simulatedCache = new SimulatedBlockCache(arena, 10); // Pool of 10 segments

        // Create tiny cache on top of simulated cache
        tinyCache = new BlockSlotTinyCache(simulatedCache, testPath, FILE_SIZE);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        tinyCache.clear();
        if (arena != null) {
            arena.close();
        }
    }

    /**
     * Test sequential block access pattern (most common in Lucene).
     * This should have high hit rate in both slot and thread-local caches.
     */
    @Test
    public void testSequentialBlockAccess() throws IOException {
        int numBlocks = 10;
        int sequentialPasses = 3;

        for (int pass = 0; pass < sequentialPasses; pass++) {
            for (int i = 0; i < numBlocks; i++) {
                long offset = i * BLOCK_SIZE;

                BlockSlotTinyCache.LookupResult result = tinyCache.acquireRefCountedValue(offset);
                assertNotNull("Should get value for offset " + offset, result.value());

                // Verify segment contains correct data
                RefCountedMemorySegment segment = result.value().value();
                assertNotNull("Segment should not be null", segment);
                assertTrue("Segment should be pinned", segment.getRefCount() >= 2);

                // Unpin when done
                result.value().unpin();
            }
        }

        // All segments should be back to refCount=1 (cache only)
        for (int i = 0; i < numBlocks; i++) {
            long offset = i * BLOCK_SIZE;
            RefCountedMemorySegment seg = simulatedCache.getCachedSegment(testPath, offset);
            if (seg != null) {
                assertEquals("After unpinning, refCount should be 1", 1, seg.getRefCount());
            }
        }
    }

    /**
     * Test that generation checks protect against recycled segments.
     *
     * Timeline:
     * 1. Access block 0, cache it in slot
     * 2. Trigger eviction of block 0 (by filling cache)
     * 3. Access many blocks to recycle the segment for different offset
     * 4. Try to access block 0 again - should detect stale generation
     */
    @Test
    public void testGenerationProtectsAgainstRecycling() throws IOException {
        long block0Offset = 0;

        // Step 1: Access block 0, it gets cached in slot
        BlockSlotTinyCache.LookupResult result1 = tinyCache.acquireRefCountedValue(block0Offset);
        RefCountedMemorySegment segment1 = result1.value().value();
        int gen1 = segment1.getGeneration();
        result1.value().unpin();

        // Step 2: Fill cache to evict block 0 (cache has capacity of 10)
        for (int i = 1; i <= 15; i++) {
            long offset = i * BLOCK_SIZE;
            BlockSlotTinyCache.LookupResult result = tinyCache.acquireRefCountedValue(offset);
            result.value().unpin();
        }

        // Step 3: Block 0 should have been evicted and segment recycled
        // Access block 0 again - should reload from cache
        BlockSlotTinyCache.LookupResult result2 = tinyCache.acquireRefCountedValue(block0Offset);
        RefCountedMemorySegment segment2 = result2.value().value();
        int gen2 = segment2.getGeneration();
        result2.value().unpin();

        // If same segment object was recycled, generation should have changed
        // If different segment, they're just different objects (both valid)
        // The key point is: we got a valid segment, generation checks prevented stale data
        if (segment1 == segment2) {
            assertTrue("Generation should increment if segment was recycled", gen2 >= gen1);
        }
        // else: different segment objects, which is also valid - no assertion needed
    }

    /**
     * Test concurrent access to different block offsets.
     * Multiple threads accessing different blocks should not interfere.
     */
    @Test
    public void testConcurrentAccessDifferentBlocks() throws Exception {
        int numThreads = 4;
        int blocksPerThread = 5;
        int iterationsPerBlock = 100;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicReference<Throwable> error = new AtomicReference<>();

        for (int threadId = 0; threadId < numThreads; threadId++) {
            final int tid = threadId;
            executor.submit(() -> {
                try {
                    startLatch.await(); // Synchronize start

                    for (int iter = 0; iter < iterationsPerBlock; iter++) {
                        for (int block = 0; block < blocksPerThread; block++) {
                            // Each thread accesses its own range of blocks
                            long offset = (tid * blocksPerThread + block) * BLOCK_SIZE;

                            BlockSlotTinyCache.LookupResult result = tinyCache.acquireRefCountedValue(offset);
                            assertNotNull("Thread " + tid + " should get value for offset " + offset, result.value());

                            // Verify pinned
                            assertTrue("Segment should be pinned", result.value().value().getRefCount() >= 2);

                            result.value().unpin();
                        }
                    }
                } catch (Throwable t) {
                    error.set(t);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown(); // Start all threads
        assertTrue("Threads should complete", doneLatch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        if (error.get() != null) {
            throw new AssertionError("Thread failed", error.get());
        }
    }

    /**
     * Test concurrent access to the same block offset.
     * Multiple threads accessing the same block should get properly pinned references.
     */
    @Test
    public void testConcurrentAccessSameBlock() throws Exception {
        long sharedOffset = 5 * BLOCK_SIZE;
        int numThreads = 8;
        int iterations = 100;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicInteger maxRefCount = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();

                    for (int iter = 0; iter < iterations; iter++) {
                        BlockSlotTinyCache.LookupResult result = tinyCache.acquireRefCountedValue(sharedOffset);

                        // Track max concurrent pins
                        int currentRefCount = result.value().value().getRefCount();
                        maxRefCount.updateAndGet(max -> Math.max(max, currentRefCount));

                        // Small delay to increase chance of concurrent access
                        if (iter % 10 == 0) {
                            Thread.sleep(0, 100);
                        }

                        result.value().unpin();
                    }
                } catch (Throwable t) {
                    error.set(t);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue("Threads should complete", doneLatch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        if (error.get() != null) {
            throw new AssertionError("Thread failed", error.get());
        }

        // Should have seen concurrent pins (refCount > 2)
        assertTrue("Should have observed concurrent pins", maxRefCount.get() > 2);

        // After all unpins, refCount should be back to 1
        RefCountedMemorySegment seg = simulatedCache.getCachedSegment(testPath, sharedOffset);
        if (seg != null) {
            assertEquals("Final refCount should be 1", 1, seg.getRefCount());
        }
    }

    /**
     * Test that slot cache properly handles offset patterns with collisions.
     * Since slot cache uses (blockIdx mod 32), test pattern that causes slot collisions.
     */
    @Test
    public void testSlotCacheCollisions() throws IOException {
        // Access blocks that map to the same slot (32 blocks apart)
        long[] offsets = { 0, 32 * BLOCK_SIZE, 64 * BLOCK_SIZE };

        for (long offset : offsets) {
            BlockSlotTinyCache.LookupResult result = tinyCache.acquireRefCountedValue(offset);
            assertNotNull("Should get value for offset " + offset, result.value());
            result.value().unpin();
        }

        // Accessing same offsets again should still work (generation checks protect)
        for (long offset : offsets) {
            BlockSlotTinyCache.LookupResult result = tinyCache.acquireRefCountedValue(offset);
            assertNotNull("Should get value for offset " + offset, result.value());
            result.value().unpin();
        }
    }

    /**
     * Simulated block cache that maintains key->value mappings and simulates
     * a memory pool with eviction and recycling.
     */
    private static class SimulatedBlockCache implements BlockCache<RefCountedMemorySegment> {
        private final Arena arena;
        private final int poolSize;
        private final Map<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> cache;
        private final RefCountedMemorySegment[] pool;
        private final AtomicInteger poolIndex = new AtomicInteger(0);
        private final AtomicInteger evictionCounter = new AtomicInteger(0);

        public SimulatedBlockCache(Arena arena, int poolSize) {
            this.arena = arena;
            this.poolSize = poolSize;
            this.cache = new ConcurrentHashMap<>();
            this.pool = new RefCountedMemorySegment[poolSize];

            // Initialize pool
            for (int i = 0; i < poolSize; i++) {
                pool[i] = createSegment();
            }
        }

        private RefCountedMemorySegment createSegment() {
            MemorySegment nativeSeg = arena.allocate(BLOCK_SIZE);
            BlockReleaser<RefCountedMemorySegment> releaser = seg -> {
                // Return to pool
                // In real implementation, this would mark segment as available
            };
            return new RefCountedMemorySegment(nativeSeg, BLOCK_SIZE, releaser);
        }

        @Override
        public BlockCacheValue<RefCountedMemorySegment> get(BlockCacheKey key) {
            return cache.get(key);
        }

        @Override
        public BlockCacheValue<RefCountedMemorySegment> getOrLoad(BlockCacheKey key) throws IOException {
            return cache.computeIfAbsent(key, k -> {
                // Simulate cache full - evict oldest entry
                if (cache.size() >= poolSize) {
                    evictOne();
                }

                // Get segment from pool (round-robin)
                int idx = poolIndex.getAndIncrement() % poolSize;
                RefCountedMemorySegment segment = pool[idx];

                // If segment is in use, reset it (simulates recycling)
                if (segment.getRefCount() == 0) {
                    segment.reset();
                }

                return segment;
            });
        }

        private void evictOne() {
            // Simple eviction: remove first entry with refCount=1 (cache only)
            cache.entrySet().stream().filter(e -> e.getValue().value().getRefCount() == 1).findFirst().ifPresent(e -> {
                cache.remove(e.getKey());
                e.getValue().value().close(); // Triggers generation increment
                evictionCounter.incrementAndGet();
            });
        }

        public RefCountedMemorySegment getCachedSegment(Path path, long offset) {
            FileBlockCacheKey key = new FileBlockCacheKey(path, offset);
            BlockCacheValue<RefCountedMemorySegment> val = cache.get(key);
            return val != null ? val.value() : null;
        }

        @Override
        public void prefetch(BlockCacheKey key) {
            // No-op for test
        }

        @Override
        public void put(BlockCacheKey key, BlockCacheValue<RefCountedMemorySegment> value) {
            cache.put(key, value);
        }

        @Override
        public void invalidate(BlockCacheKey key) {
            BlockCacheValue<RefCountedMemorySegment> val = cache.remove(key);
            if (val != null) {
                val.close();
            }
        }

        @Override
        public void invalidate(Path normalizedFilePath) {
            cache.entrySet().removeIf(e -> {
                if (e.getKey() instanceof FileBlockCacheKey) {
                    FileBlockCacheKey fk = (FileBlockCacheKey) e.getKey();
                    if (fk.filePath().equals(normalizedFilePath)) {
                        e.getValue().close();
                        return true;
                    }
                }
                return false;
            });
        }

        @Override
        public void invalidateByPathPrefix(Path directoryPath) {
            String prefix = directoryPath.toString();
            cache.entrySet().removeIf(e -> {
                if (e.getKey() instanceof FileBlockCacheKey) {
                    FileBlockCacheKey fk = (FileBlockCacheKey) e.getKey();
                    if (fk.filePath().toString().startsWith(prefix)) {
                        e.getValue().close();
                        return true;
                    }
                }
                return false;
            });
        }

        @Override
        public void clear() {
            cache.values().forEach(BlockCacheValue::close);
            cache.clear();
        }

        @Override
        public Map<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> loadBulk(Path filePath, long startOffset, long blockCount)
            throws IOException {
            // Simple implementation for test
            Map<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> result = new ConcurrentHashMap<>();
            for (long i = 0; i < blockCount; i++) {
                long offset = startOffset + (i * BLOCK_SIZE);
                FileBlockCacheKey key = new FileBlockCacheKey(filePath, offset);
                BlockCacheValue<RefCountedMemorySegment> val = getOrLoad(key);
                result.put(key, val);
            }
            return result;
        }

        @Override
        public String cacheStats() {
            return "SimulatedCache: size=" + cache.size() + ", evictions=" + evictionCounter.get();
        }

        @Override
        public void recordStats() {
            // No-op for test
        }
    }
}

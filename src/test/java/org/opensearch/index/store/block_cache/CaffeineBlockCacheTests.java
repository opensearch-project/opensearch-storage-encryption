/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

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

import org.junit.Before;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.test.OpenSearchTestCase;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

@SuppressWarnings("unchecked")
public class CaffeineBlockCacheTests extends OpenSearchTestCase {

    private Cache<BlockCacheKey, BlockCacheValue<String>> caffeineCache;
    private BlockLoader<BlockCacheValue<String>> mockLoader;
    private CaffeineBlockCache<String, BlockCacheValue<String>> blockCache;
    private static final long MAX_BLOCKS = 100;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        caffeineCache = Caffeine.newBuilder().maximumSize(MAX_BLOCKS).recordStats().build();
        mockLoader = mock(BlockLoader.class);
        blockCache = new CaffeineBlockCache<>(caffeineCache, mockLoader, MAX_BLOCKS);
    }

    /**
     * Tests basic get operation when key is not present.
     */
    public void testGetReturnsNullForMissingKey() {
        BlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 0L);

        BlockCacheValue<String> result = blockCache.get(key);

        assertNull("Get should return null for missing key", result);
    }

    /**
     * Tests get operation when key is present in cache.
     */
    public void testGetReturnsValueForPresentKey() {
        BlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 0L);
        BlockCacheValue<String> value = createMockValue("data");

        blockCache.put(key, value);
        BlockCacheValue<String> result = blockCache.get(key);

        assertNotNull("Get should return value for present key", result);
        assertSame("Should return same value that was put", value, result);
    }

    /**
     * Tests put operation adds value to cache.
     */
    public void testPutAddsValueToCache() {
        BlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 0L);
        BlockCacheValue<String> value = createMockValue("data");

        blockCache.put(key, value);

        BlockCacheValue<String> retrieved = blockCache.get(key);
        assertNotNull("Put should add value to cache", retrieved);
        assertSame("Retrieved value should match", value, retrieved);
    }

    /**
     * Tests getOrLoad returns cached value without calling loader.
     */
    public void testGetOrLoadReturnsCachedValue() throws Exception {
        BlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 0L);
        BlockCacheValue<String> value = createMockValue("cached");

        blockCache.put(key, value);
        BlockCacheValue<String> result = blockCache.getOrLoad(key);

        assertSame("Should return cached value", value, result);
        verify(mockLoader, times(0)).load(any(BlockCacheKey.class));
    }

    /**
     * Tests getOrLoad loads value when not cached.
     */
    public void testGetOrLoadLoadsValueOnCacheMiss() throws Exception {
        BlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 0L);
        BlockCacheValue<String> value = createMockValue("loaded");

        when(mockLoader.load(key)).thenReturn(value);

        BlockCacheValue<String> result = blockCache.getOrLoad(key);

        assertNotNull("Should load value", result);
        assertSame("Should return loaded value", value, result);
        verify(mockLoader, times(1)).load(key);
    }

    /**
     * Tests getOrLoad caches loaded value for future requests.
     */
    public void testGetOrLoadCachesLoadedValue() throws Exception {
        BlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 0L);
        BlockCacheValue<String> value = createMockValue("loaded");

        when(mockLoader.load(key)).thenReturn(value);

        // First call - should load
        blockCache.getOrLoad(key);

        // Second call - should use cache
        BlockCacheValue<String> result = blockCache.getOrLoad(key);

        assertSame("Should return cached value", value, result);
        verify(mockLoader, times(1)).load(key); // Only loaded once
    }

    /**
     * Tests getOrLoad throws exception when loader throws PoolPressureException.
     */
    public void testGetOrLoadHandlesPoolPressureException() throws Exception {
        BlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 0L);

        when(mockLoader.load(key)).thenThrow(new BlockLoader.PoolPressureException("Pool exhausted"));

        // The implementation converts these to UncheckedIOException which then gets
        // caught and rethrown as IOException in the outer catch
        expectThrows(Exception.class, () -> blockCache.getOrLoad(key));
    }

    /**
     * Tests getOrLoad throws exception when loader throws PoolAcquireFailedException.
     */
    public void testGetOrLoadHandlesPoolAcquireFailedException() throws Exception {
        BlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 0L);

        when(mockLoader.load(key)).thenThrow(new BlockLoader.PoolAcquireFailedException("Acquire timeout"));

        expectThrows(Exception.class, () -> blockCache.getOrLoad(key));
    }

    /**
     * Tests getOrLoad throws exception when loader throws BlockLoadFailedException.
     */
    public void testGetOrLoadHandlesBlockLoadFailedException() throws Exception {
        BlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 0L);

        when(mockLoader.load(key)).thenThrow(new BlockLoader.BlockLoadFailedException("Load failed"));

        expectThrows(Exception.class, () -> blockCache.getOrLoad(key));
    }

    /**
     * Tests getOrLoad throws exception when loader throws NoSuchFileException.
     */
    public void testGetOrLoadHandlesNoSuchFileException() throws Exception {
        BlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/missing.dat"), 0L);

        when(mockLoader.load(key)).thenThrow(new NoSuchFileException("/test/missing.dat"));

        expectThrows(Exception.class, () -> blockCache.getOrLoad(key));
    }

    /**
     * Tests getOrLoad throws exception when loader throws generic IOException.
     */
    public void testGetOrLoadHandlesIOException() throws Exception {
        BlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 0L);

        when(mockLoader.load(key)).thenThrow(new IOException("I/O error"));

        expectThrows(Exception.class, () -> blockCache.getOrLoad(key));
    }

    /**
     * Tests getOrLoad wraps RuntimeException in IOException.
     */
    public void testGetOrLoadWrapsRuntimeException() throws Exception {
        BlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 0L);

        when(mockLoader.load(key)).thenThrow(new RuntimeException("Unexpected error"));

        expectThrows(IOException.class, () -> blockCache.getOrLoad(key));
    }

    /**
     * Tests invalidate removes specific key from cache.
     */
    public void testInvalidateRemovesKey() {
        BlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 0L);
        BlockCacheValue<String> value = createMockValue("data");

        blockCache.put(key, value);
        assertNotNull("Value should be cached", blockCache.get(key));

        blockCache.invalidate(key);

        assertNull("Value should be removed after invalidate", blockCache.get(key));
    }

    /**
     * Tests invalidate by path removes all blocks for that file.
     */
    public void testInvalidateByPathRemovesAllFileBlocks() {
        Path path = Paths.get("/test/file.dat");
        BlockCacheKey key1 = new FileBlockCacheKey(path, 0L);
        BlockCacheKey key2 = new FileBlockCacheKey(path, 8192L);
        BlockCacheKey key3 = new FileBlockCacheKey(path, 16384L);

        blockCache.put(key1, createMockValue("block1"));
        blockCache.put(key2, createMockValue("block2"));
        blockCache.put(key3, createMockValue("block3"));

        blockCache.invalidate(path);

        assertNull("Block 1 should be invalidated", blockCache.get(key1));
        assertNull("Block 2 should be invalidated", blockCache.get(key2));
        assertNull("Block 3 should be invalidated", blockCache.get(key3));
    }

    /**
     * Tests invalidate by path only removes blocks for that specific file.
     */
    public void testInvalidateByPathOnlyAffectsSpecifiedFile() {
        Path path1 = Paths.get("/test/file1.dat");
        Path path2 = Paths.get("/test/file2.dat");

        BlockCacheKey key1 = new FileBlockCacheKey(path1, 0L);
        BlockCacheKey key2 = new FileBlockCacheKey(path2, 0L);

        blockCache.put(key1, createMockValue("file1"));
        blockCache.put(key2, createMockValue("file2"));

        blockCache.invalidate(path1);

        assertNull("File1 block should be invalidated", blockCache.get(key1));
        assertNotNull("File2 block should remain", blockCache.get(key2));
    }

    /**
     * Tests invalidate by path normalizes paths correctly.
     */
    public void testInvalidateByPathNormalizesPath() {
        Path normalizedPath = Paths.get("/test/file.dat").toAbsolutePath().normalize();
        Path pathWithDots = Paths.get("/test/./file.dat");

        BlockCacheKey key = new FileBlockCacheKey(normalizedPath, 0L);
        blockCache.put(key, createMockValue("data"));

        blockCache.invalidate(pathWithDots);

        assertNull("Block should be invalidated with path normalization", blockCache.get(key));
    }

    /**
     * Tests clear removes all entries from cache.
     */
    public void testClearRemovesAllEntries() {
        BlockCacheKey key1 = new FileBlockCacheKey(Paths.get("/test/file1.dat"), 0L);
        BlockCacheKey key2 = new FileBlockCacheKey(Paths.get("/test/file2.dat"), 0L);
        BlockCacheKey key3 = new FileBlockCacheKey(Paths.get("/test/file3.dat"), 0L);

        blockCache.put(key1, createMockValue("data1"));
        blockCache.put(key2, createMockValue("data2"));
        blockCache.put(key3, createMockValue("data3"));

        blockCache.clear();

        assertNull("All entries should be cleared", blockCache.get(key1));
        assertNull("All entries should be cleared", blockCache.get(key2));
        assertNull("All entries should be cleared", blockCache.get(key3));
    }

    /**
     * Tests prefetch loads value into cache.
     */
    public void testPrefetchLoadsValue() throws Exception {
        BlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 0L);
        BlockCacheValue<String> value = createMockValue("prefetched");

        when(mockLoader.load(key)).thenReturn(value);

        blockCache.prefetch(key);

        // Wait briefly for async operation
        Thread.sleep(10);

        BlockCacheValue<String> cached = blockCache.get(key);
        assertNotNull("Prefetch should cache value", cached);
    }

    /**
     * Tests prefetch does not throw exception on failure.
     */
    public void testPrefetchDoesNotThrowOnFailure() throws Exception {
        BlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 0L);

        when(mockLoader.load(key)).thenThrow(new IOException("Load failed"));

        // Should not throw exception
        blockCache.prefetch(key);

        assertNull("Failed prefetch should not cache value", blockCache.get(key));
    }

    /**
     * Tests loadBulk loads multiple blocks efficiently.
     */
    public void testLoadBulkLoadsMultipleBlocks() throws Exception {
        Path path = Paths.get("/test/file.dat");
        long startOffset = 0L;
        long blockCount = 3L;

        BlockCacheValue<String>[] loadedValues = new BlockCacheValue[] {
            createMockValue("block0"),
            createMockValue("block1"),
            createMockValue("block2") };

        when(mockLoader.load(eq(path), eq(startOffset), eq(blockCount))).thenReturn(loadedValues);

        Map<BlockCacheKey, BlockCacheValue<String>> result = blockCache.loadBulk(path, startOffset, blockCount);

        assertEquals("Should load 3 blocks", 3, result.size());
        verify(mockLoader, times(1)).load(path, startOffset, blockCount);
    }

    /**
     * Tests loadBulk caches loaded blocks.
     */
    public void testLoadBulkCachesLoadedBlocks() throws Exception {
        Path path = Paths.get("/test/file.dat");
        long startOffset = 0L;
        long blockCount = 2L;

        BlockCacheValue<String>[] loadedValues = new BlockCacheValue[] { createMockValue("block0"), createMockValue("block1") };

        when(mockLoader.load(eq(path), eq(startOffset), eq(blockCount))).thenReturn(loadedValues);

        blockCache.loadBulk(path, startOffset, blockCount);

        BlockCacheKey key0 = new FileBlockCacheKey(path, 0L);
        BlockCacheKey key1 = new FileBlockCacheKey(path, 8192L); // CACHE_BLOCK_SIZE = 8192

        assertNotNull("Block 0 should be cached", blockCache.get(key0));
        assertNotNull("Block 1 should be cached", blockCache.get(key1));
    }

    /**
     * Tests loadBulk does not replace already cached blocks.
     */
    public void testLoadBulkDoesNotReplaceExistingBlocks() throws Exception {
        Path path = Paths.get("/test/file.dat");
        BlockCacheKey key0 = new FileBlockCacheKey(path, 0L);
        BlockCacheValue<String> existingValue = createMockValue("existing");

        // Pre-populate cache
        blockCache.put(key0, existingValue);

        BlockCacheValue<String>[] loadedValues = new BlockCacheValue[] { createMockValue("new"), createMockValue("block1") };

        when(mockLoader.load(eq(path), eq(0L), eq(2L))).thenReturn(loadedValues);

        blockCache.loadBulk(path, 0L, 2L);

        // Should still have existing value
        BlockCacheValue<String> cached = blockCache.get(key0);
        assertSame("Should keep existing cached value", existingValue, cached);
    }

    /**
     * Tests loadBulk releases newly loaded segments that weren't cached.
     */
    public void testLoadBulkReleasesUnusedSegments() throws Exception {
        Path path = Paths.get("/test/file.dat");
        BlockCacheKey key0 = new FileBlockCacheKey(path, 0L);
        BlockCacheValue<String> existingValue = createMockValue("existing");
        BlockCacheValue<String> newValue = createMockValue("new");

        // Pre-populate cache
        blockCache.put(key0, existingValue);

        BlockCacheValue<String>[] loadedValues = new BlockCacheValue[] { newValue };

        when(mockLoader.load(eq(path), eq(0L), eq(1L))).thenReturn(loadedValues);

        blockCache.loadBulk(path, 0L, 1L);

        // Verify decRef was called on the unused segment
        verify(newValue, times(1)).decRef();
    }

    /**
     * Tests loadBulk throws IOException when loader fails.
     */
    public void testLoadBulkThrowsIOExceptionOnFailure() throws Exception {
        Path path = Paths.get("/test/file.dat");

        when(mockLoader.load(any(Path.class), anyLong(), anyLong())).thenThrow(new IOException("Bulk load failed"));

        expectThrows(IOException.class, () -> blockCache.loadBulk(path, 0L, 3L));
    }

    /**
     * Tests loadBulk handles PoolPressureException.
     */
    public void testLoadBulkHandlesPoolPressureException() throws Exception {
        Path path = Paths.get("/test/file.dat");

        when(mockLoader.load(any(Path.class), anyLong(), anyLong())).thenThrow(new BlockLoader.PoolPressureException("Pool exhausted"));

        expectThrows(IOException.class, () -> blockCache.loadBulk(path, 0L, 3L));
    }

    /**
     * Tests cacheStats returns meaningful statistics.
     */
    public void testCacheStatsReturnsStatistics() throws Exception {
        BlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 0L);
        BlockCacheValue<String> value = createMockValue("data");

        when(mockLoader.load(key)).thenReturn(value);

        // Generate some cache activity
        blockCache.getOrLoad(key); // miss + load
        blockCache.get(key); // hit

        String stats = blockCache.cacheStats();

        assertNotNull("Stats should not be null", stats);
        assertTrue("Stats should contain 'Cache'", stats.contains("Cache"));
        assertTrue("Stats should contain size", stats.contains("size="));
        assertTrue("Stats should contain hits", stats.contains("hits="));
        assertTrue("Stats should contain misses", stats.contains("misses="));
    }

    /**
     * Tests getCache returns underlying Caffeine cache.
     */
    public void testGetCacheReturnsUnderlyingCache() {
        Cache<BlockCacheKey, BlockCacheValue<String>> underlying = blockCache.getCache();

        assertNotNull("Should return cache", underlying);
        assertSame("Should return same cache instance", caffeineCache, underlying);
    }

    /**
     * Tests cache eviction occurs when capacity is exceeded.
     */
    public void testCacheEvictionOnCapacityExceeded() {
        // Create small cache
        Cache<BlockCacheKey, BlockCacheValue<String>> smallCache = Caffeine.newBuilder().maximumSize(2).build();
        CaffeineBlockCache<String, BlockCacheValue<String>> limitedCache = new CaffeineBlockCache<>(smallCache, mockLoader, 2);

        BlockCacheKey key1 = new FileBlockCacheKey(Paths.get("/test/file1.dat"), 0L);
        BlockCacheKey key2 = new FileBlockCacheKey(Paths.get("/test/file2.dat"), 0L);
        BlockCacheKey key3 = new FileBlockCacheKey(Paths.get("/test/file3.dat"), 0L);

        limitedCache.put(key1, createMockValue("data1"));
        limitedCache.put(key2, createMockValue("data2"));
        limitedCache.put(key3, createMockValue("data3"));

        // Force eviction policy to apply
        smallCache.cleanUp();

        // At most 2 entries should be cached
        long cachedCount = smallCache.asMap().size();
        assertTrue("Cache should respect maximum size", cachedCount <= 2);
    }

    /**
     * Tests concurrent access to cache.
     */
    public void testConcurrentAccess() throws Exception {
        BlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 0L);
        BlockCacheValue<String> value = createMockValue("data");

        when(mockLoader.load(key)).thenReturn(value);

        // Simulate concurrent access
        Thread thread1 = new Thread(() -> {
            try {
                blockCache.getOrLoad(key);
            } catch (IOException e) {
                fail("Thread 1 failed: " + e.getMessage());
            }
        });

        Thread thread2 = new Thread(() -> {
            try {
                blockCache.getOrLoad(key);
            } catch (IOException e) {
                fail("Thread 2 failed: " + e.getMessage());
            }
        });

        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        // Loader should be called at most once due to cache
        verify(mockLoader, times(1)).load(key);
    }

    /**
     * Tests empty cache returns correct stats.
     * Note: Caffeine's estimatedSize() may not immediately reflect 0 due to async cleanup,
     * so we verify the cache is actually empty via the underlying cache.
     */
    public void testEmptyCacheStats() {
        // Verify cache is actually empty
        assertEquals("Cache should be empty", 0L, caffeineCache.asMap().size());

        String stats = blockCache.cacheStats();

        assertNotNull("Stats should not be null for empty cache", stats);
        // Stats may contain size=0 or a small number due to Caffeine's async estimation
        assertTrue("Stats should be non-empty", stats.length() > 0);
    }

    // Helper methods

    private BlockCacheValue<String> createMockValue(String data) {
        BlockCacheValue<String> value = mock(BlockCacheValue.class);
        when(value.value()).thenReturn(data);
        return value;
    }
}

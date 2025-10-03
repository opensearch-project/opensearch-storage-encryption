/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.iv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.security.Key;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.crypto.spec.SecretKeySpec;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.CryptoDirectoryFactory;

public class NodeLevelKeyCacheTests {

    @Mock
    private DefaultKeyIvResolver mockResolver;

    private Key testKey1;
    private Key testKey2;
    private static final String TEST_INDEX_UUID = "test-index-123";

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        // Create test keys
        testKey1 = new SecretKeySpec(new byte[32], "AES");
        testKey2 = new SecretKeySpec(new byte[32], "AES");
        testKey2.getEncoded()[0] = 1; // Make it different from testKey1

        // Reset singleton before each test
        NodeLevelKeyCache.reset();

        // Clear the IndexKeyResolverRegistry cache
        IndexKeyResolverRegistry.clearCache();

        // Setup mock resolver
        when(mockResolver.loadKeyFromMasterKeyProvider()).thenReturn(testKey1);
    }

    @After
    public void tearDown() {
        // Clean up after each test
        NodeLevelKeyCache.reset();
        IndexKeyResolverRegistry.clearCache();
    }

    /**
     * Helper method to register a mock resolver in the IndexKeyResolverRegistry
     */
    private void registerMockResolver(String indexUuid) throws Exception {
        Field resolverCacheField = IndexKeyResolverRegistry.class.getDeclaredField("resolverCache");
        resolverCacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<String, KeyIvResolver> resolverCache = (ConcurrentMap<String, KeyIvResolver>) resolverCacheField.get(null);
        resolverCache.put(indexUuid, mockResolver);
    }

    @Test
    public void testInitialization() {
        Settings settings = Settings.builder().put("node.store.data_key_ttl_seconds", 60).build();

        NodeLevelKeyCache.initialize(settings);

        assertNotNull(NodeLevelKeyCache.getInstance());
    }

    @Test(expected = IllegalStateException.class)
    public void testGetInstanceWithoutInitialization() {
        NodeLevelKeyCache.getInstance();
    }

    @Test
    public void testInitialKeyLoad() throws Exception {
        Settings settings = Settings.EMPTY;
        NodeLevelKeyCache.initialize(settings);
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        // Register the mock resolver before using the cache
        registerMockResolver(TEST_INDEX_UUID);

        Key retrievedKey = cache.get(TEST_INDEX_UUID);

        assertEquals(testKey1, retrievedKey);
        verify(mockResolver, times(1)).loadKeyFromMasterKeyProvider();
    }

    @Test
    public void testInitialKeyLoadFailure() throws Exception {
        when(mockResolver.loadKeyFromMasterKeyProvider())
            .thenThrow(new RuntimeException("KMS unavailable"));
        
        Settings settings = Settings.EMPTY;
        NodeLevelKeyCache.initialize(settings);
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();
        
        // Register the mock resolver
        registerMockResolver(TEST_INDEX_UUID);
        
        Exception thrown = null;
        try {
            cache.get(TEST_INDEX_UUID);
            fail("Expected exception not thrown");
        } catch (Exception e) {
            thrown = e;
        }
        
        assertNotNull(thrown);
        assertTrue(thrown.getMessage().contains("KMS unavailable"));
    }

    @Test
    public void testCacheHit() throws Exception {
        Settings settings = Settings.EMPTY;
        NodeLevelKeyCache.initialize(settings);
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        // Register the mock resolver
        registerMockResolver(TEST_INDEX_UUID);

        // First call should load
        Key key1 = cache.get(TEST_INDEX_UUID);
        // Second call should hit cache
        Key key2 = cache.get(TEST_INDEX_UUID);

        assertEquals(key1, key2);
        // Should only load once
        verify(mockResolver, times(1)).loadKeyFromMasterKeyProvider();
    }

    @Test
    public void testRefreshSuccess() throws Exception {
        // Use a very short TTL for testing
        Settings settings = Settings.builder().put("node.store.data_key_ttl_seconds", 1).build();

        when(mockResolver.loadKeyFromMasterKeyProvider())
            .thenReturn(testKey1)  // Initial load
            .thenReturn(testKey2); // Refresh

        NodeLevelKeyCache.initialize(settings);
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        // Register the mock resolver
        registerMockResolver(TEST_INDEX_UUID);

        // Initial load
        Key initialKey = cache.get(TEST_INDEX_UUID);
        assertEquals(testKey1, initialKey);

        // Wait for refresh to trigger and complete
        Thread.sleep(1500);

        // Force a get to ensure refresh is complete
        cache.get(TEST_INDEX_UUID);

        // Wait a bit more for async refresh to complete
        Thread.sleep(500);

        // Access again - should get refreshed key
        Key refreshedKey = cache.get(TEST_INDEX_UUID);
        assertEquals(testKey2, refreshedKey);

        verify(mockResolver, atLeast(2)).loadKeyFromMasterKeyProvider();
    }

    @Test
    public void testRefreshFailureReturnsOldKey() throws Exception {
        // Use a very short TTL for testing
        Settings settings = Settings.builder().put("node.store.data_key_ttl_seconds", 1).build();

        when(mockResolver.loadKeyFromMasterKeyProvider())
            .thenReturn(testKey1)  // Initial load
            .thenThrow(new RuntimeException("KMS refresh failed")); // Refresh fails

        NodeLevelKeyCache.initialize(settings);
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        // Register the mock resolver
        registerMockResolver(TEST_INDEX_UUID);

        // Initial load
        Key initialKey = cache.get(TEST_INDEX_UUID);
        assertEquals(testKey1, initialKey);

        // Wait for refresh to trigger
        Thread.sleep(1500);

        // Access again - should still get old key since refresh failed
        Key stillOldKey = cache.get(TEST_INDEX_UUID);
        assertEquals(testKey1, stillOldKey);

        verify(mockResolver, times(2)).loadKeyFromMasterKeyProvider();
    }

    @Test
    public void testMultipleRefreshFailures() throws Exception {
        // Use a very short TTL for testing
        Settings settings = Settings.builder().put("node.store.data_key_ttl_seconds", 1).build();

        when(mockResolver.loadKeyFromMasterKeyProvider())
            .thenReturn(testKey1)  // Initial load
            .thenThrow(new RuntimeException("KMS refresh failed 1"))
            .thenThrow(new RuntimeException("KMS refresh failed 2"))
            .thenThrow(new RuntimeException("KMS refresh failed 3"));

        NodeLevelKeyCache.initialize(settings);
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        // Register the mock resolver
        registerMockResolver(TEST_INDEX_UUID);

        // Initial load
        Key initialKey = cache.get(TEST_INDEX_UUID);
        assertEquals(testKey1, initialKey);

        // Multiple accesses with failed refreshes
        for (int i = 0; i < 3; i++) {
            Thread.sleep(1200);
            Key key = cache.get(TEST_INDEX_UUID);
            assertEquals(testKey1, key); // Should always return original key
        }

        verify(mockResolver, atLeast(3)).loadKeyFromMasterKeyProvider();
    }

    @Test
    public void testEviction() throws Exception {
        Settings settings = Settings.EMPTY;
        NodeLevelKeyCache.initialize(settings);
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        // Register the mock resolver
        registerMockResolver(TEST_INDEX_UUID);

        // Load key
        cache.get(TEST_INDEX_UUID);
        assertEquals(1, cache.size());

        // Evict
        cache.evict(TEST_INDEX_UUID);

        // Key should be loaded again
        cache.get(TEST_INDEX_UUID);

        // Should have loaded twice (once before eviction, once after)
        verify(mockResolver, times(2)).loadKeyFromMasterKeyProvider();
    }

    @Test
    public void testSize() throws Exception {
        Settings settings = Settings.EMPTY;
        NodeLevelKeyCache.initialize(settings);
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        assertEquals(0, cache.size());

        // Register resolvers for both indices
        registerMockResolver("index1");
        registerMockResolver("index2");

        cache.get("index1");
        assertEquals(1, cache.size());

        cache.get("index2");
        assertEquals(2, cache.size());
    }

    @Test
    public void testClear() throws Exception {
        Settings settings = Settings.EMPTY;
        NodeLevelKeyCache.initialize(settings);
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        // Register resolvers for both indices
        registerMockResolver("index1");
        registerMockResolver("index2");

        cache.get("index1");
        cache.get("index2");
        assertEquals(2, cache.size());

        cache.clear();
        assertEquals(0, cache.size());
    }

    @Test
    public void testReset() throws Exception {
        Settings settings = Settings.EMPTY;
        NodeLevelKeyCache.initialize(settings);

        assertNotNull(NodeLevelKeyCache.getInstance());

        NodeLevelKeyCache.reset();

        // Should throw after reset
        Exception thrown = null;
        try {
            NodeLevelKeyCache.getInstance();
        } catch (IllegalStateException e) {
            thrown = e;
        }
        assertNotNull(thrown);
    }

    @Test
    public void testConcurrentAccess() throws Exception {
        final AtomicInteger loadCount = new AtomicInteger(0);

        when(mockResolver.loadKeyFromMasterKeyProvider()).thenAnswer(invocation -> {
            loadCount.incrementAndGet();
            Thread.sleep(100); // Simulate slow load
            return testKey1;
        });

        Settings settings = Settings.EMPTY;
        NodeLevelKeyCache.initialize(settings);
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        // Register the mock resolver
        registerMockResolver(TEST_INDEX_UUID);

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    Key key = cache.get(TEST_INDEX_UUID);
                    assertEquals(testKey1, key);
                } catch (Exception e) {
                    fail("Unexpected exception: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        executor.shutdown();

        // Should only load once despite concurrent access
        assertEquals(1, loadCount.get());
    }

    @Test
    public void testNullParameters() throws Exception {
        Settings settings = Settings.EMPTY;
        NodeLevelKeyCache.initialize(settings);
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        // Test null index UUID
        Exception thrown = null;
        try {
            cache.get(null);
        } catch (NullPointerException e) {
            thrown = e;
        }
        assertNotNull(thrown);
        assertTrue(thrown.getMessage().contains("indexUuid cannot be null"));

        // Test evict with null
        thrown = null;
        try {
            cache.evict(null);
        } catch (NullPointerException e) {
            thrown = e;
        }
        assertNotNull(thrown);
        assertTrue(thrown.getMessage().contains("indexUuid cannot be null"));
    }

    @Test
    public void testDefaultTTLValue() {
        // Test default TTL when not specified
        Settings settings = Settings.EMPTY;
        NodeLevelKeyCache.initialize(settings);

        // Should initialize successfully with default value (3600 seconds)
        assertNotNull(NodeLevelKeyCache.getInstance());
    }

    @Test
    public void testCacheWithRefreshDisabled() throws Exception {
        when(mockResolver.loadKeyFromMasterKeyProvider())
            .thenReturn(testKey1)  // Initial load
            .thenReturn(testKey2); // Should never be called with -1 TTL

        // Initialize with TTL = -1 (never refresh)
        Settings settings = Settings.builder().put("node.store.data_key_ttl_seconds", -1).build();
        NodeLevelKeyCache.initialize(settings);
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        // Register the mock resolver
        registerMockResolver(TEST_INDEX_UUID);

        // Initial load
        Key initialKey = cache.get(TEST_INDEX_UUID);
        assertEquals(testKey1, initialKey);

        // Wait for what would be a refresh period
        Thread.sleep(2000);

        // Access again - should still get same key (no refresh)
        Key sameKey = cache.get(TEST_INDEX_UUID);
        assertEquals(testKey1, sameKey);

        // Should only load once (no refresh)
        verify(mockResolver, times(1)).loadKeyFromMasterKeyProvider();
    }

    @Test
    public void testInvalidTTLValues() {
        // Test that 0 is rejected
        Settings settings = Settings.builder().put("node.store.data_key_ttl_seconds", 0).build();

        try {
            CryptoDirectoryFactory.NODE_DATA_KEY_TTL_SECONDS_SETTING.get(settings);
            fail("Expected IllegalArgumentException for invalid TTL value");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("must be -1 (never refresh) or a positive value"));
        }
    }
}

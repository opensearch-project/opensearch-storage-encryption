/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.key;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.CryptoDirectoryFactory;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.IndicesAdminClient;

public class NodeLevelKeyCacheTests extends OpenSearchTestCase {

    @Mock
    private DefaultKeyResolver mockResolver;

    @Mock
    private Client mockClient;

    @Mock
    private ClusterService mockClusterService;

    @Mock
    private ClusterState mockClusterState;

    @Mock
    private Metadata mockMetadata;

    private Key testKey1;
    private Key testKey2;
    private static final String TEST_INDEX_UUID = "test-index-123";
    private static final int TEST_SHARD_ID = 0;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        // Create test keys
        testKey1 = new SecretKeySpec(new byte[32], "AES");
        testKey2 = new SecretKeySpec(new byte[32], "AES");
        testKey2.getEncoded()[0] = 1; // Make it different from testKey1

        // Reset singletons before each test
        MasterKeyHealthMonitor.reset();
        NodeLevelKeyCache.reset();

        // Clear the ShardKeyResolverRegistry cache
        ShardKeyResolverRegistry.clearCache();

        // Setup mock cluster service
        when(mockClusterService.state()).thenReturn(mockClusterState);
        when(mockClusterState.metadata()).thenReturn(mockMetadata);
        when(mockMetadata.indices()).thenReturn(java.util.Collections.emptyMap());

        // Setup mock Client chain for block operations
        AdminClient mockAdminClient = mock(AdminClient.class);
        IndicesAdminClient mockIndicesAdminClient = mock(IndicesAdminClient.class);
        @SuppressWarnings("unchecked")
        ActionFuture<AcknowledgedResponse> mockFuture = (ActionFuture<AcknowledgedResponse>) mock(ActionFuture.class);

        when(mockClient.admin()).thenReturn(mockAdminClient);
        when(mockAdminClient.indices()).thenReturn(mockIndicesAdminClient);
        when(mockIndicesAdminClient.updateSettings(any())).thenReturn(mockFuture);
        when(mockFuture.actionGet()).thenReturn(mock(AcknowledgedResponse.class));

        // Setup mock resolver
        when(mockResolver.loadKeyFromMasterKeyProvider()).thenReturn(testKey1);
    }

    @After
    public void tearDown() throws Exception {
        // Clean up after each test
        MasterKeyHealthMonitor.reset();
        NodeLevelKeyCache.reset();
        ShardKeyResolverRegistry.clearCache();
        super.tearDown();
    }

    /**
     * Helper method to register a mock resolver in the ShardKeyResolverRegistry
     */
    @SuppressForbidden(reason = "Test needs to register mock resolver in ShardKeyResolverRegistry")
    private void registerMockResolver(String indexUuid, int shardId) throws Exception {
        Field resolverCacheField = ShardKeyResolverRegistry.class.getDeclaredField("resolverCache");
        resolverCacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<ShardCacheKey, KeyResolver> resolverCache = (ConcurrentMap<ShardCacheKey, KeyResolver>) resolverCacheField.get(null);
        resolverCache.put(new ShardCacheKey(indexUuid, shardId, "test-index"), mockResolver);
    }

    public void testInitialization() {
        Settings settings = Settings.builder().put("node.store.crypto.key_refresh_interval", "60s").build();

        MasterKeyHealthMonitor.initialize(settings, mockClient, mockClusterService);
        NodeLevelKeyCache.initialize(settings, MasterKeyHealthMonitor.getInstance());

        assertNotNull(NodeLevelKeyCache.getInstance());
    }

    public void testGetInstanceWithoutInitialization() {
        expectThrows(IllegalStateException.class, () -> { NodeLevelKeyCache.getInstance(); });
    }

    public void testInitialKeyLoad() throws Exception {
        Settings settings = Settings.EMPTY;
        MasterKeyHealthMonitor.initialize(settings, mockClient, mockClusterService);
        NodeLevelKeyCache.initialize(settings, MasterKeyHealthMonitor.getInstance());
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        // Register the mock resolver before using the cache
        registerMockResolver(TEST_INDEX_UUID, TEST_SHARD_ID);

        Key retrievedKey = cache.get(TEST_INDEX_UUID, TEST_SHARD_ID, "test-index");

        assertEquals(testKey1, retrievedKey);
        verify(mockResolver, times(1)).loadKeyFromMasterKeyProvider();
    }

    public void testInitialKeyLoadFailure() throws Exception {
        when(mockResolver.loadKeyFromMasterKeyProvider())
            .thenThrow(new RuntimeException("KMS unavailable"));
        
        Settings settings = Settings.EMPTY;
        MasterKeyHealthMonitor.initialize(settings, mockClient, mockClusterService);
        NodeLevelKeyCache.initialize(settings, MasterKeyHealthMonitor.getInstance());
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();
        
        // Register the mock resolver
        registerMockResolver(TEST_INDEX_UUID, TEST_SHARD_ID);
        
        Exception thrown = null;
        try {
            cache.get(TEST_INDEX_UUID, TEST_SHARD_ID, "test-index");
            fail("Expected exception not thrown");
        } catch (Exception e) {
            thrown = e;
        }
        
        assertNotNull(thrown);
        // Exception is now wrapped in KeyCacheException with suppressed cause
        assertTrue(thrown instanceof KeyCacheException);
        assertTrue(thrown.getMessage().contains("Failed to load key for index"));
    }

    public void testCacheHit() throws Exception {
        Settings settings = Settings.EMPTY;
        MasterKeyHealthMonitor.initialize(settings, mockClient, mockClusterService);
        NodeLevelKeyCache.initialize(settings, MasterKeyHealthMonitor.getInstance());
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        // Register the mock resolver
        registerMockResolver(TEST_INDEX_UUID, TEST_SHARD_ID);

        // First call should load
        Key key1 = cache.get(TEST_INDEX_UUID, TEST_SHARD_ID, "test-index");
        // Second call should hit cache
        Key key2 = cache.get(TEST_INDEX_UUID, TEST_SHARD_ID, "test-index");

        assertEquals(key1, key2);
        // Should only load once
        verify(mockResolver, times(1)).loadKeyFromMasterKeyProvider();
    }

    public void testRefreshSuccess() throws Exception {
        // Use a very short TTL for testing
        Settings settings = Settings.builder().put("node.store.crypto.key_refresh_interval", "1s").build();

        when(mockResolver.loadKeyFromMasterKeyProvider())
            .thenReturn(testKey1)  // Initial load
            .thenReturn(testKey2); // Refresh

        MasterKeyHealthMonitor.initialize(settings, mockClient, mockClusterService);
        NodeLevelKeyCache.initialize(settings, MasterKeyHealthMonitor.getInstance());
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        // Register the mock resolver
        registerMockResolver(TEST_INDEX_UUID, TEST_SHARD_ID);

        // Initial load
        Key initialKey = cache.get(TEST_INDEX_UUID, TEST_SHARD_ID, "test-index");
        assertEquals(testKey1, initialKey);

        // Wait for refresh to trigger and complete
        Thread.sleep(1500);

        // Force a get to ensure refresh is complete
        cache.get(TEST_INDEX_UUID, TEST_SHARD_ID, "test-index");

        // Wait a bit more for async refresh to complete
        Thread.sleep(500);

        // Access again - should get refreshed key
        Key refreshedKey = cache.get(TEST_INDEX_UUID, TEST_SHARD_ID, "test-index");
        assertEquals(testKey2, refreshedKey);
    }

    public void testRefreshFailureReturnsOldKey() throws Exception {
        // Use a very short TTL for testing
        Settings settings = Settings.builder().put("node.store.crypto.key_refresh_interval", "1s").build();

        when(mockResolver.loadKeyFromMasterKeyProvider())
            .thenReturn(testKey1)  // Initial load
            .thenThrow(new RuntimeException("KMS refresh failed")); // Refresh fails

        MasterKeyHealthMonitor.initialize(settings, mockClient, mockClusterService);
        NodeLevelKeyCache.initialize(settings, MasterKeyHealthMonitor.getInstance());
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        // Register the mock resolver
        registerMockResolver(TEST_INDEX_UUID, TEST_SHARD_ID);

        // Initial load
        Key initialKey = cache.get(TEST_INDEX_UUID, TEST_SHARD_ID, "test-index");
        assertEquals(testKey1, initialKey);

        // Wait for refresh to trigger (longer wait for async refresh)
        Thread.sleep(2000);

        // Force multiple gets to ensure refresh is triggered and completed
        for (int i = 0; i < 3; i++) {
            Key stillOldKey = cache.get(TEST_INDEX_UUID, TEST_SHARD_ID, "test-index");
            assertEquals(testKey1, stillOldKey);
            Thread.sleep(100); // Small delay between attempts
        }

        // Wait a bit more for any background refresh to complete
        Thread.sleep(500);

        // Verify that refresh was attempted (should be at least 2 calls total)
        verify(mockResolver, org.mockito.Mockito.atLeast(2)).loadKeyFromMasterKeyProvider();
    }

    public void testMultipleRefreshFailures() throws Exception {
        // Use a very short TTL for testing, with explicit expiry interval
        Settings settings = Settings
            .builder()
            .put("node.store.crypto.key_refresh_interval", "1s")
            .put("node.store.crypto.key_expiry_interval", "3s")
            .build();

        when(mockResolver.loadKeyFromMasterKeyProvider())
            .thenReturn(testKey1)  // Initial load
            .thenThrow(new RuntimeException("KMS refresh failed 1"))
            .thenThrow(new RuntimeException("KMS refresh failed 2"))
            .thenThrow(new RuntimeException("KMS refresh failed 3"))
            .thenThrow(new RuntimeException("KMS refresh failed 4")); // For post-expiry load

        MasterKeyHealthMonitor.initialize(settings, mockClient, mockClusterService);
        NodeLevelKeyCache.initialize(settings, MasterKeyHealthMonitor.getInstance());
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        // Register the mock resolver
        registerMockResolver(TEST_INDEX_UUID, TEST_SHARD_ID);

        // Initial load at t=0
        Key initialKey = cache.get(TEST_INDEX_UUID, TEST_SHARD_ID, "test-index");
        assertEquals(testKey1, initialKey);

        // Access at t=1.2s - triggers async refresh which fails
        Thread.sleep(1200);
        Key key1 = cache.get(TEST_INDEX_UUID, TEST_SHARD_ID, "test-index");
        assertEquals(testKey1, key1); // Still returns stale key

        // Access at t=2.4s - triggers another async refresh which fails
        Thread.sleep(1200);
        Key key2 = cache.get(TEST_INDEX_UUID, TEST_SHARD_ID, "test-index");
        assertEquals(testKey1, key2); // Still returns stale key

        // Wait past expiry time (3s from initial load) + buffer for async operations
        // At t=4s, the entry is expired and evicted
        Thread.sleep(1800);

        // Next access should trigger fresh load() which will fail
        Exception thrown = null;
        try {
            cache.get(TEST_INDEX_UUID, TEST_SHARD_ID, "test-index");
            fail("Expected KeyCacheException after cache expiry");
        } catch (KeyCacheException e) {
            thrown = e;
            assertTrue(e.getMessage().contains("Failed to load key for index"));
        }
        assertNotNull(thrown);

        // Verify at least 4 load attempts (initial + 2 refreshes + post-expiry load)
        verify(mockResolver, org.mockito.Mockito.atLeast(4)).loadKeyFromMasterKeyProvider();
    }

    public void testEviction() throws Exception {
        Settings settings = Settings.EMPTY;
        MasterKeyHealthMonitor.initialize(settings, mockClient, mockClusterService);
        NodeLevelKeyCache.initialize(settings, MasterKeyHealthMonitor.getInstance());
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        // Register the mock resolver
        registerMockResolver(TEST_INDEX_UUID, TEST_SHARD_ID);

        // Load key
        cache.get(TEST_INDEX_UUID, TEST_SHARD_ID, "test-index");
        assertEquals(1, cache.size());

        // Evict
        cache.evict(TEST_INDEX_UUID, TEST_SHARD_ID, "test-index");

        // Key should be loaded again
        cache.get(TEST_INDEX_UUID, TEST_SHARD_ID, "test-index");

        // Should have loaded twice (once before eviction, once after)
        verify(mockResolver, times(2)).loadKeyFromMasterKeyProvider();
    }

    public void testSize() throws Exception {
        Settings settings = Settings.EMPTY;
        MasterKeyHealthMonitor.initialize(settings, mockClient, mockClusterService);
        NodeLevelKeyCache.initialize(settings, MasterKeyHealthMonitor.getInstance());
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        assertEquals(0, cache.size());

        // Register resolvers for both indices
        registerMockResolver("index1", TEST_SHARD_ID);
        registerMockResolver("index2", TEST_SHARD_ID);

        cache.get("index1", TEST_SHARD_ID, "index1");
        assertEquals(1, cache.size());

        cache.get("index2", TEST_SHARD_ID, "index2");
        assertEquals(2, cache.size());
    }

    public void testClear() throws Exception {
        Settings settings = Settings.EMPTY;
        MasterKeyHealthMonitor.initialize(settings, mockClient, mockClusterService);
        NodeLevelKeyCache.initialize(settings, MasterKeyHealthMonitor.getInstance());
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        // Register resolvers for both indices
        registerMockResolver("index1", TEST_SHARD_ID);
        registerMockResolver("index2", TEST_SHARD_ID);

        cache.get("index1", TEST_SHARD_ID, "index1");
        cache.get("index2", TEST_SHARD_ID, "index2");
        assertEquals(2, cache.size());

        cache.clear();
        assertEquals(0, cache.size());
    }

    public void testReset() throws Exception {
        Settings settings = Settings.EMPTY;
        MasterKeyHealthMonitor.initialize(settings, mockClient, mockClusterService);
        NodeLevelKeyCache.initialize(settings, MasterKeyHealthMonitor.getInstance());

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

    public void testConcurrentAccess() throws Exception {
        final AtomicInteger loadCount = new AtomicInteger(0);

        when(mockResolver.loadKeyFromMasterKeyProvider()).thenAnswer(invocation -> {
            loadCount.incrementAndGet();
            Thread.sleep(100); // Simulate slow load
            return testKey1;
        });

        Settings settings = Settings.EMPTY;
        MasterKeyHealthMonitor.initialize(settings, mockClient, mockClusterService);
        NodeLevelKeyCache.initialize(settings, MasterKeyHealthMonitor.getInstance());
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        // Register the mock resolver
        registerMockResolver(TEST_INDEX_UUID, TEST_SHARD_ID);

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    Key key = cache.get(TEST_INDEX_UUID, TEST_SHARD_ID, "test-index");
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

    public void testNullParameters() throws Exception {
        Settings settings = Settings.EMPTY;
        MasterKeyHealthMonitor.initialize(settings, mockClient, mockClusterService);
        NodeLevelKeyCache.initialize(settings, MasterKeyHealthMonitor.getInstance());
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        // Test null index UUID
        Exception thrown = null;
        try {
            cache.get(null, TEST_SHARD_ID, "test-index");
        } catch (NullPointerException e) {
            thrown = e;
        }
        assertNotNull(thrown);
        assertTrue(thrown.getMessage().contains("indexUuid cannot be null"));

        // Test null resolver
        thrown = null;
        try {
            cache.get(TEST_INDEX_UUID, TEST_SHARD_ID, "test-index");
        } catch (IllegalStateException e) {
            thrown = e;
        }
        assertNotNull(thrown);
        assertTrue(thrown.getMessage().contains("No resolver registered for shard"));

        // Test evict with null
        thrown = null;
        try {
            cache.evict(null, TEST_SHARD_ID, "test-index");
        } catch (NullPointerException e) {
            thrown = e;
        }
        assertNotNull(thrown);
        assertTrue(thrown.getMessage().contains("indexUuid cannot be null"));
    }

    public void testNullDependenciesInConstructor() {
        Settings settings = Settings.EMPTY;

        // Test null healthMonitor
        Exception thrown = null;
        try {
            NodeLevelKeyCache.initialize(settings, null);
            fail("Expected NullPointerException for null healthMonitor");
        } catch (NullPointerException e) {
            thrown = e;
            assertTrue(e.getMessage().contains("healthMonitor cannot be null"));
        }
        assertNotNull(thrown);
    }

    public void testDefaultTTLValue() {
        // Test default TTL when not specified
        Settings settings = Settings.EMPTY;
        MasterKeyHealthMonitor.initialize(settings, mockClient, mockClusterService);
        NodeLevelKeyCache.initialize(settings, MasterKeyHealthMonitor.getInstance());

        // Should initialize successfully with default value (3600 seconds)
        assertNotNull(NodeLevelKeyCache.getInstance());
    }

    public void testCacheWithRefreshDisabled() throws Exception {
        when(mockResolver.loadKeyFromMasterKeyProvider())
            .thenReturn(testKey1)  // Initial load
            .thenReturn(testKey2); // Should never be called with -1 TTL

        // Initialize with TTL = -1 (never refresh)
        Settings settings = Settings.builder().put("node.store.crypto.key_refresh_interval", "-1").build();
        MasterKeyHealthMonitor.initialize(settings, mockClient, mockClusterService);
        NodeLevelKeyCache.initialize(settings, MasterKeyHealthMonitor.getInstance());
        NodeLevelKeyCache cache = NodeLevelKeyCache.getInstance();

        // Register the mock resolver
        registerMockResolver(TEST_INDEX_UUID, TEST_SHARD_ID);

        // Initial load
        Key initialKey = cache.get(TEST_INDEX_UUID, TEST_SHARD_ID, "test-index");
        assertEquals(testKey1, initialKey);

        // Wait for what would be a refresh period
        Thread.sleep(2000);

        // Access again - should still get same key (no refresh)
        Key sameKey = cache.get(TEST_INDEX_UUID, TEST_SHARD_ID, "test-index");
        assertEquals(testKey1, sameKey);

        // Should only load once (no refresh)
        verify(mockResolver, times(1)).loadKeyFromMasterKeyProvider();
    }

    public void testInvalidTTLValues() {
        // Test that invalid time values are rejected
        Settings settings = Settings.builder().put("node.store.crypto.key_refresh_interval", "invalid").build();

        try {
            CryptoDirectoryFactory.NODE_KEY_REFRESH_INTERVAL_SETTING.get(settings);
            fail("Expected IllegalArgumentException for invalid TTL value");
        } catch (IllegalArgumentException e) {
            // TimeValue parsing will throw IllegalArgumentException for invalid formats
            assertTrue(e.getMessage().contains("failed to parse") || e.getMessage().contains("unit is missing"));
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.key;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.security.Key;
import java.security.Provider;
import java.security.Security;
import java.util.concurrent.ConcurrentMap;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.NIOFSDirectory;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.IndicesAdminClient;

/**
 * Unit tests for {@link DefaultKeyResolver}
 */
public class DefaultKeyResolverTests extends OpenSearchTestCase {

    @Mock
    private MasterKeyProvider mockKeyProvider;

    private Directory directory;
    private Provider provider;
    private Path tempDir;
    private static final String TEST_INDEX_UUID = "test-index-uuid";
    private static final int TEST_SHARD_ID = 0;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        tempDir = createTempDir();
        directory = new NIOFSDirectory(tempDir, FSLockFactory.getDefault());
        provider = Security.getProvider("SunJCE");
        assertNotNull("SunJCE provider should be available", provider);

        // Initialize CryptoMetricsService
        CryptoMetricsService.initialize(mock(MetricsRegistry.class));

        // Initialize NodeLevelKeyCache with mock Client and ClusterService
        MasterKeyHealthMonitor.reset();
        NodeLevelKeyCache.reset();
        Client mockClient = mock(Client.class);
        ClusterService mockClusterService = mock(ClusterService.class);

        // Setup mock Client chain for block operations
        AdminClient mockAdminClient = mock(AdminClient.class);
        IndicesAdminClient mockIndicesAdminClient = mock(IndicesAdminClient.class);
        @SuppressWarnings("unchecked")
        ActionFuture<AcknowledgedResponse> mockFuture = (ActionFuture<AcknowledgedResponse>) mock(ActionFuture.class);

        when(mockClient.admin()).thenReturn(mockAdminClient);
        when(mockAdminClient.indices()).thenReturn(mockIndicesAdminClient);
        when(mockIndicesAdminClient.updateSettings(any())).thenReturn(mockFuture);
        when(mockFuture.actionGet()).thenReturn(mock(AcknowledgedResponse.class));

        MasterKeyHealthMonitor.initialize(Settings.EMPTY, mockClient, mockClusterService);
        NodeLevelKeyCache.initialize(Settings.EMPTY, MasterKeyHealthMonitor.getInstance());
    }

    @After
    public void tearDown() throws Exception {
        if (directory != null) {
            directory.close();
        }
        MasterKeyHealthMonitor.reset();
        NodeLevelKeyCache.reset();
        ShardKeyResolverRegistry.clearCache();
        super.tearDown();
    }

    /**
     * Helper method to register a resolver in the ShardKeyResolverRegistry
     */
    @SuppressForbidden(reason = "Test needs to register resolver in ShardKeyResolverRegistry")
    private void registerResolver(String indexUuid, int shardId, KeyResolver resolver) throws Exception {
        Field resolverCacheField = ShardKeyResolverRegistry.class.getDeclaredField("resolverCache");
        resolverCacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<ShardCacheKey, KeyResolver> resolverCache = (ConcurrentMap<ShardCacheKey, KeyResolver>) resolverCacheField.get(null);
        resolverCache.put(new ShardCacheKey(indexUuid, shardId, "test-index"), resolver);
    }

    public void testInitializationWithNewKey() throws Exception {
        byte[] dataKey = new byte[32];
        byte[] encryptedKey = new byte[32];
        for (int i = 0; i < 32; i++) {
            dataKey[i] = (byte) i;
            encryptedKey[i] = (byte) (i + 1);
        }

        DataKeyPair keyPair = new DataKeyPair(dataKey, encryptedKey);
        when(mockKeyProvider.generateDataPair()).thenReturn(keyPair);
        when(mockKeyProvider.decryptKey(any())).thenReturn(dataKey);

        DefaultKeyResolver resolver = new DefaultKeyResolver(
            TEST_INDEX_UUID,
            "test-index",
            directory,
            provider,
            mockKeyProvider,
            TEST_SHARD_ID
        );
        registerResolver(TEST_INDEX_UUID, TEST_SHARD_ID, resolver);

        assertNotNull(resolver);
        assertNotNull(resolver.getDataKey());
    }

    public void testInitializationWithExistingKey() throws Exception {
        byte[] dataKey = new byte[32];
        byte[] encryptedKey = new byte[32];
        for (int i = 0; i < 32; i++) {
            dataKey[i] = (byte) i;
            encryptedKey[i] = (byte) (i + 1);
        }

        DataKeyPair keyPair = new DataKeyPair(dataKey, encryptedKey);
        when(mockKeyProvider.generateDataPair()).thenReturn(keyPair);
        when(mockKeyProvider.decryptKey(any())).thenReturn(dataKey);

        // First resolver creates the key
        DefaultKeyResolver resolver1 = new DefaultKeyResolver(
            TEST_INDEX_UUID,
            "test-index",
            directory,
            provider,
            mockKeyProvider,
            TEST_SHARD_ID
        );
        registerResolver(TEST_INDEX_UUID, TEST_SHARD_ID, resolver1);

        Key key1 = resolver1.getDataKey();

        // Second resolver should read existing key
        DefaultKeyResolver resolver2 = new DefaultKeyResolver(
            TEST_INDEX_UUID,
            "test-index",
            directory,
            provider,
            mockKeyProvider,
            TEST_SHARD_ID
        );
        registerResolver(TEST_INDEX_UUID, TEST_SHARD_ID, resolver2);

        Key key2 = resolver2.getDataKey();

        // Keys should be the same
        assertArrayEquals(key1.getEncoded(), key2.getEncoded());
    }

    public void testGetDataKey() throws Exception {
        byte[] dataKey = new byte[32];
        byte[] encryptedKey = new byte[32];
        for (int i = 0; i < 32; i++) {
            dataKey[i] = (byte) i;
            encryptedKey[i] = (byte) (i + 1);
        }

        DataKeyPair keyPair = new DataKeyPair(dataKey, encryptedKey);
        when(mockKeyProvider.generateDataPair()).thenReturn(keyPair);
        when(mockKeyProvider.decryptKey(any())).thenReturn(dataKey);

        DefaultKeyResolver resolver = new DefaultKeyResolver(
            TEST_INDEX_UUID,
            "test-index",
            directory,
            provider,
            mockKeyProvider,
            TEST_SHARD_ID
        );
        registerResolver(TEST_INDEX_UUID, TEST_SHARD_ID, resolver);

        Key key = resolver.getDataKey();
        assertNotNull(key);
        assertEquals("AES", key.getAlgorithm());
        assertEquals(32, key.getEncoded().length);
    }

    public void testLoadKeyFromMasterKeyProvider() throws Exception {
        byte[] dataKey = new byte[32];
        byte[] encryptedKey = new byte[32];
        for (int i = 0; i < 32; i++) {
            dataKey[i] = (byte) i;
            encryptedKey[i] = (byte) (i + 1);
        }

        DataKeyPair keyPair = new DataKeyPair(dataKey, encryptedKey);
        when(mockKeyProvider.generateDataPair()).thenReturn(keyPair);
        when(mockKeyProvider.decryptKey(any())).thenReturn(dataKey);

        DefaultKeyResolver resolver = new DefaultKeyResolver(
            TEST_INDEX_UUID,
            "test-index",
            directory,
            provider,
            mockKeyProvider,
            TEST_SHARD_ID
        );

        Key loadedKey = resolver.loadKeyFromMasterKeyProvider();
        assertNotNull(loadedKey);
        assertEquals("AES", loadedKey.getAlgorithm());
        assertEquals(32, loadedKey.getEncoded().length);

        // Verify consistency - calling twice should return the same derived key
        Key loadedKey2 = resolver.loadKeyFromMasterKeyProvider();
        assertArrayEquals(loadedKey.getEncoded(), loadedKey2.getEncoded());
    }

    public void testMultipleResolversShareSameKey() throws Exception {
        byte[] dataKey = new byte[32];
        byte[] encryptedKey = new byte[32];
        for (int i = 0; i < 32; i++) {
            dataKey[i] = (byte) i;
            encryptedKey[i] = (byte) (i + 1);
        }

        DataKeyPair keyPair = new DataKeyPair(dataKey, encryptedKey);
        when(mockKeyProvider.generateDataPair()).thenReturn(keyPair);
        when(mockKeyProvider.decryptKey(any())).thenReturn(dataKey);

        // Create multiple resolvers
        DefaultKeyResolver resolver1 = new DefaultKeyResolver(
            TEST_INDEX_UUID,
            "test-index",
            directory,
            provider,
            mockKeyProvider,
            TEST_SHARD_ID
        );
        registerResolver(TEST_INDEX_UUID, TEST_SHARD_ID, resolver1);

        DefaultKeyResolver resolver2 = new DefaultKeyResolver(
            TEST_INDEX_UUID,
            "test-index",
            directory,
            provider,
            mockKeyProvider,
            TEST_SHARD_ID
        );
        registerResolver(TEST_INDEX_UUID, TEST_SHARD_ID, resolver2);

        DefaultKeyResolver resolver3 = new DefaultKeyResolver(
            TEST_INDEX_UUID,
            "test-index",
            directory,
            provider,
            mockKeyProvider,
            TEST_SHARD_ID
        );
        registerResolver(TEST_INDEX_UUID, TEST_SHARD_ID, resolver3);

        // All should have same key
        assertArrayEquals(resolver1.getDataKey().getEncoded(), resolver2.getDataKey().getEncoded());
        assertArrayEquals(resolver1.getDataKey().getEncoded(), resolver3.getDataKey().getEncoded());
    }

    public void testKeyFileCreation() throws Exception {
        byte[] dataKey = new byte[32];
        byte[] encryptedKey = new byte[32];
        for (int i = 0; i < 32; i++) {
            dataKey[i] = (byte) i;
            encryptedKey[i] = (byte) (i + 1);
        }

        DataKeyPair keyPair = new DataKeyPair(dataKey, encryptedKey);
        when(mockKeyProvider.generateDataPair()).thenReturn(keyPair);
        when(mockKeyProvider.decryptKey(any())).thenReturn(dataKey);

        DefaultKeyResolver resolver = new DefaultKeyResolver(
            TEST_INDEX_UUID,
            "test-index",
            directory,
            provider,
            mockKeyProvider,
            TEST_SHARD_ID
        );

        // Verify key file exists
        String[] files = directory.listAll();
        boolean keyFileExists = false;

        for (String file : files) {
            if (file.equals("keyfile")) {
                keyFileExists = true;
            }
        }

        assertTrue("keyfile should exist", keyFileExists);
    }

    public void testKeyIsConsistentAcrossReads() throws Exception {
        byte[] dataKey = new byte[32];
        byte[] encryptedKey = new byte[32];
        for (int i = 0; i < 32; i++) {
            dataKey[i] = (byte) i;
            encryptedKey[i] = (byte) (i + 1);
        }

        DataKeyPair keyPair = new DataKeyPair(dataKey, encryptedKey);
        when(mockKeyProvider.generateDataPair()).thenReturn(keyPair);
        when(mockKeyProvider.decryptKey(any())).thenReturn(dataKey);

        DefaultKeyResolver resolver = new DefaultKeyResolver(
            TEST_INDEX_UUID,
            "test-index",
            directory,
            provider,
            mockKeyProvider,
            TEST_SHARD_ID
        );
        registerResolver(TEST_INDEX_UUID, TEST_SHARD_ID, resolver);

        Key key1 = resolver.getDataKey();
        Key key2 = resolver.getDataKey();
        Key key3 = resolver.getDataKey();

        assertArrayEquals(key1.getEncoded(), key2.getEncoded());
        assertArrayEquals(key1.getEncoded(), key3.getEncoded());
    }

    public void testInitializationFailureOnKeyProviderError() throws Exception {
        when(mockKeyProvider.generateDataPair()).thenThrow(new RuntimeException("Key provider unavailable"));

        try {
            new DefaultKeyResolver(TEST_INDEX_UUID, "test-index", directory, provider, mockKeyProvider, TEST_SHARD_ID);
            fail("Expected KeyCacheException");
        } catch (KeyCacheException e) {
            assertTrue(e.getMessage().contains("KMS error for index"));
        }
    }
}

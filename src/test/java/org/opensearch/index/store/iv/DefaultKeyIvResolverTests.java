/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.iv;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.security.Key;
import java.security.Provider;
import java.security.Security;
import java.util.Base64;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.NIOFSDirectory;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link DefaultKeyIvResolver}
 */
public class DefaultKeyIvResolverTests extends OpenSearchTestCase {

    @Mock
    private MasterKeyProvider mockKeyProvider;

    private Directory directory;
    private Provider provider;
    private Path tempDir;
    private static final String TEST_INDEX_UUID = "test-index-uuid";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        tempDir = createTempDir();
        directory = new NIOFSDirectory(tempDir, FSLockFactory.getDefault());
        provider = Security.getProvider("SunJCE");
        assertNotNull("SunJCE provider should be available", provider);

        // Initialize NodeLevelKeyCache
        NodeLevelKeyCache.reset();
        NodeLevelKeyCache.initialize(org.opensearch.common.settings.Settings.EMPTY);
    }

    @After
    public void tearDown() throws Exception {
        if (directory != null) {
            directory.close();
        }
        NodeLevelKeyCache.reset();
        IndexKeyResolverRegistry.clearCache();
        super.tearDown();
    }

    /**
     * Helper method to register a resolver in the IndexKeyResolverRegistry
     */
    private void registerResolver(String indexUuid, KeyIvResolver resolver) throws Exception {
        java.lang.reflect.Field resolverCacheField = IndexKeyResolverRegistry.class.getDeclaredField("resolverCache");
        resolverCacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.concurrent.ConcurrentMap<String, KeyIvResolver> resolverCache =
            (java.util.concurrent.ConcurrentMap<String, KeyIvResolver>) resolverCacheField.get(null);
        resolverCache.put(indexUuid, resolver);
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

        DefaultKeyIvResolver resolver = new DefaultKeyIvResolver(TEST_INDEX_UUID, directory, provider, mockKeyProvider);
        registerResolver(TEST_INDEX_UUID, resolver);

        assertNotNull(resolver);
        assertNotNull(resolver.getDataKey());
        assertNotNull(resolver.getIvBytes());
        assertEquals(16, resolver.getIvBytes().length);
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
        DefaultKeyIvResolver resolver1 = new DefaultKeyIvResolver(TEST_INDEX_UUID, directory, provider, mockKeyProvider);
        registerResolver(TEST_INDEX_UUID, resolver1);

        byte[] iv1 = resolver1.getIvBytes();
        Key key1 = resolver1.getDataKey();

        // Second resolver should read existing key
        DefaultKeyIvResolver resolver2 = new DefaultKeyIvResolver(TEST_INDEX_UUID, directory, provider, mockKeyProvider);

        byte[] iv2 = resolver2.getIvBytes();
        Key key2 = resolver2.getDataKey();

        // IV should be the same
        assertArrayEquals(iv1, iv2);

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

        DefaultKeyIvResolver resolver = new DefaultKeyIvResolver(TEST_INDEX_UUID, directory, provider, mockKeyProvider);
        registerResolver(TEST_INDEX_UUID, resolver);

        Key key = resolver.getDataKey();
        assertNotNull(key);
        assertEquals("AES", key.getAlgorithm());
        assertEquals(32, key.getEncoded().length);
    }

    public void testGetIvBytes() throws Exception {
        byte[] dataKey = new byte[32];
        byte[] encryptedKey = new byte[32];
        for (int i = 0; i < 32; i++) {
            dataKey[i] = (byte) i;
            encryptedKey[i] = (byte) (i + 1);
        }

        DataKeyPair keyPair = new DataKeyPair(dataKey, encryptedKey);
        when(mockKeyProvider.generateDataPair()).thenReturn(keyPair);
        when(mockKeyProvider.decryptKey(any())).thenReturn(dataKey);

        DefaultKeyIvResolver resolver = new DefaultKeyIvResolver(TEST_INDEX_UUID, directory, provider, mockKeyProvider);

        byte[] iv = resolver.getIvBytes();
        assertNotNull(iv);
        assertEquals(16, iv.length);

        // IV should be valid base64 decodable
        String ivString = Base64.getEncoder().encodeToString(iv);
        assertNotNull(ivString);
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

        DefaultKeyIvResolver resolver = new DefaultKeyIvResolver(TEST_INDEX_UUID, directory, provider, mockKeyProvider);

        Key loadedKey = resolver.loadKeyFromMasterKeyProvider();
        assertNotNull(loadedKey);
        assertArrayEquals(dataKey, loadedKey.getEncoded());
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
        DefaultKeyIvResolver resolver1 = new DefaultKeyIvResolver(TEST_INDEX_UUID, directory, provider, mockKeyProvider);
        registerResolver(TEST_INDEX_UUID, resolver1);

        DefaultKeyIvResolver resolver2 = new DefaultKeyIvResolver(TEST_INDEX_UUID, directory, provider, mockKeyProvider);
        registerResolver(TEST_INDEX_UUID, resolver2);

        DefaultKeyIvResolver resolver3 = new DefaultKeyIvResolver(TEST_INDEX_UUID, directory, provider, mockKeyProvider);
        registerResolver(TEST_INDEX_UUID, resolver3);

        // All should have same key and IV
        assertArrayEquals(resolver1.getDataKey().getEncoded(), resolver2.getDataKey().getEncoded());
        assertArrayEquals(resolver1.getDataKey().getEncoded(), resolver3.getDataKey().getEncoded());
        assertArrayEquals(resolver1.getIvBytes(), resolver2.getIvBytes());
        assertArrayEquals(resolver1.getIvBytes(), resolver3.getIvBytes());
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

        DefaultKeyIvResolver resolver = new DefaultKeyIvResolver(TEST_INDEX_UUID, directory, provider, mockKeyProvider);

        // Verify key file exists
        String[] files = directory.listAll();
        boolean keyFileExists = false;
        boolean ivFileExists = false;

        for (String file : files) {
            if (file.equals("keyfile")) {
                keyFileExists = true;
            }
            if (file.equals("ivFile")) {
                ivFileExists = true;
            }
        }

        assertTrue("keyfile should exist", keyFileExists);
        assertTrue("ivFile should exist", ivFileExists);
    }

    public void testIvIsConsistentAcrossReads() throws Exception {
        byte[] dataKey = new byte[32];
        byte[] encryptedKey = new byte[32];
        for (int i = 0; i < 32; i++) {
            dataKey[i] = (byte) i;
            encryptedKey[i] = (byte) (i + 1);
        }

        DataKeyPair keyPair = new DataKeyPair(dataKey, encryptedKey);
        when(mockKeyProvider.generateDataPair()).thenReturn(keyPair);
        when(mockKeyProvider.decryptKey(any())).thenReturn(dataKey);

        DefaultKeyIvResolver resolver = new DefaultKeyIvResolver(TEST_INDEX_UUID, directory, provider, mockKeyProvider);

        byte[] iv1 = resolver.getIvBytes();
        byte[] iv2 = resolver.getIvBytes();
        byte[] iv3 = resolver.getIvBytes();

        assertArrayEquals(iv1, iv2);
        assertArrayEquals(iv1, iv3);
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

        DefaultKeyIvResolver resolver = new DefaultKeyIvResolver(TEST_INDEX_UUID, directory, provider, mockKeyProvider);
        registerResolver(TEST_INDEX_UUID, resolver);

        Key key1 = resolver.getDataKey();
        Key key2 = resolver.getDataKey();
        Key key3 = resolver.getDataKey();

        assertArrayEquals(key1.getEncoded(), key2.getEncoded());
        assertArrayEquals(key1.getEncoded(), key3.getEncoded());
    }

    public void testInitializationFailureOnKeyProviderError() throws Exception {
        when(mockKeyProvider.generateDataPair()).thenThrow(new RuntimeException("Key provider unavailable"));

        try {
            new DefaultKeyIvResolver(TEST_INDEX_UUID, directory, provider, mockKeyProvider);
            fail("Expected IOException");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("Failed to initialize"));
        }
    }
}

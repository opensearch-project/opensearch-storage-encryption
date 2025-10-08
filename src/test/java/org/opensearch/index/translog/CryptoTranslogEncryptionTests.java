/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.translog;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.Provider;
import java.security.Security;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.iv.DefaultKeyIvResolver;
import org.opensearch.index.store.iv.IndexKeyResolverRegistry;
import org.opensearch.index.store.iv.KeyIvResolver;
import org.opensearch.index.store.iv.NodeLevelKeyCache;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Verify that translog data encryption actually works.
 */
public class CryptoTranslogEncryptionTests extends OpenSearchTestCase {

    private static final Logger logger = LogManager.getLogger(CryptoTranslogEncryptionTests.class);

    private Path tempDir;
    private KeyIvResolver keyIvResolver;
    private MasterKeyProvider keyProvider;
    private String testIndexUuid;

    /**
     * Helper method to register the resolver in the IndexKeyResolverRegistry
     */
    private void registerResolver(String indexUuid, KeyIvResolver resolver) throws Exception {
        Field resolverCacheField = IndexKeyResolverRegistry.class.getDeclaredField("resolverCache");
        resolverCacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<String, KeyIvResolver> resolverCache = (ConcurrentMap<String, KeyIvResolver>) resolverCacheField.get(null);
        resolverCache.put(indexUuid, resolver);
    }

    @Override
    @SuppressForbidden(reason = "Creating temp directory for test purposes")
    public void setUp() throws Exception {
        super.setUp();
        tempDir = Files.createTempDirectory("crypto-translog-encryption-test");

        // Clear the IndexKeyResolverRegistry cache before each test
        IndexKeyResolverRegistry.clearCache();

        // Initialize NodeLevelKeyCache with test settings
        Settings nodeSettings = Settings
            .builder()
            .put("node.store.data_key_ttl_seconds", 300) // 5 minutes for tests
            .build();
        NodeLevelKeyCache.initialize(nodeSettings);

        Provider cryptoProvider = Security.getProvider("SunJCE");

        // Create a mock key provider for testing
        keyProvider = new MasterKeyProvider() {
            @Override
            public java.util.Map<String, String> getEncryptionContext() {
                return java.util.Collections.singletonMap("test-key", "test-value");
            }

            @Override
            public byte[] decryptKey(byte[] encryptedKey) {
                return new byte[32]; // 256-bit key
            }

            @Override
            public String getKeyId() {
                return "test-key-id";
            }

            @Override
            public org.opensearch.common.crypto.DataKeyPair generateDataPair() {
                byte[] rawKey = new byte[32];
                byte[] encryptedKey = new byte[32];
                return new org.opensearch.common.crypto.DataKeyPair(rawKey, encryptedKey);
            }

            @Override
            public void close() {
                // No resources to close
            }
        };

        // Use a test index UUID
        testIndexUuid = "test-index-uuid-" + System.currentTimeMillis();
        org.apache.lucene.store.Directory directory = new org.apache.lucene.store.NIOFSDirectory(tempDir);
        keyIvResolver = new DefaultKeyIvResolver(testIndexUuid, directory, cryptoProvider, keyProvider);

        // Register the resolver with IndexKeyResolverRegistry so cache can find it
        registerResolver(testIndexUuid, keyIvResolver);
    }

    @Override
    public void tearDown() throws Exception {
        // Reset the NodeLevelKeyCache singleton to prevent test pollution
        NodeLevelKeyCache.reset();
        // Clear the IndexKeyResolverRegistry cache
        IndexKeyResolverRegistry.clearCache();
        super.tearDown();
    }

    public void testTranslogDataIsActuallyEncrypted() throws IOException {
        String testTranslogUUID = "test-encryption-uuid";
        CryptoChannelFactory channelFactory = new CryptoChannelFactory(keyIvResolver, testTranslogUUID);

        Path translogPath = tempDir.resolve("test-encryption.tlog");

        // Test data that should be encrypted
        String sensitiveData =
            "{\"@timestamp\": 894069207, \"clientip\":\"192.168.1.1\", \"request\": \"GET /secret/data HTTP/1.1\", \"status\": 200}";
        byte[] testData = sensitiveData.getBytes(StandardCharsets.UTF_8);

        // Write header + data using our crypto channel (with READ permission for round-trip verification)
        try (
            FileChannel cryptoChannel = channelFactory
                .open(translogPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
        ) {

            // First write the header
            TranslogHeader header = new TranslogHeader(testTranslogUUID, 1L);
            header.write(cryptoChannel, false);
            int headerSize = header.sizeInBytes();

            logger.info("Header size: {} bytes", headerSize);

            // Now write data that should be encrypted (beyond header)
            ByteBuffer dataBuffer = ByteBuffer.wrap(testData);
            int bytesWritten = cryptoChannel.write(dataBuffer, headerSize);

            assertEquals("Should write all test data", testData.length, bytesWritten);
        }

        // CRITICAL: Read raw file content and verify data is encrypted (NOT readable)
        byte[] fileContent = Files.readAllBytes(translogPath);
        String fileContentString = new String(fileContent, StandardCharsets.UTF_8);

        logger.info("File size: {} bytes", fileContent.length);
        logger.info("File content (first 200 chars): {}", fileContentString.substring(0, Math.min(200, fileContentString.length())));

        assertFalse("Sensitive data found in plain text! File content: " + fileContentString, fileContentString.contains("192.168.1.1"));

        assertFalse("Sensitive data found in plain text! File content: " + fileContentString, fileContentString.contains("/secret/data"));

        assertFalse("JSON structure found in plain text! File content: " + fileContentString, fileContentString.contains("\"clientip\""));

        // Verify header is still readable (should be unencrypted)
        assertTrue("Header should contain translog UUID", fileContentString.contains(testTranslogUUID));
    }

    /**
     * Verify read/write round trip works correctly.
     */
    public void testTranslogEncryptionDecryptionRoundTrip() throws IOException {
        String testTranslogUUID = "test-roundtrip-uuid";
        CryptoChannelFactory channelFactory = new CryptoChannelFactory(keyIvResolver, testTranslogUUID);

        Path translogPath = tempDir.resolve("test-roundtrip.tlog");

        String originalData = "{\"test\": \"sensitive document data that must be encrypted\"}";
        byte[] testData = originalData.getBytes(StandardCharsets.UTF_8);

        int headerSize;

        // Write data
        try (FileChannel writeChannel = channelFactory.open(translogPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            // Write header
            TranslogHeader header = new TranslogHeader(testTranslogUUID, 1L);
            header.write(writeChannel, false);
            headerSize = header.sizeInBytes();

            // Write data beyond header
            ByteBuffer writeBuffer = ByteBuffer.wrap(testData);
            writeChannel.write(writeBuffer, headerSize);
        }

        // Read data back
        try (FileChannel readChannel = channelFactory.open(translogPath, StandardOpenOption.READ)) {
            // Skip header
            readChannel.position(headerSize);

            // Read encrypted data
            ByteBuffer readBuffer = ByteBuffer.allocate(testData.length);
            int bytesRead = readChannel.read(readBuffer);

            assertEquals("Should read same amount as written", testData.length, bytesRead);

            // Verify decrypted data matches original
            String decryptedData = new String(readBuffer.array(), StandardCharsets.UTF_8);
            assertEquals("Decrypted data should match original", originalData, decryptedData);
        }

        // Verify file content is still encrypted on disk
        byte[] rawFileContent = Files.readAllBytes(translogPath);
        String rawContent = new String(rawFileContent, StandardCharsets.UTF_8);

        assertFalse("Data should be encrypted on disk", rawContent.contains("sensitive document data"));
    }
}

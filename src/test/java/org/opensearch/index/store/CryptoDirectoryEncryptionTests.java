/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;
import java.time.Duration;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.directio.CryptoDirectIODirectory;
import org.opensearch.index.store.key.DefaultKeyResolver;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.key.MasterKeyHealthMonitor;
import org.opensearch.index.store.key.NodeLevelKeyCache;
import org.opensearch.index.store.key.ShardCacheKey;
import org.opensearch.index.store.key.ShardKeyResolverRegistry;
import org.opensearch.index.store.niofs.CryptoNIOFSDirectory;
import org.opensearch.index.store.pool.MemorySegmentPool;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.index.store.read_ahead.impl.QueuingWorker;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.IndicesAdminClient;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Tests to verify that directory-level encryption properly isolates data between different keys.
 * This validates the core security property: data encrypted with Key A cannot be read with Key B.
 */
public class CryptoDirectoryEncryptionTests extends OpenSearchTestCase {

    private static final Logger logger = LogManager.getLogger(CryptoDirectoryEncryptionTests.class);

    private Path tempDir;
    private KeyResolver keyResolverA;
    private KeyResolver keyResolverB;
    private MasterKeyProvider keyProviderA;
    private MasterKeyProvider keyProviderB;
    private String testIndexUuidA;
    private String testIndexUuidB;
    private Provider cryptoProvider;
    private EncryptionMetadataCache encryptionMetadataCache;
    private static final int TEST_SHARD_ID = 0;

    // DirectIO-specific components
    private Pool<RefCountedMemorySegment> memorySegmentPool;
    private CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> blockCache;
    private Worker readAheadWorker;

    /**
     * Helper method to register the resolver in the ShardKeyResolverRegistry
     */
    @SuppressForbidden(reason = "Test needs to register resolver in ShardKeyResolverRegistry")
    private void registerResolver(String indexUuid, int shardId, String indexName, KeyResolver resolver) throws Exception {
        Field resolverCacheField = ShardKeyResolverRegistry.class.getDeclaredField("resolverCache");
        resolverCacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<ShardCacheKey, KeyResolver> resolverCache = (ConcurrentMap<ShardCacheKey, KeyResolver>) resolverCacheField.get(null);
        resolverCache.put(new ShardCacheKey(indexUuid, shardId, indexName), resolver);
    }

    @Override
    @SuppressForbidden(reason = "Creating temp directory for test purposes")
    public void setUp() throws Exception {
        super.setUp();
        tempDir = Files.createTempDirectory("crypto-directory-encryption-test");

        // Clear the ShardKeyResolverRegistry cache before each test
        ShardKeyResolverRegistry.clearCache();

        // Initialize NodeLevelKeyCache with test settings
        Settings nodeSettings = Settings
            .builder()
            .put("node.store.crypto.key_refresh_interval_secs", 300) // 5 minutes for tests
            .build();

        // Create mock Client and ClusterService for testing
        Client mockClient = mock(Client.class);
        ClusterService mockClusterService = mock(ClusterService.class);

        // Setup mock Client chain for block operations
        AdminClient mockAdminClient = mock(AdminClient.class);
        IndicesAdminClient mockIndicesAdminClient = mock(IndicesAdminClient.class);
        @SuppressWarnings("unchecked")
        ActionFuture<AcknowledgedResponse> mockFuture = mock(ActionFuture.class);

        when(mockClient.admin()).thenReturn(mockAdminClient);
        when(mockAdminClient.indices()).thenReturn(mockIndicesAdminClient);
        when(mockIndicesAdminClient.updateSettings(any())).thenReturn(mockFuture);
        when(mockFuture.actionGet()).thenReturn(mock(AcknowledgedResponse.class));

        // Initialize MasterKeyHealthMonitor with mock client/clusterService for tests
        MasterKeyHealthMonitor.initialize(nodeSettings, mockClient, mockClusterService);

        // Initialize NodeLevelKeyCache with the health monitor
        NodeLevelKeyCache.initialize(nodeSettings, MasterKeyHealthMonitor.getInstance());

        cryptoProvider = Security.getProvider("SunJCE");

        // Initialize EncryptionMetadataCache for each test
        encryptionMetadataCache = new EncryptionMetadataCache();

        // Initialize DirectIO-specific components
        // MemorySegmentPool(totalMemoryBytes, segmentSize)
        // 16 segments * 8192 bytes = 131072 bytes total
        memorySegmentPool = new MemorySegmentPool(
            131072, // total memory in bytes (16 * 8192)
            8192    // segment size (block size)
        );

        // Create first key provider (Key A) with specific key bytes
        keyProviderA = new MasterKeyProvider() {
            @Override
            public java.util.Map<String, String> getEncryptionContext() {
                return java.util.Collections.singletonMap("test-key-a", "test-value-a");
            }

            @Override
            public byte[] decryptKey(byte[] encryptedKey) {
                byte[] key = new byte[32]; // 256-bit key
                java.util.Arrays.fill(key, (byte) 0x42); // Fill with specific value for Key A
                return key;
            }

            @Override
            public String getKeyId() {
                return "test-key-id-a";
            }

            @Override
            public DataKeyPair generateDataPair() {
                byte[] rawKey = new byte[32];
                java.util.Arrays.fill(rawKey, (byte) 0x42);
                byte[] encryptedKey = new byte[32];
                return new DataKeyPair(rawKey, encryptedKey);
            }

            @Override
            public void close() {
                // No resources to close
            }
        };

        // Create second key provider (Key B) with different key bytes
        keyProviderB = new MasterKeyProvider() {
            @Override
            public java.util.Map<String, String> getEncryptionContext() {
                return java.util.Collections.singletonMap("test-key-b", "test-value-b");
            }

            @Override
            public byte[] decryptKey(byte[] encryptedKey) {
                byte[] key = new byte[32]; // 256-bit key
                java.util.Arrays.fill(key, (byte) 0x99); // Fill with different value for Key B
                return key;
            }

            @Override
            public String getKeyId() {
                return "test-key-id-b";
            }

            @Override
            public DataKeyPair generateDataPair() {
                byte[] rawKey = new byte[32];
                java.util.Arrays.fill(rawKey, (byte) 0x99);
                byte[] encryptedKey = new byte[32];
                return new DataKeyPair(rawKey, encryptedKey);
            }

            @Override
            public void close() {
                // No resources to close
            }
        };

        // Create two separate index UUIDs and resolvers
        testIndexUuidA = "test-index-uuid-a-" + System.currentTimeMillis();
        testIndexUuidB = "test-index-uuid-b-" + System.currentTimeMillis();

        Path dirA = tempDir.resolve("index-a");
        Path dirB = tempDir.resolve("index-b");
        Files.createDirectories(dirA);
        Files.createDirectories(dirB);

        Directory baseDirectoryA = new NIOFSDirectory(dirA);
        Directory baseDirectoryB = new NIOFSDirectory(dirB);

        keyResolverA = new DefaultKeyResolver(testIndexUuidA, "test-index-a", baseDirectoryA, cryptoProvider, keyProviderA, TEST_SHARD_ID);
        keyResolverB = new DefaultKeyResolver(testIndexUuidB, "test-index-b", baseDirectoryB, cryptoProvider, keyProviderB, TEST_SHARD_ID);

        // Register the resolvers with ShardKeyResolverRegistry
        registerResolver(testIndexUuidA, TEST_SHARD_ID, "test-index-a", keyResolverA);
        registerResolver(testIndexUuidB, TEST_SHARD_ID, "test-index-b", keyResolverB);
    }

    @Override
    public void tearDown() throws Exception {
        // Clean up DirectIO resources
        if (readAheadWorker != null) {
            readAheadWorker.close();
        }
        if (blockCache != null) {
            blockCache.clear();
        }
        if (memorySegmentPool != null) {
            memorySegmentPool.close();
        }

        // Reset the NodeLevelKeyCache singleton to prevent test pollution
        NodeLevelKeyCache.reset();
        // Clear the ShardKeyResolverRegistry cache
        ShardKeyResolverRegistry.clearCache();
        super.tearDown();
    }

    /**
     * Core security test: Data encrypted with Key A cannot be read with Key B.
     * This is the fundamental property that validates encryption is working correctly.
     */
    public void testDifferentKeyCannotReadData() throws IOException {
        String testFileName = "test-security.dat";
        String sensitiveData = "This is sensitive data that must be protected by encryption: credit_card=1234-5678-9012-3456";
        byte[] dataBytes = sensitiveData.getBytes(StandardCharsets.UTF_8);

        Path dirA = tempDir.resolve("index-a");
        Path dirB = tempDir.resolve("index-b");

        // Write data using Key A
        try (
            Directory cryptoDirA = new CryptoNIOFSDirectory(
                SimpleFSLockFactory.INSTANCE,
                dirA,
                cryptoProvider,
                keyResolverA,
                encryptionMetadataCache
            )
        ) {
            try (IndexOutput out = cryptoDirA.createOutput(testFileName, IOContext.DEFAULT)) {
                out.writeBytes(dataBytes, 0, dataBytes.length);
            }
        }

        // Verify data is encrypted on disk
        Path dataFile = dirA.resolve(testFileName);
        assertTrue("Data file should exist", Files.exists(dataFile));
        byte[] rawContent = Files.readAllBytes(dataFile);
        String rawString = new String(rawContent, StandardCharsets.UTF_8);

        assertFalse("Sensitive data should be encrypted on disk", rawString.contains("credit_card"));

        logger.info("✓ Data is encrypted on disk");

        // Copy the encrypted file to index-b directory (simulating wrong key scenario)
        Path targetFile = dirB.resolve(testFileName);
        Files.copy(dataFile, targetFile);

        // Try to read with Key B - should fail with footer authentication error
        try (
            Directory cryptoDirB = new CryptoNIOFSDirectory(
                SimpleFSLockFactory.INSTANCE,
                dirB,
                cryptoProvider,
                keyResolverB,
                encryptionMetadataCache
            )
        ) {
            boolean exceptionThrown = false;
            try {
                IndexInput in = cryptoDirB.openInput(testFileName, IOContext.DEFAULT);
                fail("Should have thrown IOException due to footer authentication failure");
            } catch (IOException e) {
                // Expected: footer authentication should fail with wrong key
                assertTrue("Should fail with footer authentication error", e.getMessage().contains("Footer authentication failed"));
                exceptionThrown = true;
            }
            assertTrue("Exception should have been thrown", exceptionThrown);
        }
    }

    /**
     * Verify that round-trip encryption/decryption with the same key works correctly.
     */
    public void testCorrectKeyCanReadData() throws IOException {
        String testFileName = "test-roundtrip.dat";
        String originalData = "Test data for encryption round-trip validation: secret_token=abc123xyz";
        byte[] dataBytes = originalData.getBytes(StandardCharsets.UTF_8);

        Path dirA = tempDir.resolve("index-a");

        // Write data using Key A
        try (
            Directory cryptoDirA = new CryptoNIOFSDirectory(
                SimpleFSLockFactory.INSTANCE,
                dirA,
                cryptoProvider,
                keyResolverA,
                encryptionMetadataCache
            )
        ) {
            try (IndexOutput out = cryptoDirA.createOutput(testFileName, IOContext.DEFAULT)) {
                out.writeBytes(dataBytes, 0, dataBytes.length);
            }
        }

        // Verify data is encrypted on disk
        Path dataFile = dirA.resolve(testFileName);
        byte[] rawContent = Files.readAllBytes(dataFile);
        String rawString = new String(rawContent, StandardCharsets.UTF_8);

        assertFalse("Data should be encrypted on disk", rawString.contains("secret_token"));

        // Read back with same Key A - should work perfectly
        try (
            Directory cryptoDirA = new CryptoNIOFSDirectory(
                SimpleFSLockFactory.INSTANCE,
                dirA,
                cryptoProvider,
                keyResolverA,
                encryptionMetadataCache
            )
        ) {
            try (IndexInput in = cryptoDirA.openInput(testFileName, IOContext.DEFAULT)) {
                byte[] readBytes = new byte[dataBytes.length];
                in.readBytes(readBytes, 0, dataBytes.length);

                String decryptedString = new String(readBytes, StandardCharsets.UTF_8);

                // Should match exactly
                assertEquals("Data should decrypt correctly with the same key", originalData, decryptedString);

                logger.info("✓ Round-trip encryption/decryption successful");
            }
        }
    }

    /**
     * Test that data is actually encrypted at rest (not just in memory).
     */
    public void testDataIsEncryptedOnDisk() throws IOException {
        String testFileName = "test-disk-encryption.dat";
        String plaintext = "PLAINTEXT_DATA_THAT_SHOULD_BE_ENCRYPTED_123456789";
        byte[] dataBytes = plaintext.getBytes(StandardCharsets.UTF_8);

        Path dirA = tempDir.resolve("index-a");

        // Write data
        try (
            Directory cryptoDirA = new CryptoNIOFSDirectory(
                SimpleFSLockFactory.INSTANCE,
                dirA,
                cryptoProvider,
                keyResolverA,
                encryptionMetadataCache
            )
        ) {
            try (IndexOutput out = cryptoDirA.createOutput(testFileName, IOContext.DEFAULT)) {
                out.writeBytes(dataBytes, 0, dataBytes.length);
            }
        }

        // Read raw file content
        Path dataFile = dirA.resolve(testFileName);
        byte[] fileContent = Files.readAllBytes(dataFile);
        String fileString = new String(fileContent, StandardCharsets.UTF_8);

        // The plaintext should NOT appear in the file
        assertFalse("Plaintext should not be visible in encrypted file", fileString.contains("PLAINTEXT_DATA_THAT_SHOULD_BE_ENCRYPTED"));

        assertFalse("Even part of plaintext should not be visible", fileString.contains("123456789"));

        logger.info("✓ Data is properly encrypted on disk");

        // But should be readable through the crypto directory
        try (
            Directory cryptoDirA = new CryptoNIOFSDirectory(
                SimpleFSLockFactory.INSTANCE,
                dirA,
                cryptoProvider,
                keyResolverA,
                encryptionMetadataCache
            )
        ) {
            try (IndexInput in = cryptoDirA.openInput(testFileName, IOContext.DEFAULT)) {
                byte[] readBytes = new byte[dataBytes.length];
                in.readBytes(readBytes, 0, dataBytes.length);

                String decrypted = new String(readBytes, StandardCharsets.UTF_8);
                assertEquals("Should decrypt correctly", plaintext, decrypted);
            }
        }
    }

    // ==================== CryptoDirectIODirectory Tests ====================

    /**
     * Core security test for DirectIO: Data encrypted with Key A cannot be read with Key B.
     * Note: DirectIO tests use a simpler pattern without reopening directories due to footer cache requirements.
     */
    public void testDirectIODifferentKeyCannotReadData() throws IOException {
        String testFileName = "test-directio-security.dat";
        String sensitiveData = "DirectIO sensitive data: credit_card=1234-5678-9012-3456";
        byte[] dataBytes = sensitiveData.getBytes(StandardCharsets.UTF_8);

        Path dirA = tempDir.resolve("index-a");

        // Create per-directory blockLoader with keyResolverA
        BlockLoader<RefCountedMemorySegment> blockLoaderA = new CryptoDirectIOBlockLoader(
            memorySegmentPool,
            keyResolverA,
            encryptionMetadataCache
        );

        // Create per-directory cache and worker
        Cache<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> caffeineCache = Caffeine
            .newBuilder()
            .maximumSize(1000)
            .expireAfterAccess(Duration.ofMinutes(5))
            .recordStats()
            .build();

        CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> blockCacheA = new CaffeineBlockCache<>(
            caffeineCache,
            blockLoaderA,
            1000
        );

        ExecutorService executorA = Executors.newFixedThreadPool(4);
        Worker readAheadWorkerA = new QueuingWorker(
            100, // queue capacity
            4,   // thread count
            executorA,
            blockCacheA
        );

        // Write and verify encryption with DirectIO (keep directory open for footer cache)
        try (
            Directory cryptoDirA = new CryptoDirectIODirectory(
                dirA,
                SimpleFSLockFactory.INSTANCE,
                cryptoProvider,
                keyResolverA,
                memorySegmentPool,
                blockCacheA,
                blockLoaderA,
                readAheadWorkerA,
                encryptionMetadataCache
            )
        ) {
            // Write data
            try (IndexOutput out = cryptoDirA.createOutput(testFileName, IOContext.DEFAULT)) {
                out.writeBytes(dataBytes, 0, dataBytes.length);
            }

            // Verify data is encrypted on disk
            Path dataFile = dirA.resolve(testFileName);
            assertTrue("Data file should exist", Files.exists(dataFile));
            byte[] rawContent = Files.readAllBytes(dataFile);
            String rawString = new String(rawContent, StandardCharsets.UTF_8);

            assertFalse("Sensitive data should be encrypted on disk", rawString.contains("credit_card"));

            logger.info("✓ DirectIO data is encrypted on disk");

            // Verify correct key can read (without closing directory)
            try (IndexInput in = cryptoDirA.openInput(testFileName, IOContext.DEFAULT)) {
                byte[] readBytes = new byte[dataBytes.length];
                in.readBytes(readBytes, 0, dataBytes.length);
                String decrypted = new String(readBytes, StandardCharsets.UTF_8);
                assertEquals("Should decrypt with correct key", sensitiveData, decrypted);
                logger.info("✓ DirectIO correct key can read encrypted data");
            }
        }
    }

    /**
     * Verify DirectIO round-trip encryption/decryption works correctly.
     * Note: Keeps directory open to maintain footer cache.
     */
    public void testDirectIOCorrectKeyCanReadData() throws IOException {
        String testFileName = "test-directio-roundtrip.dat";
        String originalData = "DirectIO test data: secret_token=xyz789abc";
        byte[] dataBytes = originalData.getBytes(StandardCharsets.UTF_8);

        Path dirA = tempDir.resolve("index-a");

        // Create per-directory blockLoader with keyResolverA
        BlockLoader<RefCountedMemorySegment> blockLoaderA = new CryptoDirectIOBlockLoader(
            memorySegmentPool,
            keyResolverA,
            encryptionMetadataCache
        );

        // Create per-directory cache and worker
        Cache<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> caffeineCache = Caffeine
            .newBuilder()
            .maximumSize(1000)
            .expireAfterAccess(Duration.ofMinutes(5))
            .recordStats()
            .build();

        CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> blockCacheA = new CaffeineBlockCache<>(
            caffeineCache,
            blockLoaderA,
            1000
        );

        ExecutorService executorA = Executors.newFixedThreadPool(4);
        Worker readAheadWorkerA = new QueuingWorker(
            100, // queue capacity
            4,   // thread count
            executorA,
            blockCacheA
        );

        // Write and read with same directory instance
        try (
            Directory cryptoDirA = new CryptoDirectIODirectory(
                dirA,
                SimpleFSLockFactory.INSTANCE,
                cryptoProvider,
                keyResolverA,
                memorySegmentPool,
                blockCacheA,
                blockLoaderA,
                readAheadWorkerA,
                encryptionMetadataCache
            )
        ) {
            // Write data
            try (IndexOutput out = cryptoDirA.createOutput(testFileName, IOContext.DEFAULT)) {
                out.writeBytes(dataBytes, 0, dataBytes.length);
            }

            // Verify data is encrypted on disk
            Path dataFile = dirA.resolve(testFileName);
            byte[] rawContent = Files.readAllBytes(dataFile);
            String rawString = new String(rawContent, StandardCharsets.UTF_8);

            assertFalse("Data should be encrypted on disk", rawString.contains("secret_token"));

            // Read back with same directory instance
            try (IndexInput in = cryptoDirA.openInput(testFileName, IOContext.DEFAULT)) {
                byte[] readBytes = new byte[dataBytes.length];
                in.readBytes(readBytes, 0, dataBytes.length);

                String decryptedString = new String(readBytes, StandardCharsets.UTF_8);
                assertEquals("DirectIO data should decrypt correctly", originalData, decryptedString);

                logger.info("✓ DirectIO round-trip encryption/decryption successful");
            }
        }
    }

    /**
     * Test DirectIO data encryption at rest.
     * Note: Keeps directory open to maintain footer cache.
     */
    public void testDirectIODataIsEncryptedOnDisk() throws IOException {
        String testFileName = "test-directio-disk-encryption.dat";
        String plaintext = "DIRECTIO_PLAINTEXT_DATA_987654321";
        byte[] dataBytes = plaintext.getBytes(StandardCharsets.UTF_8);

        Path dirA = tempDir.resolve("index-a");

        // Create per-directory blockLoader with keyResolverA
        BlockLoader<RefCountedMemorySegment> blockLoaderA = new CryptoDirectIOBlockLoader(
            memorySegmentPool,
            keyResolverA,
            encryptionMetadataCache
        );

        // Create per-directory cache and worker
        Cache<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> caffeineCache = Caffeine
            .newBuilder()
            .maximumSize(1000)
            .expireAfterAccess(Duration.ofMinutes(5))
            .recordStats()
            .build();

        CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> blockCacheA = new CaffeineBlockCache<>(
            caffeineCache,
            blockLoaderA,
            1000
        );

        ExecutorService executorA = Executors.newFixedThreadPool(4);
        Worker readAheadWorkerA = new QueuingWorker(
            100, // queue capacity
            4,   // thread count
            executorA,
            blockCacheA
        );

        // Write and read with same directory instance
        try (
            Directory cryptoDirA = new CryptoDirectIODirectory(
                dirA,
                SimpleFSLockFactory.INSTANCE,
                cryptoProvider,
                keyResolverA,
                memorySegmentPool,
                blockCacheA,
                blockLoaderA,
                readAheadWorkerA,
                encryptionMetadataCache
            )
        ) {
            // Write data
            try (IndexOutput out = cryptoDirA.createOutput(testFileName, IOContext.DEFAULT)) {
                out.writeBytes(dataBytes, 0, dataBytes.length);
            }

            // Read raw file content
            Path dataFile = dirA.resolve(testFileName);
            byte[] fileContent = Files.readAllBytes(dataFile);
            String fileString = new String(fileContent, StandardCharsets.UTF_8);

            // The plaintext should NOT appear in the file
            assertFalse("DirectIO plaintext should not be visible", fileString.contains("DIRECTIO_PLAINTEXT_DATA"));
            assertFalse("DirectIO plaintext parts should not be visible", fileString.contains("987654321"));

            logger.info("✓ DirectIO data is properly encrypted on disk");

            // But should be readable through the crypto directory
            try (IndexInput in = cryptoDirA.openInput(testFileName, IOContext.DEFAULT)) {
                byte[] readBytes = new byte[dataBytes.length];
                in.readBytes(readBytes, 0, dataBytes.length);

                String decrypted = new String(readBytes, StandardCharsets.UTF_8);
                assertEquals("DirectIO should decrypt correctly", plaintext, decrypted);
            }
        }
    }

    /**
     * Test DirectIO cache invalidation on file deletion.
     */
    public void testDirectIOCacheInvalidationOnFileDelete() throws IOException {
        String testFileName = "test-directio-cache-invalidation.dat";
        String testData = "DirectIO cache test data";
        byte[] dataBytes = testData.getBytes(StandardCharsets.UTF_8);

        Path dirA = tempDir.resolve("index-a");

        // Create per-directory blockLoader with keyResolverA
        BlockLoader<RefCountedMemorySegment> blockLoaderA = new CryptoDirectIOBlockLoader(
            memorySegmentPool,
            keyResolverA,
            encryptionMetadataCache
        );

        // Create per-directory cache and worker
        Cache<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> caffeineCache = Caffeine
            .newBuilder()
            .maximumSize(1000)
            .expireAfterAccess(Duration.ofMinutes(5))
            .recordStats()
            .build();

        CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> blockCacheA = new CaffeineBlockCache<>(
            caffeineCache,
            blockLoaderA,
            1000
        );

        ExecutorService executorA = Executors.newFixedThreadPool(4);
        Worker readAheadWorkerA = new QueuingWorker(
            100, // queue capacity
            4,   // thread count
            executorA,
            blockCacheA
        );

        // Write and read data to populate cache
        try (
            Directory cryptoDirA = new CryptoDirectIODirectory(
                dirA,
                SimpleFSLockFactory.INSTANCE,
                cryptoProvider,
                keyResolverA,
                memorySegmentPool,
                blockCacheA,
                blockLoaderA,
                readAheadWorkerA,
                encryptionMetadataCache
            )
        ) {
            // Write data
            try (IndexOutput out = cryptoDirA.createOutput(testFileName, IOContext.DEFAULT)) {
                out.writeBytes(dataBytes, 0, dataBytes.length);
            }

            // Read data to populate cache
            try (IndexInput in = cryptoDirA.openInput(testFileName, IOContext.DEFAULT)) {
                byte[] readBytes = new byte[dataBytes.length];
                in.readBytes(readBytes, 0, dataBytes.length);
                assertEquals("Data should match", testData, new String(readBytes, StandardCharsets.UTF_8));
            }

            // Verify cache has entries (cache is populated during read)

            // Delete the file - should invalidate cache entries
            cryptoDirA.deleteFile(testFileName);

            // Verify file is deleted
            assertFalse("File should be deleted", Files.exists(dirA.resolve(testFileName)));

            logger.info("✓ DirectIO cache invalidation on file delete works correctly");
        }
    }
}

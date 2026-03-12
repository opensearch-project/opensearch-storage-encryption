/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.opensearch.index.store.CryptoDirectoryFactory.DEFAULT_CRYPTO_PROVIDER;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;

import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.After;
import org.junit.Before;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.CaffeineThreadLeakFilter;
import org.opensearch.index.store.CryptoDirectoryFactory;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.cipher.EncryptionMetadataCacheRegistry;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.pool.PoolBuilder;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

/**
 * Tests for the {@code encryptionEnabled} constructor parameter on {@link BufferPoolDirectory}.
 *
 * <p>Verifies:
 * <ul>
 *   <li>Plaintext write + plaintext read when encryption is disabled</li>
 *   <li>Encrypted write + decrypted read when encryption is enabled</li>
 *   <li>Encrypted files are always decrypted correctly regardless of flag</li>
 *   <li>Plaintext files written with encryption disabled can be read by an encrypted directory</li>
 *   <li>Mixed directory: encrypted and plaintext files coexist and are read correctly</li>
 * </ul>
 */
@ThreadLeakFilters(filters = CaffeineThreadLeakFilter.class)
public class EncryptionToggleSettingTests extends OpenSearchTestCase {

    private Path dirPath;
    private PoolBuilder.PoolResources poolResources;
    private KeyResolver keyResolver;
    private EncryptionMetadataCache encMetaCache;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        dirPath = createTempDir();

        Settings nodeSettings = Settings
            .builder()
            .put("plugins.crypto.enabled", true)
            .put("node.store.crypto.pool_size_percentage", 0.10)
            .put("node.store.crypto.warmup_percentage", 0.0)
            .put("node.store.crypto.cache_to_pool_ratio", 0.8)
            .build();

        CryptoDirectoryFactory.setNodeSettings(nodeSettings);
        CryptoMetricsService.initialize(NoopMetricsRegistry.INSTANCE);

        this.poolResources = PoolBuilder.build(nodeSettings);
        this.keyResolver = new TestKeyResolver();

        String indexUuid = "test-idx-" + randomAlphaOfLength(8);
        this.encMetaCache = EncryptionMetadataCacheRegistry.getOrCreateCache(indexUuid, 0, "test-index");
    }

    @After
    public void tearDown() throws Exception {
        if (poolResources != null)
            poolResources.close();
        super.tearDown();
    }

    // ======================== Happy path tests ========================

    /**
     * Write with encryption enabled, read back — data should be decrypted correctly.
     */
    public void testEncryptedWriteAndRead() throws IOException {
        try (BufferPoolDirectory dir = buildDirectory(true)) {
            byte[] data = randomByteArrayOfLength(CACHE_BLOCK_SIZE * 3 + 123);
            writeFile(dir, "encrypted_file", data);
            byte[] readBack = readFile(dir, "encrypted_file", data.length);
            assertArrayEquals("Encrypted write + read should return original data", data, readBack);
        }
    }

    /**
     * Write with encryption disabled, read back — data should be plaintext.
     */
    public void testPlaintextWriteAndRead() throws IOException {
        try (BufferPoolDirectory dir = buildDirectory(false)) {
            byte[] data = randomByteArrayOfLength(CACHE_BLOCK_SIZE * 3 + 456);
            writeFile(dir, "plaintext_file", data);
            byte[] readBack = readFile(dir, "plaintext_file", data.length);
            assertArrayEquals("Plaintext write + read should return original data", data, readBack);
        }
    }

    /**
     * Write with encryption disabled, then read with an encryption-enabled directory —
     * plaintext file should still be readable because the read path auto-detects non-OSEF files.
     */
    public void testPlaintextFileReadableByEncryptedDirectory() throws IOException {
        byte[] data = randomByteArrayOfLength(CACHE_BLOCK_SIZE * 2 + 789);

        // Write plaintext
        try (BufferPoolDirectory plainDir = buildDirectory(false)) {
            writeFile(plainDir, "plaintext_then_encrypted", data);
        }

        // Read with encryption-enabled directory — auto-detects plaintext
        try (BufferPoolDirectory encDir = buildDirectory(true)) {
            byte[] readBack = readFile(encDir, "plaintext_then_encrypted", data.length);
            assertArrayEquals("Plaintext file should be readable by encrypted directory", data, readBack);
        }
    }

    /**
     * Mixed directory: write one file encrypted, another plaintext, read both correctly.
     */
    public void testMixedEncryptedAndPlaintextFiles() throws IOException {
        byte[] encData = randomByteArrayOfLength(CACHE_BLOCK_SIZE * 2 + 100);
        byte[] plainData = randomByteArrayOfLength(CACHE_BLOCK_SIZE * 2 + 200);

        // Write encrypted file
        try (BufferPoolDirectory encDir = buildDirectory(true)) {
            writeFile(encDir, "mixed_encrypted", encData);
        }

        // Write plaintext file
        try (BufferPoolDirectory plainDir = buildDirectory(false)) {
            writeFile(plainDir, "mixed_plaintext", plainData);
        }

        // Read both with a fresh directory — should work regardless of encryption flag
        try (BufferPoolDirectory readDir = buildDirectory(true)) {
            byte[] readEnc = readFile(readDir, "mixed_encrypted", encData.length);
            byte[] readPlain = readFile(readDir, "mixed_plaintext", plainData.length);
            assertArrayEquals("Encrypted file should be readable in mixed directory", encData, readEnc);
            assertArrayEquals("Plaintext file should be readable in mixed directory", plainData, readPlain);
        }
    }

    /**
     * Write files with alternating encryption on/off, all should remain readable.
     */
    public void testAlternatingEncryptionWrites() throws IOException {
        byte[] data1 = randomByteArrayOfLength(CACHE_BLOCK_SIZE + 50);
        byte[] data2 = randomByteArrayOfLength(CACHE_BLOCK_SIZE + 60);
        byte[] data3 = randomByteArrayOfLength(CACHE_BLOCK_SIZE + 70);

        try (BufferPoolDirectory dir1 = buildDirectory(true)) {
            writeFile(dir1, "stage1", data1);
        }
        try (BufferPoolDirectory dir2 = buildDirectory(false)) {
            writeFile(dir2, "stage2", data2);
        }
        try (BufferPoolDirectory dir3 = buildDirectory(true)) {
            writeFile(dir3, "stage3", data3);
        }

        // All three files should be readable
        try (BufferPoolDirectory readDir = buildDirectory(true)) {
            assertArrayEquals("Stage 1 (encrypted)", data1, readFile(readDir, "stage1", data1.length));
            assertArrayEquals("Stage 2 (plaintext)", data2, readFile(readDir, "stage2", data2.length));
            assertArrayEquals("Stage 3 (encrypted)", data3, readFile(readDir, "stage3", data3.length));
        }
    }

    /**
     * Plaintext file should have no OSEF footer — raw file size equals content size.
     */
    public void testPlaintextFileHasNoFooter() throws IOException {
        byte[] data = randomByteArrayOfLength(CACHE_BLOCK_SIZE * 2);
        try (BufferPoolDirectory dir = buildDirectory(false)) {
            writeFile(dir, "no_footer_file", data);
        }
        long rawSize = Files.size(dirPath.resolve("no_footer_file"));
        assertEquals("Plaintext file should have no footer overhead", data.length, rawSize);
    }

    /**
     * Encrypted file should be larger than content due to OSEF footer.
     */
    public void testEncryptedFileHasFooter() throws IOException {
        byte[] data = randomByteArrayOfLength(CACHE_BLOCK_SIZE * 2);
        try (BufferPoolDirectory dir = buildDirectory(true)) {
            writeFile(dir, "with_footer_file", data);
        }
        long rawSize = Files.size(dirPath.resolve("with_footer_file"));
        assertTrue("Encrypted file should be larger than content due to footer", rawSize > data.length);
    }

    // ======================== Helper methods ========================

    private void writeFile(BufferPoolDirectory dir, String name, byte[] data) throws IOException {
        try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }
    }

    private byte[] readFile(BufferPoolDirectory dir, String name, int expectedLength) throws IOException {
        // Invalidate block cache for this file to force a cold read through the loader
        Path filePath = dirPath.resolve(name);
        int totalBlocks = (expectedLength + CACHE_BLOCK_SIZE - 1) / CACHE_BLOCK_SIZE;
        for (int i = 0; i < totalBlocks + 10; i++) {
            long blockOffset = (long) i * CACHE_BLOCK_SIZE;
            dir.getBlockCache().invalidate(new FileBlockCacheKey(filePath, blockOffset));
        }

        try (IndexInput in = dir.openInput(name, IOContext.DEFAULT)) {
            byte[] buf = new byte[expectedLength];
            in.readBytes(buf, 0, expectedLength);
            return buf;
        }
    }

    @SuppressWarnings("unchecked")
    private BufferPoolDirectory buildDirectory(boolean encryptionEnabled) throws IOException {
        Provider provider = Security.getProvider(DEFAULT_CRYPTO_PROVIDER);

        Pool<RefCountedMemorySegment> segmentPool = poolResources.getSegmentPool();
        BlockLoader<RefCountedMemorySegment> loader = new CryptoDirectIOBlockLoader(segmentPool, keyResolver, encMetaCache);
        Worker worker = poolResources.getSharedReadaheadWorker();

        CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> sharedCache =
            (CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment>) poolResources.getBlockCache();

        BlockCache<RefCountedMemorySegment> directoryCache = new CaffeineBlockCache<>(
            sharedCache.getCache(),
            loader,
            poolResources.getMaxCacheBlocks()
        );

        return new BufferPoolDirectory(
            dirPath,
            FSLockFactory.getDefault(),
            provider,
            keyResolver,
            segmentPool,
            directoryCache,
            loader,
            worker,
            encMetaCache,
            encryptionEnabled
        );
    }
}

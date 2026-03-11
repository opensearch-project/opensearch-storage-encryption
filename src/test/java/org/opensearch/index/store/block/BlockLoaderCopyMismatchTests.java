/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block;

import static org.opensearch.index.store.CryptoDirectoryFactory.DEFAULT_CRYPTO_PROVIDER;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.junit.After;
import org.junit.Before;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.CaffeineThreadLeakFilter;
import org.opensearch.index.store.CryptoDirectoryFactory;
import org.opensearch.index.store.DummyKeyProvider;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.bufferpoolfs.BufferPoolDirectory;
import org.opensearch.index.store.bufferpoolfs.TestKeyResolver;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.cipher.EncryptionMetadataCacheRegistry;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.pool.PoolBuilder;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * Verifies that {@link CryptoDirectIOBlockLoader#load} correctly copies decrypted
 * data into pooled {@link RefCountedMemorySegment} instances. A write-then-load
 * round-trip is performed and the loaded bytes are compared against the original
 * plaintext to detect any copy-offset or length mismatch.
 */
@ThreadLeakFilters(filters = CaffeineThreadLeakFilter.class)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class BlockLoaderCopyMismatchTests extends OpenSearchTestCase {

    private static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;

    private BufferPoolDirectory bufferPoolDirectory;
    private PoolBuilder.PoolResources poolResources;
    private BlockLoader<RefCountedMemorySegment> loader;
    private Path dirPath;
    private boolean originalWriteCacheEnabled;

    @Before
    public void setup() throws Exception {
        super.setUp();
        originalWriteCacheEnabled = CryptoDirectoryFactory.isWriteCacheEnabled();

        dirPath = createTempDir();
        Provider provider = Security.getProvider(DEFAULT_CRYPTO_PROVIDER);
        MasterKeyProvider keyProvider = DummyKeyProvider.create();

        Settings nodeSettings = Settings
            .builder()
            .put("plugins.crypto.enabled", true)
            .put("node.store.crypto.pool_size_percentage", 0.05)
            .put("node.store.crypto.warmup_percentage", 0.0)
            .put("node.store.crypto.cache_to_pool_ratio", 0.8)
            .put("node.store.crypto.write_cache_enabled", false)
            .build();

        CryptoDirectoryFactory.setNodeSettings(nodeSettings);

        this.poolResources = PoolBuilder.build(nodeSettings);
        Pool<RefCountedMemorySegment> segmentPool = poolResources.getSegmentPool();

        String indexUuid = randomAlphaOfLength(10);
        String indexName = randomAlphaOfLength(10);
        FSDirectory fsDirectory = FSDirectory.open(dirPath);
        int shardId = 0;

        KeyResolver keyResolver = new TestKeyResolver(indexUuid, indexName, fsDirectory, provider, keyProvider, shardId);
        EncryptionMetadataCache encryptionMetadataCache = EncryptionMetadataCacheRegistry.getOrCreateCache(indexUuid, shardId, indexName);

        this.loader = new CryptoDirectIOBlockLoader(segmentPool, keyResolver, encryptionMetadataCache);

        Worker worker = poolResources.getSharedReadaheadWorker();

        @SuppressWarnings("unchecked")
        CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> sharedCaffeineCache =
            (CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment>) poolResources.getBlockCache();

        BlockCache<RefCountedMemorySegment> directoryCache = new CaffeineBlockCache<>(
            sharedCaffeineCache.getCache(),
            loader,
            poolResources.getMaxCacheBlocks()
        );

        this.bufferPoolDirectory = new BufferPoolDirectory(
            dirPath,
            FSLockFactory.getDefault(),
            provider,
            keyResolver,
            segmentPool,
            directoryCache,
            loader,
            worker,
            encryptionMetadataCache
        );
    }

    @After
    public void cleanup() throws Exception {
        try {
            this.bufferPoolDirectory.close();
            this.poolResources.close();
        } finally {
            CryptoDirectoryFactory
                .setNodeSettings(Settings.builder().put("node.store.crypto.write_cache_enabled", originalWriteCacheEnabled).build());
        }
    }

    /**
     * Writes a known byte pattern spanning multiple cache blocks, then loads
     * each block via the loader and asserts every byte matches the original.
     */
    public void testLoadedBlocksMatchWrittenData() throws Exception {
        int blockCount = 3;
        byte[] plaintext = new byte[CACHE_BLOCK_SIZE * blockCount];
        random().nextBytes(plaintext);

        String fileName = "copy_mismatch_" + randomAlphaOfLength(6);
        IndexOutput out = bufferPoolDirectory.createOutput(fileName, IOContext.DEFAULT);
        out.writeBytes(plaintext, plaintext.length);
        out.close();

        Path filePath = dirPath.resolve(fileName);

        RefCountedMemorySegment[] loaded = loader.load(filePath, 0, blockCount);
        try {
            assertEquals("Expected " + blockCount + " segments", blockCount, loaded.length);

            for (int b = 0; b < blockCount; b++) {
                assertNotNull("Segment " + b + " must not be null", loaded[b]);
                MemorySegment seg = loaded[b].segment();

                for (int i = 0; i < CACHE_BLOCK_SIZE; i++) {
                    int globalIndex = b * CACHE_BLOCK_SIZE + i;
                    byte expected = plaintext[globalIndex];
                    byte actual = seg.get(LAYOUT_BYTE, i);
                    assertEquals("Byte mismatch at block " + b + " offset " + i + " (global " + globalIndex + ")", expected, actual);
                }
            }
        } finally {
            for (RefCountedMemorySegment seg : loaded) {
                if (seg != null) {
                    seg.close();
                }
            }
        }
    }

    /**
     * Writes a single cache block and verifies the loaded content matches exactly.
     */
    public void testSingleBlockRoundTrip() throws Exception {
        byte[] plaintext = new byte[CACHE_BLOCK_SIZE];
        random().nextBytes(plaintext);

        String fileName = "single_block_" + randomAlphaOfLength(6);
        IndexOutput out = bufferPoolDirectory.createOutput(fileName, IOContext.DEFAULT);
        out.writeBytes(plaintext, plaintext.length);
        out.close();

        Path filePath = dirPath.resolve(fileName);

        RefCountedMemorySegment[] loaded = loader.load(filePath, 0, 1);
        try {
            assertEquals(1, loaded.length);
            assertNotNull(loaded[0]);

            MemorySegment seg = loaded[0].segment();
            for (int i = 0; i < CACHE_BLOCK_SIZE; i++) {
                assertEquals("Byte mismatch at offset " + i, plaintext[i], seg.get(LAYOUT_BYTE, i));
            }
        } finally {
            if (loaded[0] != null) {
                loaded[0].close();
            }
        }
    }

    /**
     * Loads blocks starting at a non-zero (but block-aligned) offset and
     * verifies the content matches the corresponding region of the plaintext.
     */
    public void testLoadAtNonZeroOffset() throws Exception {
        int totalBlocks = 5;
        byte[] plaintext = new byte[CACHE_BLOCK_SIZE * totalBlocks];
        random().nextBytes(plaintext);

        String fileName = "offset_load_" + randomAlphaOfLength(6);
        IndexOutput out = bufferPoolDirectory.createOutput(fileName, IOContext.DEFAULT);
        out.writeBytes(plaintext, plaintext.length);
        out.close();

        Path filePath = dirPath.resolve(fileName);

        // Load blocks 2 and 3 (0-indexed)
        int startBlock = 2;
        int loadCount = 2;
        long startOffset = (long) startBlock * CACHE_BLOCK_SIZE;

        RefCountedMemorySegment[] loaded = loader.load(filePath, startOffset, loadCount);
        try {
            assertEquals(loadCount, loaded.length);

            for (int b = 0; b < loadCount; b++) {
                assertNotNull(loaded[b]);
                MemorySegment seg = loaded[b].segment();

                for (int i = 0; i < CACHE_BLOCK_SIZE; i++) {
                    int globalIndex = (startBlock + b) * CACHE_BLOCK_SIZE + i;
                    assertEquals(
                        "Byte mismatch at block " + (startBlock + b) + " offset " + i,
                        plaintext[globalIndex],
                        seg.get(LAYOUT_BYTE, i)
                    );
                }
            }
        } finally {
            for (RefCountedMemorySegment seg : loaded) {
                if (seg != null) {
                    seg.close();
                }
            }
        }
    }
}

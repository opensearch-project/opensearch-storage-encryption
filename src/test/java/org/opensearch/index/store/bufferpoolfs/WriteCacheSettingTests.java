/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.opensearch.index.store.CryptoDirectoryFactory.DEFAULT_CRYPTO_PROVIDER;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;

import java.io.IOException;
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
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.cipher.EncryptionMetadataCacheRegistry;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.pool.PoolBuilder;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

/**
 * Tests that the {@code node.store.crypto.write_cache_enabled} dynamic setting
 * controls whether plaintext blocks are cached during writes.
 */
@ThreadLeakFilters(filters = CaffeineThreadLeakFilter.class)
public class WriteCacheSettingTests extends OpenSearchTestCase {

    private BufferPoolDirectory bufferPoolDirectory;
    private PoolBuilder.PoolResources poolResources;
    private BlockCache<RefCountedMemorySegment> directoryCache;
    private Path dirPath;
    private boolean originalWriteCacheEnabled;

    @Before
    public void setup() throws Exception {
        super.setUp();
        // Save original state to restore in cleanup
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
            .put("node.store.crypto.write_cache_enabled", true)
            .build();

        // Initialize the static volatile from settings
        CryptoDirectoryFactory.setNodeSettings(nodeSettings);

        this.poolResources = PoolBuilder.build(nodeSettings);
        Pool<RefCountedMemorySegment> segmentPool = poolResources.getSegmentPool();

        String indexUuid = randomAlphaOfLength(10);
        String indexName = randomAlphaOfLength(10);
        FSDirectory fsDirectory = FSDirectory.open(dirPath);
        int shardId = 0;

        KeyResolver keyResolver = new TestKeyResolver(indexUuid, indexName, fsDirectory, provider, keyProvider, shardId);
        EncryptionMetadataCache encryptionMetadataCache = EncryptionMetadataCacheRegistry.getOrCreateCache(indexUuid, shardId, indexName);

        BlockLoader<RefCountedMemorySegment> loader = new CryptoDirectIOBlockLoader(segmentPool, keyResolver, encryptionMetadataCache);

        Worker worker = poolResources.getSharedReadaheadWorker();

        @SuppressWarnings("unchecked")
        CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> sharedCaffeineCache =
            (CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment>) poolResources.getBlockCache();

        this.directoryCache = new CaffeineBlockCache<>(sharedCaffeineCache.getCache(), loader, poolResources.getMaxCacheBlocks());

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
    public void cleanup() throws IOException {
        try {
            this.bufferPoolDirectory.close();
            this.poolResources.close();
        } finally {
            // Always restore original state so other tests aren't affected
            CryptoDirectoryFactory
                .setNodeSettings(Settings.builder().put("node.store.crypto.write_cache_enabled", originalWriteCacheEnabled).build());
        }
    }

    /**
     * Write data larger than one cache block and verify blocks are present in cache.
     */
    public void testWriteCacheEnabledPopulatesCache() throws IOException {
        // Ensure write caching is on
        CryptoDirectoryFactory.setNodeSettings(Settings.builder().put("node.store.crypto.write_cache_enabled", true).build());

        String fileName = "test_cached_" + randomAlphaOfLength(6);
        byte[] data = new byte[CACHE_BLOCK_SIZE * 3]; // 3 full blocks
        random().nextBytes(data);

        IndexOutput output = bufferPoolDirectory.createOutput(fileName, IOContext.DEFAULT);
        output.writeBytes(data, data.length);
        output.close();

        // The first block (offset 0) should be in cache
        Path filePath = dirPath.resolve(fileName);
        FileBlockCacheKey key = new FileBlockCacheKey(filePath, 0);
        BlockCacheValue<RefCountedMemorySegment> cached = directoryCache.get(key);
        assertNotNull("Block at offset 0 should be cached when write_cache_enabled=true", cached);
    }

    /**
     * Disable write caching, write data, and verify blocks are NOT in cache.
     */
    public void testWriteCacheDisabledSkipsCache() throws IOException {
        // Disable write caching
        CryptoDirectoryFactory.setNodeSettings(Settings.builder().put("node.store.crypto.write_cache_enabled", false).build());

        String fileName = "test_not_cached_" + randomAlphaOfLength(6);
        byte[] data = new byte[CACHE_BLOCK_SIZE * 3];
        random().nextBytes(data);

        IndexOutput output = bufferPoolDirectory.createOutput(fileName, IOContext.DEFAULT);
        output.writeBytes(data, data.length);
        output.close();

        // No blocks should be in cache
        Path filePath = dirPath.resolve(fileName);
        for (int i = 0; i < 3; i++) {
            FileBlockCacheKey key = new FileBlockCacheKey(filePath, (long) i * CACHE_BLOCK_SIZE);
            BlockCacheValue<RefCountedMemorySegment> cached = directoryCache.get(key);
            assertNull("Block at offset " + (i * CACHE_BLOCK_SIZE) + " should NOT be cached when write_cache_enabled=false", cached);
        }
    }

    /**
     * Toggle the setting between writes and verify each write respects the current value.
     */
    public void testWriteCacheToggleDynamic() throws IOException {
        // 1. Write with caching enabled
        CryptoDirectoryFactory.setNodeSettings(Settings.builder().put("node.store.crypto.write_cache_enabled", true).build());

        String cachedFile = "toggle_cached_" + randomAlphaOfLength(6);
        byte[] data1 = new byte[CACHE_BLOCK_SIZE * 2];
        random().nextBytes(data1);

        IndexOutput out1 = bufferPoolDirectory.createOutput(cachedFile, IOContext.DEFAULT);
        out1.writeBytes(data1, data1.length);
        out1.close();

        Path cachedPath = dirPath.resolve(cachedFile);
        assertNotNull("Block should be cached after write with setting enabled", directoryCache.get(new FileBlockCacheKey(cachedPath, 0)));

        // 2. Disable and write another file
        CryptoDirectoryFactory.setNodeSettings(Settings.builder().put("node.store.crypto.write_cache_enabled", false).build());

        String uncachedFile = "toggle_uncached_" + randomAlphaOfLength(6);
        byte[] data2 = new byte[CACHE_BLOCK_SIZE * 2];
        random().nextBytes(data2);

        IndexOutput out2 = bufferPoolDirectory.createOutput(uncachedFile, IOContext.DEFAULT);
        out2.writeBytes(data2, data2.length);
        out2.close();

        Path uncachedPath = dirPath.resolve(uncachedFile);
        assertNull(
            "Block should NOT be cached after write with setting disabled",
            directoryCache.get(new FileBlockCacheKey(uncachedPath, 0))
        );

        // 3. Re-enable and verify caching works again
        CryptoDirectoryFactory.setNodeSettings(Settings.builder().put("node.store.crypto.write_cache_enabled", true).build());

        String recachedFile = "toggle_recached_" + randomAlphaOfLength(6);
        byte[] data3 = new byte[CACHE_BLOCK_SIZE * 2];
        random().nextBytes(data3);

        IndexOutput out3 = bufferPoolDirectory.createOutput(recachedFile, IOContext.DEFAULT);
        out3.writeBytes(data3, data3.length);
        out3.close();

        Path recachedPath = dirPath.resolve(recachedFile);
        assertNotNull(
            "Block should be cached again after re-enabling the setting",
            directoryCache.get(new FileBlockCacheKey(recachedPath, 0))
        );
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.opensearch.index.store.CryptoDirectoryFactory.DEFAULT_CRYPTO_PROVIDER;

import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.junit.After;
import org.junit.Before;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.CryptoDirectoryFactory;
import org.opensearch.index.store.DummyKeyProvider;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.cipher.EncryptionMetadataCacheRegistry;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.pool.PoolBuilder;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * Unit tests for {@link BufferPoolDirectory}.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class BufferPoolDirectoryTests extends OpenSearchTestCase {

    private BufferPoolDirectory directory;
    private PoolBuilder.PoolResources poolResources;
    private boolean originalWriteCacheEnabled;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        originalWriteCacheEnabled = CryptoDirectoryFactory.isWriteCacheEnabled();
        CryptoDirectoryFactory.setNodeSettings(Settings.builder().put("node.store.crypto.write_cache_enabled", false).build());

        Path path = createTempDir();
        Provider provider = Security.getProvider(DEFAULT_CRYPTO_PROVIDER);
        MasterKeyProvider keyProvider = DummyKeyProvider.create();

        Settings nodeSettings = Settings
            .builder()
            .put("plugins.crypto.enabled", true)
            .put("node.store.crypto.pool_size_percentage", 0.05)
            .put("node.store.crypto.warmup_percentage", 0.0)
            .put("node.store.crypto.cache_to_pool_ratio", 0.8)
            .build();

        this.poolResources = PoolBuilder.build(nodeSettings);
        Pool<RefCountedMemorySegment> segmentPool = poolResources.getSegmentPool();

        String indexUuid = randomAlphaOfLength(10);
        String indexName = randomAlphaOfLength(10);
        FSDirectory fsDirectory = FSDirectory.open(path);
        int shardId = 0;

        KeyResolver keyResolver = new TestKeyResolver(indexUuid, indexName, fsDirectory, provider, keyProvider, shardId);
        EncryptionMetadataCache encryptionMetadataCache = EncryptionMetadataCacheRegistry.getOrCreateCache(indexUuid, shardId, indexName);

        BlockLoader<RefCountedMemorySegment> loader = new CryptoDirectIOBlockLoader(
            segmentPool,
            keyResolver,
            encryptionMetadataCache,
            poolResources.getFileChannelCache()
        );

        Worker worker = poolResources.getSharedReadaheadWorker();

        @SuppressWarnings("unchecked")
        CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> sharedCache =
            (CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment>) poolResources.getBlockCache();

        BlockCache<RefCountedMemorySegment> directoryCache = new CaffeineBlockCache<>(
            sharedCache.getCache(),
            loader,
            poolResources.getMaxCacheBlocks()
        );

        this.directory = new BufferPoolDirectory(
            path,
            FSLockFactory.getDefault(),
            provider,
            keyResolver,
            segmentPool,
            directoryCache,
            loader,
            worker,
            encryptionMetadataCache,
            poolResources.getFileChannelCache()
        );
    }

    @After
    public void tearDown() throws Exception {
        CryptoDirectoryFactory
            .setNodeSettings(Settings.builder().put("node.store.crypto.write_cache_enabled", originalWriteCacheEnabled).build());
        // directory may already be closed by the test
        try {
            this.directory.close();
        } catch (AlreadyClosedException e) {
            // expected if test already closed it
        }
        this.poolResources.close();
        super.tearDown();
    }

    /**
     * Verifies that close() calls super.close() so that ensureOpen() throws
     * AlreadyClosedException on subsequent operations. Without super.close(),
     * the directory appears open and operations silently proceed on stale state.
     *
     * We test via createTempOutput since its ensureOpen() call is not wrapped in
     * a catch block that calls CryptoMetricsService (which isn't initialized in
     * unit tests).
     */
    public void testCloseMarksDirectoryAsClosed() throws Exception {
        // Close the directory
        directory.close();

        // After close, createTempOutput should throw AlreadyClosedException from ensureOpen()
        expectThrows(AlreadyClosedException.class, () -> directory.createTempOutput("test", "tmp", IOContext.DEFAULT));
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.mockito.Mockito.mock;
import static org.opensearch.index.store.CryptoDirectoryFactory.DEFAULT_CRYPTO_PROVIDER;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;

import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.junit.After;
import org.junit.Before;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.CryptoDirectoryFactory;
import org.opensearch.index.store.DummyKeyProvider;
import org.opensearch.index.store.block.RefCountedByteBuffer;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.cipher.EncryptionMetadataCacheRegistry;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.pool.PoolBuilder;
import org.opensearch.index.store.pool.SlabByteBufferPool;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * A speculative load (prefetch) that finds the pool full gives up with a {@link BlockLoader.PoolPressureException}
 * and is not logged as an error; an on-demand load still gets its block.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class SpeculativeLoadPoolPressureTests extends OpenSearchTestCase {

    private static final String FILE = "data.bin";

    private BufferPoolDirectory directory;
    private PoolBuilder.PoolResources poolResources;
    private SlabByteBufferPool fullPool;
    private BlockLoader<RefCountedByteBuffer> loader;
    private Path file;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        CryptoMetricsService.initialize(mock(MetricsRegistry.class));
        CryptoDirectoryFactory
            .setNodeSettings(
                Settings
                    .builder()
                    .put("node.store.crypto.write_cache_enabled", false)
                    .put("node.store.crypto.byte_buffer_mode_enabled", true)
                    .put("node.store.crypto.byte_buffer_slab_pool_enabled", true)
                    .build()
            );
        Path path = createTempDir();
        Provider provider = Security.getProvider(DEFAULT_CRYPTO_PROVIDER);
        poolResources = PoolBuilder
            .build(
                Settings
                    .builder()
                    .put("plugins.crypto.enabled", true)
                    .put("node.store.crypto.pool_size_percentage", 0.0)
                    .put("node.store.crypto.warmup_percentage", 0.0)
                    .put("node.store.crypto.cache_to_pool_ratio", 0.8)
                    .build()
            );
        @SuppressWarnings("unchecked")
        Pool<RefCountedByteBuffer> pool = (Pool<RefCountedByteBuffer>) poolResources.getPool();
        String indexUuid = randomAlphaOfLength(10);
        String indexName = randomAlphaOfLength(10);
        KeyResolver keyResolver = new TestKeyResolver(indexUuid, indexName, FSDirectory.open(path), provider, DummyKeyProvider.create(), 0);
        EncryptionMetadataCache metadataCache = EncryptionMetadataCacheRegistry.getOrCreateCache(indexUuid, 0, indexName);
        @SuppressWarnings("unchecked")
        CaffeineBlockCache<RefCountedByteBuffer, RefCountedByteBuffer> shared =
            (CaffeineBlockCache<RefCountedByteBuffer, RefCountedByteBuffer>) poolResources.getBlockCache();
        BlockCache<RefCountedByteBuffer> cache = new CaffeineBlockCache<>(
            shared.getCache(),
            new CryptoDirectIOBlockLoader<>(pool, keyResolver, metadataCache, poolResources.getFileChannelCache(), true),
            poolResources.getMaxCacheBlocks()
        );
        directory = new BufferPoolDirectory(
            path,
            FSLockFactory.getDefault(),
            provider,
            keyResolver,
            pool,
            cache,
            poolResources.getSharedReadaheadWorker(),
            metadataCache,
            poolResources.getFileChannelCache(),
            true
        );
        byte[] data = new byte[4 * CACHE_BLOCK_SIZE];
        random().nextBytes(data);
        try (IndexOutput out = directory.createOutput(FILE, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }
        file = path.resolve(FILE);

        // A two-slot pool whose slots are both held below: no slot is free.
        fullPool = new SlabByteBufferPool(2L * CACHE_BLOCK_SIZE, CACHE_BLOCK_SIZE);
        loader = new CryptoDirectIOBlockLoader<>(fullPool, keyResolver, metadataCache, poolResources.getFileChannelCache(), true);
    }

    @After
    public void tearDown() throws Exception {
        CryptoDirectoryFactory.setNodeSettings(Settings.EMPTY);
        fullPool.close();
        directory.close();
        poolResources.close();
        super.tearDown();
    }

    public void testSpeculativeLoadOnFullPoolIsPoolPressureAndNotAnError() throws Exception {
        RefCountedByteBuffer a = fullPool.acquire();
        RefCountedByteBuffer b = fullPool.acquire();
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(CryptoDirectIOBlockLoader.class))) {
            appender
                .addExpectation(
                    new MockLogAppender.UnseenEventExpectation(
                        "no error",
                        CryptoDirectIOBlockLoader.class.getCanonicalName(),
                        Level.ERROR,
                        "*"
                    )
                );
            expectThrows(BlockLoader.PoolPressureException.class, () -> loader.load(file, 0, 1, 50));
            appender.assertAllExpectationsMatched();
        }
        assertNotNull(a);
        assertNotNull(b);
    }

    public void testDemandLoadOnFullPoolStillGetsItsBlock() throws Exception {
        RefCountedByteBuffer a = fullPool.acquire();
        RefCountedByteBuffer b = fullPool.acquire();
        Object[] loaded = ((BlockLoader<?>) loader).load(file, 0, 1, BlockLoader.DEMAND_POOL_TIMEOUT_MS);
        assertEquals(1, loaded.length);
        assertEquals("the read path took an overflow buffer", 1, fullPool.getOverflowAllocations());
        assertNotNull(a);
        assertNotNull(b);
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.store.LockFactory;
import org.opensearch.common.Randomness;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.bufferpoolfs.BufferPoolDirectory;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.hybrid.HybridCryptoDirectory;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.niofs.CryptoNIOFSDirectory;
import org.opensearch.index.store.pool.MemorySegmentPool;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.index.store.read_ahead.impl.QueuingWorker;
import org.opensearch.telemetry.metrics.MetricsRegistry;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Shared factory for constructing crypto directory instances in tests.
 * Provides real (not mocked) encryption infrastructure with random keys.
 */
public final class CryptoTestDirectoryFactory {

    private static final Provider PROVIDER = Security.getProvider("SunJCE");
    private static final Set<String> NIO_EXTENSIONS = Set.of("si", "cfe", "fnm", "fdx", "fdm");

    private CryptoTestDirectoryFactory() {}

    /** Ensures CryptoMetricsService is initialized for tests. Safe to call multiple times. */
    public static void initMetrics() {
        try {
            CryptoMetricsService.initialize(mock(MetricsRegistry.class));
        } catch (Exception e) {
            // already initialized
        }
    }

    /** Creates a random 256-bit AES KeyResolver using the test's random source. */
    public static KeyResolver createKeyResolver() {
        byte[] rawKey = new byte[32];
        Randomness.get().nextBytes(rawKey);
        KeyResolver resolver = mock(KeyResolver.class);
        when(resolver.getDataKey()).thenReturn(new SecretKeySpec(rawKey, "AES"));
        return resolver;
    }

    public static CryptoNIOFSDirectory createCryptoNIOFSDirectory(Path path, LockFactory lockFactory) throws IOException {
        initMetrics();
        return new CryptoNIOFSDirectory(lockFactory, path, PROVIDER, createKeyResolver(), new EncryptionMetadataCache());
    }

    public static BufferPoolDirectory createBufferPoolDirectory(Path path, LockFactory lockFactory) throws IOException {
        initMetrics();
        KeyResolver keyResolver = createKeyResolver();
        EncryptionMetadataCache metadataCache = new EncryptionMetadataCache();
        Pool<RefCountedMemorySegment> pool = new MemorySegmentPool(131072, 8192);

        CryptoDirectIOBlockLoader blockLoader = new CryptoDirectIOBlockLoader(pool, keyResolver, metadataCache);

        Cache<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> caffeineCache = Caffeine
            .newBuilder()
            .maximumSize(1000)
            .expireAfterAccess(Duration.ofMinutes(5))
            .recordStats()
            .build();

        CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> blockCache = new CaffeineBlockCache<>(
            caffeineCache,
            blockLoader,
            1000
        );

        ExecutorService executor = Executors.newFixedThreadPool(4);
        Worker worker = new QueuingWorker(100, executor);

        return new BufferPoolDirectory(path, lockFactory, PROVIDER, keyResolver, pool, blockCache, blockLoader, worker, metadataCache);
    }

    public static HybridCryptoDirectory createHybridCryptoDirectory(Path path, LockFactory lockFactory) throws IOException {
        initMetrics();
        KeyResolver keyResolver = createKeyResolver();
        EncryptionMetadataCache metadataCache = new EncryptionMetadataCache();
        Pool<RefCountedMemorySegment> pool = new MemorySegmentPool(131072, 8192);

        CryptoDirectIOBlockLoader blockLoader = new CryptoDirectIOBlockLoader(pool, keyResolver, metadataCache);

        Cache<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> caffeineCache = Caffeine
            .newBuilder()
            .maximumSize(1000)
            .expireAfterAccess(Duration.ofMinutes(5))
            .recordStats()
            .build();

        CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> blockCache = new CaffeineBlockCache<>(
            caffeineCache,
            blockLoader,
            1000
        );

        ExecutorService executor = Executors.newFixedThreadPool(4);
        Worker worker = new QueuingWorker(100, executor);

        BufferPoolDirectory bufferPoolDir = new BufferPoolDirectory(
            path,
            lockFactory,
            PROVIDER,
            keyResolver,
            pool,
            blockCache,
            blockLoader,
            worker,
            metadataCache
        );

        return new HybridCryptoDirectory(lockFactory, bufferPoolDir, PROVIDER, keyResolver, metadataCache, NIO_EXTENSIONS);
    }
}

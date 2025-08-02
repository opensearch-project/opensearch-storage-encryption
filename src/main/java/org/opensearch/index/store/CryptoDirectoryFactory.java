/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE;
import static org.opensearch.index.store.directio.DirectIoConfigs.RESEVERED_POOL_SIZE_IN_BYTES;
import static org.opensearch.index.store.directio.DirectIoConfigs.WARM_UP_PERCENTAGE;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.crypto.CryptoHandlerRegistry;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.BlockLoader;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_cache.MemorySegmentPool;
import org.opensearch.index.store.block_cache.Pool;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;
import org.opensearch.index.store.directio.CryptoDirectIODirectory;
import org.opensearch.index.store.directio.CryptoDirectIOSegmentBlockLoader;
import org.opensearch.index.store.hybrid.HybridCryptoDirectory;
import org.opensearch.index.store.iv.DefaultKeyIvResolver;
import org.opensearch.index.store.iv.KeyIvResolver;
import org.opensearch.index.store.mmap.EagerDecryptedCryptoMMapDirectory;
import org.opensearch.index.store.mmap.LazyDecryptedCryptoMMapDirectory;
import org.opensearch.index.store.niofs.CryptoNIOFSDirectory;
import org.opensearch.plugins.IndexStorePlugin;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

@SuppressWarnings("preview")
@SuppressForbidden(reason = "temporary")
/**
 * Factory for an encrypted filesystem directory
 */
public class CryptoDirectoryFactory implements IndexStorePlugin.DirectoryFactory {

    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectoryFactory.class);

    private static volatile Pool<MemorySegment> sharedSegmentPool;
    private static final Object initLock = new Object();

    /**
     * Creates a new CryptoDirectoryFactory
     */
    public CryptoDirectoryFactory() {
        super();
    }

    /**
     *  Specifies a crypto provider to be used for encryption. The default value is SunJCE.
     */
    public static final Setting<Provider> INDEX_CRYPTO_PROVIDER_SETTING = new Setting<>("index.store.crypto.provider", "SunJCE", (s) -> {
        Provider p = Security.getProvider(s);
        if (p == null) {
            throw new SettingsException("unrecognized [index.store.crypto.provider] \"" + s + "\"");
        } else
            return p;
    }, Property.IndexScope, Property.InternalIndex);

    /**
     *  Specifies the Key management plugin type to be used. The desired KMS plugin should be installed.
     */
    public static final Setting<String> INDEX_KMS_TYPE_SETTING = new Setting<>("index.store.kms.type", "", Function.identity(), (s) -> {
        if (s == null || s.isEmpty()) {
            throw new SettingsException("index.store.kms.type must be set");
        }
    }, Property.NodeScope, Property.IndexScope);

    MasterKeyProvider getKeyProvider(IndexSettings indexSettings) {
        final String KEY_PROVIDER_TYPE = indexSettings.getValue(INDEX_KMS_TYPE_SETTING);
        final Settings settings = Settings.builder().put(indexSettings.getNodeSettings(), false).build();
        CryptoMetadata cryptoMetadata = new CryptoMetadata("", KEY_PROVIDER_TYPE, settings);
        MasterKeyProvider keyProvider;
        try {
            keyProvider = CryptoHandlerRegistry
                .getInstance()
                .getCryptoKeyProviderPlugin(KEY_PROVIDER_TYPE)
                .createKeyProvider(cryptoMetadata);
        } catch (NullPointerException npe) {
            throw new RuntimeException("could not find key provider: " + KEY_PROVIDER_TYPE, npe);
        }
        return keyProvider;
    }

    /**
     * {@inheritDoc}
     * @param indexSettings the index settings
     * @param path the shard file path
     */
    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath path) throws IOException {
        final Path location = path.resolveIndex();
        final LockFactory lockFactory = indexSettings.getValue(org.opensearch.index.store.FsDirectoryFactory.INDEX_LOCK_FACTOR_SETTING);
        Files.createDirectories(location);
        return newFSDirectory(location, lockFactory, indexSettings);
    }

    /**
     * {@inheritDoc}
     * @param location the directory location
     * @param lockFactory the lockfactory for this FS directory
     * @param indexSettings the read index settings 
     * @return the concrete implementation of the directory based on index setttings.
     * @throws IOException
     */
    protected Directory newFSDirectory(Path location, LockFactory lockFactory, IndexSettings indexSettings) throws IOException {
        final Provider provider = indexSettings.getValue(INDEX_CRYPTO_PROVIDER_SETTING);
        Directory baseDir = new NIOFSDirectory(location, lockFactory);
        KeyIvResolver keyIvResolver = new DefaultKeyIvResolver(baseDir, provider, getKeyProvider(indexSettings));

        IndexModule.Type type = IndexModule.defaultStoreType(IndexModule.NODE_STORE_ALLOW_MMAP.get(indexSettings.getNodeSettings()));
        Set<String> preLoadExtensions = new HashSet<>(indexSettings.getValue(IndexModule.INDEX_STORE_PRE_LOAD_SETTING));
        // [cfe, tvd, fnm, nvm, write.lock, dii, pay, segments_N, pos, si, fdt, tvx, liv, dvm, fdx, vem]
        Set<String> nioExtensions = new HashSet<>(indexSettings.getValue(IndexModule.INDEX_STORE_HYBRID_NIO_EXTENSIONS));

        switch (type) {
            case HYBRIDFS -> {
                LOGGER.debug("Using HYBRIDFS directory");
                LazyDecryptedCryptoMMapDirectory lazyDecryptedCryptoMMapDirectory = new LazyDecryptedCryptoMMapDirectory(
                    location,
                    provider,
                    keyIvResolver
                );
                EagerDecryptedCryptoMMapDirectory eagerDecryptedCryptoMMapDirectory = new EagerDecryptedCryptoMMapDirectory(
                    location,
                    provider,
                    keyIvResolver
                );

                CryptoDirectIODirectory cryptoDirectIODirectory = createCryptoDirectIODirectory(
                    location,
                    lockFactory,
                    provider,
                    keyIvResolver
                );

                return new HybridCryptoDirectory(
                    lockFactory,
                    lazyDecryptedCryptoMMapDirectory,
                    eagerDecryptedCryptoMMapDirectory,
                    cryptoDirectIODirectory,
                    provider,
                    keyIvResolver,
                    nioExtensions
                );
            }
            case MMAPFS -> {
                LOGGER.debug("Using MMAPFS directory");
                LazyDecryptedCryptoMMapDirectory cryptoMMapDir = new LazyDecryptedCryptoMMapDirectory(location, provider, keyIvResolver);
                cryptoMMapDir.setPreloadExtensions(preLoadExtensions);
                return cryptoMMapDir;
            }
            case SIMPLEFS, NIOFS -> {
                LOGGER.debug("Using NIOFS directory");
                return new CryptoNIOFSDirectory(lockFactory, location, provider, keyIvResolver);
            }
            default -> throw new AssertionError("unexpected built-in store type [" + type + "]");
        }
    }

    private CryptoDirectIODirectory createCryptoDirectIODirectory(
        Path location,
        LockFactory lockFactory,
        Provider provider,
        KeyIvResolver keyIvResolver
    ) throws IOException {
        ensureSharedPoolInitialized();

        BlockLoader<RefCountedMemorySegment> loader = new CryptoDirectIOSegmentBlockLoader(sharedSegmentPool, keyIvResolver);

        /*
        * ================================
        * Block Cache with RefCountedMemorySegment
        * ================================
        *
        * This Caffeine cache stores decrypted MemorySegment blocks for direct I/O access,
        * using reference counting to ensure safe reuse across multiple readers.
        *
        * Cache Type:
        * ------------
        * - Key:   BlockCacheKey (typically includes file path, offset, etc.)
        * - Value: BlockCacheValue<RefCountedMemorySegment>
        *
        * Memory Lifecycle:
        * ------------------
        * - Each cached block is a RefCountedMemorySegment, which wraps a MemorySegment
        *   and manages its lifetime via reference counting.
        *
        * - On load, we increment the reference count via `incRef()` for each use
        *   (i.e., each IndexInput clone or slice).
        *
        * - On close, `decRef()` is called. When the count hits zero, the underlying
        *   MemorySegment is released via a `SegmentReleaser` (typically returning
        *   the segment to a pool or freeing it).
        *
        * Eviction Semantics:
        * --------------------
        * - We use `expireAfterAccess` + `maximumSize` to control cache lifecycle.
        * - When an entry is evicted from the cache:
        *     → It is removed from the map.
        *     → Its associated segment is not released immediately if refCount > 1.
        *     → Only when all holders release the segment (refCount → 0), the memory is returned.
        *
        * Subtle Implication:
        * --------------------
        * - This design decouples **cache visibility** from **memory lifetime**.
        *   Even if a segment is evicted from cache, it may continue to live
        *   until all active holders release it.
        *
        * - This also means:
        *     → If the segment is heavily referenced (e.g., during merges), it will stay alive.
        *     → If the pool is full, new segments may bypass the cache entirely.
        *     → Under memory pressure, we still maintain correctness by relying on ref-counting.
        *
        * Threading:
        * -----------
        * - Caffeine eviction is single-threaded by default (runs in caller thread via `Runnable::run`),
        *   which avoids offloading release to background threads that may hold on to native memory.
        *
        * TODO:
        * -----
        * - Tune `maximumSize` and eviction policy based on benchmark results and memory pressure.
        */

        // todo: int maxEntries = PER_DIR_CACHE_SIZE / SEGMENT_SIZE_BYTES;
        Cache<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> cache = Caffeine
            .newBuilder()
            .maximumSize(16384) // todo figure out a good config.
            .recordStats()
            .expireAfterAccess(15, TimeUnit.MINUTES)
            .executor(Runnable::run)
            .removalListener((BlockCacheKey key, BlockCacheValue<RefCountedMemorySegment> value, RemovalCause cause) -> {
                if (value != null) {
                    value.close();
                }
            })
            .build();

        BlockCache<RefCountedMemorySegment> blockCache = new CaffeineBlockCache<>(cache, loader, 16384);

        return new CryptoDirectIODirectory(location, lockFactory, provider, keyIvResolver, sharedSegmentPool, blockCache, loader);

    }

    private void ensureSharedPoolInitialized() {
        if (sharedSegmentPool == null) {
            synchronized (initLock) {
                if (sharedSegmentPool == null) {
                    long maxBlocks = RESEVERED_POOL_SIZE_IN_BYTES / CACHE_BLOCK_SIZE;
                    sharedSegmentPool = new MemorySegmentPool(RESEVERED_POOL_SIZE_IN_BYTES, CACHE_BLOCK_SIZE);
                    LOGGER
                        .info(
                            "Creating pool with sizeBytes={}, segmentSize={}, totalSegments={}",
                            RESEVERED_POOL_SIZE_IN_BYTES,
                            CACHE_BLOCK_SIZE,
                            RESEVERED_POOL_SIZE_IN_BYTES / CACHE_BLOCK_SIZE
                        );
                    sharedSegmentPool.warmUp((long) (maxBlocks * WARM_UP_PERCENTAGE));
                    // startTelemetry();
                }
            }
        }
    }

    private void publishPoolStats() {
        try {
            LOGGER.info("{} {} \n {}", sharedSegmentPool.poolStats());

        } catch (Exception e) {
            LOGGER.warn("Failed to log cache/pool stats", e);
        }
    }

    private void startTelemetry() {
        Thread loggerThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(10_000); // 60 seconds
                    publishPoolStats();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Throwable t) {
                    LOGGER.warn("Error in buffer pool stats logger", t);
                }
            }
        });

        loggerThread.setDaemon(true);
        loggerThread.setName("DirectIOBufferPoolStatsLogger");
        loggerThread.start();
    }
}

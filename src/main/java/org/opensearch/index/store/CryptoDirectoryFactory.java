/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import static org.opensearch.index.store.directio.DirectIoConfigs.PER_DIRECTORY_RESEVERED_POOL_SIZE_IN_BYTES;
import static org.opensearch.index.store.directio.DirectIoConfigs.SEGMENT_POOL_TO_CACHE_SIZE_RATIO;
import static org.opensearch.index.store.directio.DirectIoConfigs.SEGMENT_SIZE_BYTES;
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
                EagerDecryptedCryptoMMapDirectory egarDecryptedCryptoMMapDirectory = new EagerDecryptedCryptoMMapDirectory(
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
                    egarDecryptedCryptoMMapDirectory,
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

    private static CryptoDirectIODirectory createCryptoDirectIODirectory(
        Path location,
        LockFactory lockFactory,
        Provider provider,
        KeyIvResolver keyIvResolver
    ) throws IOException {

        int maxBlocks = (int) (PER_DIRECTORY_RESEVERED_POOL_SIZE_IN_BYTES / SEGMENT_SIZE_BYTES);
        Pool<MemorySegment> memorySegmentPool = new MemorySegmentPool(PER_DIRECTORY_RESEVERED_POOL_SIZE_IN_BYTES, SEGMENT_SIZE_BYTES);

        // Warm-up the pool with allocated segments.
        memorySegmentPool.warmUp((int) (maxBlocks * WARM_UP_PERCENTAGE));

        // Cache should have lowere budger than the segment budget
        int maxCacheBlocks = (int) (maxBlocks * SEGMENT_POOL_TO_CACHE_SIZE_RATIO);

        BlockLoader<RefCountedMemorySegment> blockLoader = new CryptoDirectIOSegmentBlockLoader(memorySegmentPool, keyIvResolver);

        Cache<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> cache = Caffeine
            .newBuilder()
            .maximumSize(maxCacheBlocks)
            .recordStats()
            .expireAfterAccess(30, TimeUnit.MINUTES)
            .executor(Runnable::run)
            .removalListener((BlockCacheKey key, BlockCacheValue<RefCountedMemorySegment> value, RemovalCause cause) -> {
                if (value == null) {
                    throw new IllegalStateException("Unexpected null value during cache eviction for key: " + key);
                }
                RefCountedMemorySegment refSegment = value.block();
                // a runnable will cleanup after the last usage of the segment.
                refSegment.setOnFullyReleased(segment -> { value.close(); });
            })
            .build();

        BlockCache<RefCountedMemorySegment> blockCache = new CaffeineBlockCache<>(cache, blockLoader, maxCacheBlocks);

        return new CryptoDirectIODirectory(location, lockFactory, provider, keyIvResolver, memorySegmentPool, blockCache);
    }
}

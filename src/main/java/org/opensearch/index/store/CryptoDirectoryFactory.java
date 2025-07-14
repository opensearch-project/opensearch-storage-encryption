/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;
import java.util.HashSet;
import java.util.Set;
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
import org.opensearch.index.store.block_cache.DefaultMemorySegmentPool;
import org.opensearch.index.store.block_cache.DirectIOBlockCache;
import org.opensearch.index.store.block_cache.MemorySegmentPool;
import org.opensearch.index.store.directio.CryptoDirectIODirectory;
import org.opensearch.index.store.hybrid.HybridCryptoDirectory;
import org.opensearch.index.store.iv.DefaultKeyIvResolver;
import org.opensearch.index.store.iv.KeyIvResolver;
import org.opensearch.index.store.mmap.EagerDecryptedCryptoMMapDirectory;
import org.opensearch.index.store.mmap.LazyDecryptedCryptoMMapDirectory;
import org.opensearch.index.store.niofs.CryptoNIOFSDirectory;
import org.opensearch.plugins.IndexStorePlugin;

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

                // Per directory configuration.
                // todo: This will be later per node configuration.
                long poolMemoryBytes = 100L * 1024 * 1024;
                int segementSizeBytes = 65_536;
                int maxBlocks = (int) (poolMemoryBytes / segementSizeBytes);

                MemorySegmentPool memorySegmentPool = new DefaultMemorySegmentPool(poolMemoryBytes, segementSizeBytes);
                // warm up the pool with a few segments. 
                memorySegmentPool.warmUp((int) (maxBlocks * 0.2));

                // Cache should use 90% of the segments
                int maxCacheBlocks = (int) (maxBlocks * 0.9);

                BlockCache blockCache = new DirectIOBlockCache(memorySegmentPool, maxCacheBlocks);

                CryptoDirectIODirectory cryptoDirectIODirectory = new CryptoDirectIODirectory(
                    location,
                    lockFactory,
                    provider,
                    keyIvResolver,
                    memorySegmentPool,
                    blockCache
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
}

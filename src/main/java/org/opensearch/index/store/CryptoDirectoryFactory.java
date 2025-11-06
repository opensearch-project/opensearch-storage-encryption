/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import static org.opensearch.index.store.directio.DirectIoConfigs.READ_AHEAD_QUEUE_SIZE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockFactory;
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
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.directio.CryptoDirectIODirectory;
import org.opensearch.index.store.hybrid.HybridCryptoDirectory;
import org.opensearch.index.store.iv.IndexKeyResolverRegistry;
import org.opensearch.index.store.iv.KeyIvResolver;
import org.opensearch.index.store.niofs.CryptoNIOFSDirectory;
import org.opensearch.index.store.pool.PoolBuilder;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.index.store.read_ahead.impl.QueuingWorker;
import org.opensearch.plugins.IndexStorePlugin;

/**
 * Factory for creating encrypted filesystem directories with support for various storage types.
 *
 * <p>Supports:
 * <ul>
 * <li>NIOFS: NIO-based encrypted file system</li>
 * <li>HYBRIDFS: Hybrid directory with Direct I/O and block caching</li>
 * <li>MMAPFS: Not supported (throws AssertionError)</li>
 * </ul>
 *
 * <p>The factory maintains node-level shared resources (pool and cache) for efficient
 * memory utilization across all encrypted directories.
 *
 * @opensearch.internal
 */
@SuppressForbidden(reason = "temporary")
public class CryptoDirectoryFactory implements IndexStorePlugin.DirectoryFactory {

    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectoryFactory.class);

    /**
     * Shared pool resources including pool, cache, read-ahead executor, and telemetry.
     * Initialized once per node and shared across all CryptoDirectIODirectory instances.
     */
    private static volatile PoolBuilder.PoolResources poolResources;

    /**
     * Lock for thread-safe initialization of shared resources.
     */
    private static final Object initLock = new Object();

    /**
     * Creates a new CryptoDirectoryFactory
     */
    public CryptoDirectoryFactory() {
        super();
    }

    /**
     * Base setting prefix for crypto-related index settings.
     */
    public static final String CRYPTO_SETTING = "index.store.crypto";

    /**
     * Specifies a crypto provider to be used for encryption. The default value
     * is SunJCE.
     */
    public static final Setting<Provider> INDEX_CRYPTO_PROVIDER_SETTING = new Setting<>("index.store.crypto.provider", "SunJCE", (s) -> {
        Provider p = Security.getProvider(s);
        if (p == null) {
            throw new SettingsException("unrecognized [index.store.crypto.provider] \"" + s + "\"");
        } else {
            return p;
        }
    }, Property.IndexScope, Property.InternalIndex);

    /**
     * Specifies the Key management plugin type to be used. The desired CryptoKeyProviderPlugin
     * plugin should be installed.
     */
    public static final Setting<String> INDEX_KEY_PROVIDER_SETTING = new Setting<>(
        "index.store.crypto.key_provider",
        "",
        Function.identity(),
        (s) -> {
            if (s == null || s.isEmpty()) {
                throw new SettingsException("index.store.crypto.key_provider must be set");
            }
        },
        Property.NodeScope,
        Property.IndexScope
    );

    /**
     * AWS KMS key ARN for index-level encryption.
     * Specifies the Amazon Resource Name of the KMS key used as master key for encrypting index data.
     */
    public static final Setting<String> INDEX_KMS_ARN_SETTING = new Setting<>(
        "index.store.crypto.kms.key_arn",
        "",
        Function.identity(),
        (s) -> {
            if (s == null || s.isEmpty()) {
                throw new SettingsException("index.store.kms.arn must be set");
            }
        },
        Property.IndexScope
    );

    /**
     * AWS KMS encryption context for additional authenticated data.
     * Provides extra security by requiring the same context for both encrypt and decrypt operations.
    */
    public static final Setting<String> INDEX_KMS_ENC_CTX_SETTING = new Setting<>(
        "index.store.crypto.kms.encryption_context",
        "",
        Function.identity(),
        (s) -> {
            if (s == null || s.isEmpty()) {
                throw new SettingsException("index.store.kms.arn must be set");
            }
        },
        Property.IndexScope
    );

    /**
     * Specifies the node-level TTL for data keys in seconds.
     * Default is 3600 seconds (1 hour).
     * Set to -1 to disable key refresh (keys are loaded once and cached forever).
     * This setting applies globally to all indices.
     */
    public static final Setting<Integer> NODE_KEY_REFRESH_INTERVAL_SECS_SETTING = Setting
        .intSetting(
            "node.store.crypto.key_refresh_interval_secs",
            3600,  // default: 3600 seconds (1 hour)
            -1,    // minimum: -1 means never refresh
            (value) -> {
                if (value != -1 && value < 1) {
                    throw new IllegalArgumentException(
                        "node.store.crypto.key_refresh_interval_secs must be -1 (never refresh) or a positive value"
                    );
                }
            },
            Property.NodeScope
        );

    MasterKeyProvider getKeyProvider(IndexSettings indexSettings) {
        final String KEY_PROVIDER = indexSettings.getValue(INDEX_KEY_PROVIDER_SETTING);

        // Handle dummy type for testing
        if (KeyProviderType.DUMMY.getValue().equals(KEY_PROVIDER)) {
            LOGGER.debug("Using dummy key provider for testing");
            return DummyKeyProvider.create();
        }

        Settings settings = indexSettings.getSettings().getAsSettings(CRYPTO_SETTING);
        CryptoMetadata cryptoMetadata = new CryptoMetadata(KEY_PROVIDER, "", settings);
        MasterKeyProvider keyProvider;
        try {
            keyProvider = CryptoHandlerRegistry.getInstance().getCryptoKeyProviderPlugin(KEY_PROVIDER).createKeyProvider(cryptoMetadata);
        } catch (NullPointerException npe) {
            throw new RuntimeException("could not find key provider: " + KEY_PROVIDER, npe);
        }
        return keyProvider;
    }

    /**
     * {@inheritDoc}
     *
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
     * Creates an encrypted directory based on the configured store type.
     *
     * @param location the directory location
     * @param lockFactory the lock factory for this directory
     * @param indexSettings the index settings
     * @return the concrete implementation of the encrypted directory based on store type
     * @throws IOException if directory creation fails
     */
    protected Directory newFSDirectory(Path location, LockFactory lockFactory, IndexSettings indexSettings) throws IOException {
        final Provider provider = indexSettings.getValue(INDEX_CRYPTO_PROVIDER_SETTING);

        // Use index-level key resolver - store keys at index level

        Path indexDirectory = location.getParent().getParent(); // Go up two levels: index -> shard -> index
        MasterKeyProvider keyProvider = getKeyProvider(indexSettings);

        // Create a directory for the index-level keys
        Directory indexKeyDirectory = FSDirectory.open(indexDirectory);

        // Use shared resolver registry to prevent race conditions
        String indexUuid = indexSettings.getIndex().getUUID();
        KeyIvResolver keyIvResolver = IndexKeyResolverRegistry.getOrCreateResolver(indexUuid, indexKeyDirectory, provider, keyProvider);

        IndexModule.Type type = IndexModule.defaultStoreType(IndexModule.NODE_STORE_ALLOW_MMAP.get(indexSettings.getNodeSettings()));

        switch (type) {
            case HYBRIDFS -> {
                LOGGER.debug("Using HYBRIDFS directory with Direct I/O and block caching");

                CryptoDirectIODirectory cryptoDirectIODirectory = createCryptoDirectIODirectory(
                    location,
                    lockFactory,
                    provider,
                    keyIvResolver
                );
                return new HybridCryptoDirectory(lockFactory, cryptoDirectIODirectory, provider, keyIvResolver);
            }
            case MMAPFS -> {
                throw new AssertionError("MMAPFS not supported with index level encryption");
            }
            case SIMPLEFS, NIOFS -> {
                LOGGER.debug("Using NIOFS directory for encrypted storage");
                return new CryptoNIOFSDirectory(lockFactory, location, provider, keyIvResolver);
            }
            default -> throw new AssertionError("unexpected built-in store type [" + type + "]");
        }
    }

    @SuppressWarnings("unchecked")
    private CryptoDirectIODirectory createCryptoDirectIODirectory(
        Path location,
        LockFactory lockFactory,
        Provider provider,
        KeyIvResolver keyIvResolver
    ) throws IOException {
        /*
        * ================================
        * Shared Block Cache Architecture
        * ================================
        *
        * This method creates a CryptoDirectIODirectory that uses node-level shared resources
        * (pool and cache) for efficient memory utilization and high cache hit rates.
        *
        * Shared Resources:
        * -----------------
        * - sharedSegmentPool: Pool of RefCountedMemorySegments (initialized in initializeSharedPool)
        * - sharedBlockCache: Caffeine cache storing decrypted blocks (initialized in initializeSharedPool)
        *
        * Per-Directory Resources:
        * ------------------------
        * - BlockLoader: Directory-specific loader using this directory's keyIvResolver for decryption
        * - Cache Wrapper: Wraps the shared cache with directory-specific loader
        * - ReadAhead Worker: Asynchronous prefetching for sequential reads
        *
        * Memory Lifecycle:
        * -----------------
        * 1. Cache miss: Loader reads encrypted data, decrypts it, stores in RefCountedMemorySegment
        * 2. Initial refCount=1 (cache's reference)
        * 3. Reader pins: refCount incremented via tryPin()
        * 4. Reader unpins: refCount decremented via decRef()
        * 5. Cache eviction: retired=true set (prevents new pins), then decRef() called
        * 6. refCount=0: Segment returned to pool for reuse
        *
        * Two-Phase Eviction (prevents stale reads):
        * -------------------------------------------
        * - evictionListener: Sets retired=true (marks stale for BlockSlotTinyCache)
        * - removalListener: Calls decRef() (releases cache's reference)
        */

        // Create a per-directory loader that uses this directory's keyIvResolver for decryption
        BlockLoader<RefCountedMemorySegment> loader = new CryptoDirectIOBlockLoader(poolResources.getSegmentPool(), keyIvResolver);

        // Cache architecture: One shared Caffeine cache storage, multiple wrapper instances
        // - sharedBlockCache: Created once in initializeSharedPool(), holds the actual cache storage
        // - directoryCache: Per-directory wrapper that shares the underlying cache but uses its own loader
        // This design allows:
        // * Shared cache capacity across all directories
        // * Per-directory decryption via directory-specific loaders with unique keyIvResolvers
        // * Unified eviction policy managed by the shared cache
        CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> sharedCaffeineCache =
            (CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment>) poolResources.getBlockCache();

        BlockCache<RefCountedMemorySegment> directoryCache = new CaffeineBlockCache<>(
            sharedCaffeineCache.getCache(),
            loader,
            poolResources.getMaxCacheBlocks()
        );

        // Create per-shard worker with isolated queue but shared executor threads
        // Limit concurrent drainers per shard to prevent overwhelming the shared pool
        int maxRunners = Math.max(2, Runtime.getRuntime().availableProcessors() / 8);
        Worker readaheadWorker = new QueuingWorker(READ_AHEAD_QUEUE_SIZE, maxRunners, poolResources.getReadAheadExecutor(), directoryCache);

        return new CryptoDirectIODirectory(
            location,
            lockFactory,
            provider,
            keyIvResolver,
            poolResources.getSegmentPool(),
            directoryCache,
            loader,
            readaheadWorker
        );
    }

    /**
     * Initialize the shared MemorySegmentPool and BlockCache once per node.
     * This method is called from CryptoDirectoryPlugin.createComponents().
     *
     * Thread Safety:
     * - Uses double-checked locking for initialization -- safe.
     *
     * @param settings the node settings for configuration
     * @return a handle that can be closed to stop telemetry
     */
    @SuppressWarnings("DoubleCheckedLocking")
    public static PoolBuilder.PoolResources initializeSharedPool(Settings settings) {
        if (poolResources == null) {
            synchronized (initLock) {
                if (poolResources == null) {
                    poolResources = PoolBuilder.build(settings);
                }
            }
        }
        return poolResources;
    }
}

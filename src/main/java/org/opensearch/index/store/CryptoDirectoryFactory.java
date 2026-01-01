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
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockFactory;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.crypto.CryptoHandlerRegistry;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.bufferpoolfs.BufferPoolDirectory;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.cipher.EncryptionMetadataCacheRegistry;
import org.opensearch.index.store.hybrid.HybridCryptoDirectory;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.key.ShardKeyResolverRegistry;
import org.opensearch.index.store.kms_encryption_context.EncryptionContextResolver;
import org.opensearch.index.store.kms_encryption_context.EncryptionContextResolverFactory;
import org.opensearch.index.store.niofs.CryptoNIOFSDirectory;
import org.opensearch.index.store.pool.PoolBuilder;
import org.opensearch.index.store.read_ahead.Worker;
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
     * Shared pool resources including pool, cache, and telemetry.
     * Lazily initialized on first cryptofs shard creation and shared across all CryptoBufferPoolFSDirectory instances.
     * This prevents resource allocation on dedicated master nodes which never create shards.
     */
    private static volatile PoolBuilder.PoolResources poolResources;

    /**
     * Node settings used for lazy pool initialization.
     */
    private static volatile Settings nodeSettings;

    /**
     * Lock for thread-safe initialization of shared resources.
     */
    private static final Object initLock = new Object();

    /**
     * Resolver for obtaining default encryption context from cluster metadata.
     * Abstracted to allow Amazon-specific logic to be maintained separately.
     */
    private static volatile EncryptionContextResolver encryptionContextResolver;

    /**
     * Creates a new CryptoDirectoryFactory
     */
    public CryptoDirectoryFactory() {
        super();
    }

    /**
     * Store type identifier for encrypted filesystem directories.
     */
    public static final String STORE_TYPE = "cryptofs";

    /**
     * Base setting prefix for crypto-related index settings.
     */
    public static final String CRYPTO_SETTING = "index.store.crypto";

    /**
     * Default crypto provider name.
     */
    public static final String DEFAULT_CRYPTO_PROVIDER = "SunJCE";

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
        Property.IndexScope,
        Property.InternalIndex
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
        Property.IndexScope,
        Property.InternalIndex
    );

    /**
     * AWS KMS encryption context for additional authenticated data.
     * Provides extra security by requiring the same context for both encrypt and decrypt operations.
    */
    public static final Setting<String> INDEX_KMS_ENC_CTX_SETTING = new Setting<>(
        "index.store.crypto.kms.encryption_context",
        Constants.DEFAULT_KMS_ENC_CTX,
        Function.identity(),
        Property.IndexScope,
        Property.InternalIndex
    );

    /**
     * Specifies the node-level interval for proactive health monitoring of encryption keys.
     * The health monitor periodically validates all encrypted indices and attempts to refresh their keys,
     * providing early detection of issues and automatic recovery.
     * 
     * Default: 1 hour (1h)
     * Minimum: 1 second (1s) - must be positive
     * 
     * This setting applies globally to all indices.
     * 
     * Supported units: s (seconds), m (minutes), h (hours), d (days)
     * Examples: 30s, 5m, 1h, 2h
     */
    public static final Setting<TimeValue> NODE_KEY_REFRESH_INTERVAL_SETTING = Setting
        .timeSetting(
            "node.store.crypto.key_refresh_interval",
            TimeValue.timeValueHours(1),  // default: 1 hour
            TimeValue.timeValueSeconds(1),  // minimum: 1 second (must be positive)
            Property.NodeScope
        );

    /**
     * Specifies the node-level expiration time for cached encryption keys.
     * Keys are evicted from cache after this duration and must be reloaded from the key provider.
     * 
     * Default: 24 hours (24h)
     * Set to -1 to never expire keys (cache forever until node restart).
     * 
     * This setting applies globally to all indices.
     * 
     * Supported units: s (seconds), m (minutes), h (hours), d (days)
     * Examples: 60s, 10m, 3h, 12h, -1 (never expire)
     */
    public static final Setting<TimeValue> NODE_KEY_EXPIRY_INTERVAL_SETTING = Setting
        .timeSetting(
            "node.store.crypto.key_expiry_interval",
            TimeValue.timeValueHours(24),  // default: 24 hours
            TimeValue.timeValueSeconds(-1),  // minimum: -1 means never expire
            Property.NodeScope
        );

    /**
     * Get default encryption context from cluster metadata using the configured resolver.
     *
     * @return the encryption context from cluster settings, or empty string if not found
     */
    private String getDefaultEncryptionContextFromCluster() {
        if (encryptionContextResolver == null) {
            return "";
        }

        return encryptionContextResolver.resolveDefaultEncryptionContext();
    }

    MasterKeyProvider getKeyProvider(IndexSettings indexSettings) {
        final String KEY_PROVIDER = indexSettings.getValue(INDEX_KEY_PROVIDER_SETTING);

        // Handle dummy type for testing
        if (KeyProviderType.DUMMY.getValue().equals(KEY_PROVIDER)) {
            LOGGER.debug("Using dummy key provider for testing");
            return DummyKeyProvider.create();
        }

        Settings settings = indexSettings.getSettings().getAsSettings(CRYPTO_SETTING);

        // Always try to get default encryption context from cluster repositories as a baseline
        String defaultEncCtx = getDefaultEncryptionContextFromCluster();
        String indexEncCtx = settings.get("kms.encryption_context");

        // Merge default encryption context with index-specific context
        if (!defaultEncCtx.isEmpty()) {
            if (indexEncCtx == null || indexEncCtx.isEmpty()) {
                // Use default encryption context if index doesn't specify one
                LOGGER
                    .info(
                        "Using default encryption context from cluster repository for index {}: {}",
                        indexSettings.getIndex().getName(),
                        defaultEncCtx
                    );
                settings = Settings.builder().put(settings).put("kms.encryption_context", defaultEncCtx).build();
            } else {
                // Merge: default context is the baseline, index context is additional
                String mergedEncCtx = defaultEncCtx + "," + indexEncCtx;
                LOGGER
                    .info(
                        "Merging default encryption context '{}' with index-specific context '{}' for index {}: result='{}'",
                        defaultEncCtx,
                        indexEncCtx,
                        indexSettings.getIndex().getName(),
                        mergedEncCtx
                    );
                settings = Settings.builder().put(settings).put("kms.encryption_context", mergedEncCtx).build();
            }
        }

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
        int shardId = path.getShardId().getId();
        return newFSDirectory(location, lockFactory, indexSettings, shardId);
    }

    /**
     * Handles keyfile copying for clone/resize operations.
     * When an index is cloned, Lucene copies the ciphertext segment files verbatim,
     * but if we generate a new key for the target index, decryption will fail.
     * This method detects clone operations and copies the source keyfile to the target.
     *
     * Package-private for testing.
     *
     * @param indexSettings the index settings
     * @param targetIndexDirectory the target index directory path
     * @throws IOException if keyfile copy fails
     */
    void handleResizeOperation(IndexSettings indexSettings, Path targetIndexDirectory) throws IOException {
        // Check for resize source UUID setting (indicates clone/shrink/split operation)
        String resizeSourceUuid = indexSettings.getSettings().get("index.resize.source.uuid");
        String resizeSourceName = indexSettings.getSettings().get("index.resize.source.name");

        if (resizeSourceUuid == null || resizeSourceUuid.isEmpty()) {
            // Not a resize operation, proceed with normal key generation
            return;
        }

        LOGGER
            .info(
                "Detected resize operation for index {} from source index {} (UUID: {})",
                indexSettings.getIndex().getName(),
                resizeSourceName,
                resizeSourceUuid
            );

        // Determine source index directory path
        Path targetParent = targetIndexDirectory.getParent(); // indices/
        Path sourceIndexDirectory = targetParent.resolve(resizeSourceUuid);

        Path sourceKeyfile = sourceIndexDirectory.resolve("keyfile");
        Path targetKeyfile = targetIndexDirectory.resolve("keyfile");

        // Check if source keyfile exists
        if (!Files.exists(sourceKeyfile)) {
            LOGGER
                .warn(
                    "[Resize operation] for index {} from source index {} which does not have index-level encryption enabled. "
                        + "Target index will generate a new encryption key.",
                    indexSettings.getIndex().getName(),
                    resizeSourceName
                );
            return;
        }

        // Now, check if target keyfile already exists
        // This can happen when multiple shards are initialized concurrently on the same node
        // and another shard has already copied the keyfile
        if (Files.exists(targetKeyfile)) {
            LOGGER
                .debug(
                    "[Resize operation] encryption keyfile already exists at {} for index {}"
                        + "Skipping copy as it was likely created by another shard initialization.",
                    targetKeyfile,
                    indexSettings.getIndex().getName()
                );
            return;
        }

        // Copy keyfile from source to target
        try {
            Files.copy(sourceKeyfile, targetKeyfile);
            LOGGER.debug("Successfully copied keyfile from {} to {} for resize operation", sourceKeyfile, targetKeyfile);
        } catch (IOException e) {
            throw new IOException(
                "[Resize operation] Failed to copy keyfile from source index "
                    + resizeSourceName
                    + " to target index "
                    + indexSettings.getIndex().getName(),
                e
            );
        }
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
    protected Directory newFSDirectory(Path location, LockFactory lockFactory, IndexSettings indexSettings, int shardId)
        throws IOException {
        final Provider provider = Security.getProvider(DEFAULT_CRYPTO_PROVIDER);

        // Use index-level key resolver - store keys at index level

        Path indexDirectory = location.getParent().getParent(); // Go up two levels: index -> shard -> index
        MasterKeyProvider keyProvider = getKeyProvider(indexSettings);

        // Create a directory for the index-level keys
        Directory indexKeyDirectory = FSDirectory.open(indexDirectory);

        // Check if this is a clone/resize operation
        handleResizeOperation(indexSettings, indexDirectory);

        // Use shared resolver registry to prevent race conditions
        String indexUuid = indexSettings.getIndex().getUUID();
        String indexName = indexSettings.getIndex().getName();
        KeyResolver keyResolver = ShardKeyResolverRegistry
            .getOrCreateResolver(indexUuid, indexKeyDirectory, provider, keyProvider, shardId, indexName);

        // Get or create per-shard encryption metadata cache
        EncryptionMetadataCache encryptionMetadataCache = EncryptionMetadataCacheRegistry.getOrCreateCache(indexUuid, shardId, indexName);

        IndexModule.Type type = IndexModule.defaultStoreType(IndexModule.NODE_STORE_ALLOW_MMAP.get(indexSettings.getNodeSettings()));

        switch (type) {
            case HYBRIDFS -> {
                LOGGER.debug("Using HYBRIDFS directory with Direct I/O and block caching");
                final Set<String> nioExtensions = new HashSet<>(indexSettings.getValue(IndexModule.INDEX_STORE_HYBRID_NIO_EXTENSIONS));
                BufferPoolDirectory bufferPoolDirectory = createCryptoBufferPoolFSDirectory(
                    location,
                    lockFactory,
                    provider,
                    keyResolver,
                    encryptionMetadataCache
                );
                return new HybridCryptoDirectory(
                    lockFactory,
                    bufferPoolDirectory,
                    provider,
                    keyResolver,
                    encryptionMetadataCache,
                    nioExtensions
                );
            }
            case MMAPFS -> {
                LOGGER.info("MMAPFS not supported natively for index-level-encryption; using bufferpoolfs with block caching");
                return createCryptoBufferPoolFSDirectory(location, lockFactory, provider, keyResolver, encryptionMetadataCache);
            }
            case SIMPLEFS, NIOFS -> {
                LOGGER.debug("Using NIOFS directory for encrypted storage");
                return new CryptoNIOFSDirectory(lockFactory, location, provider, keyResolver, encryptionMetadataCache);
            }
            default -> throw new AssertionError("unexpected built-in store type [" + type + "]");
        }
    }

    @SuppressWarnings("unchecked")
    private BufferPoolDirectory createCryptoBufferPoolFSDirectory(
        Path location,
        LockFactory lockFactory,
        Provider provider,
        KeyResolver keyResolver,
        EncryptionMetadataCache encryptionMetadataCache
    ) throws IOException {
        /*
        * ================================
        * Shared Block Cache Architecture
        * ================================
        *
        * This method creates a CryptoBufferPoolFSDirectory that uses node-level shared resources
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

        // Ensure pool resources are initialized before creating directory
        PoolBuilder.PoolResources resources = ensurePoolInitialized();

        // Create a per-directory loader that uses this directory's keyIvResolver for decryption
        BlockLoader<RefCountedMemorySegment> loader = new CryptoDirectIOBlockLoader(
            resources.getSegmentPool(),
            keyResolver,
            encryptionMetadataCache
        );

        // Cache architecture: One shared Caffeine cache storage, multiple wrapper instances
        // - sharedBlockCache: Created once in ensurePoolInitialized(), holds the actual cache storage
        // - directoryCache: Per-directory wrapper that shares the underlying cache but uses its own loader
        // This design allows:
        // * Shared cache capacity across all directories
        // * Per-directory decryption via directory-specific loaders with unique keyIvResolvers
        // * Unified eviction policy managed by the shared cache
        CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> sharedCaffeineCache =
            (CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment>) resources.getBlockCache();

        BlockCache<RefCountedMemorySegment> directoryCache = new CaffeineBlockCache<>(
            sharedCaffeineCache.getCache(),
            loader,
            resources.getMaxCacheBlocks()
        );

        // Use the shared node-wide read-ahead worker
        // All shards/directories share a single queue and executor pool for better resource utilization
        Worker readaheadWorker = resources.getSharedReadaheadWorker();

        return new BufferPoolDirectory(
            location,
            lockFactory,
            provider,
            keyResolver,
            resources.getSegmentPool(),
            directoryCache,
            loader,
            readaheadWorker,
            encryptionMetadataCache
        );
    }

    /**
     * Set node settings for lazy pool initialization.
     * Called from CryptoDirectoryPlugin.createComponents() during node startup.
     *
     * @param settings the node settings for configuration
     */
    public static void setNodeSettings(Settings settings) {
        nodeSettings = settings;
    }

    /**
     * Set cluster service for accessing cluster metadata and initialize encryption context resolver.
     * Called from CryptoDirectoryPlugin.createComponents() during node startup.
     *
     * @param service the cluster service
     */
    public static void setClusterService(ClusterService service) {
        // Initialize encryption context resolver
        encryptionContextResolver = EncryptionContextResolverFactory.create(service);
    }

    /**
     * Lazily initialize the shared MemorySegmentPool and BlockCache on first cryptofs shard creation.
     * This prevents resource allocation on dedicated master nodes which never create shards.
     *
     * Thread Safety:
     * - Uses double-checked locking for initialization -- safe.
     *
     * @return the initialized pool resources
     */
    @SuppressWarnings("DoubleCheckedLocking")
    private static PoolBuilder.PoolResources ensurePoolInitialized() {
        if (poolResources == null) {
            synchronized (initLock) {
                if (poolResources == null) {
                    if (nodeSettings == null) {
                        throw new IllegalStateException("Node settings must be set before initializing pool resources");
                    }
                    LOGGER.info("Lazily initializing shared pool resources on first cryptofs shard creation");
                    poolResources = PoolBuilder.build(nodeSettings);
                }
            }
        }
        return poolResources;
    }

    /**
     * Close the shared pool resources if they were initialized.
     * Called from CryptoDirectoryPlugin.close() during node shutdown.
     */
    public static void closeSharedPool() {
        if (poolResources != null) {
            poolResources.close();
        }
    }

    /**
     * Get the shared block cache instance.
     * This can be used for cache invalidation when indices or shards are deleted.
     *
     * @return the shared block cache, or null if not initialized
     */
    public static BlockCache<?> getSharedBlockCache() {
        return poolResources != null ? poolResources.getBlockCache() : null;
    }
}

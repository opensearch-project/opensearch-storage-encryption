/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE;
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_INITIAL_SIZE;
import static org.opensearch.index.store.directio.DirectIoConfigs.READ_AHEAD_QUEUE_SIZE;
import static org.opensearch.index.store.directio.DirectIoConfigs.RESEVERED_POOL_SIZE_IN_BYTES;
import static org.opensearch.index.store.directio.DirectIoConfigs.WARM_UP_PERCENTAGE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;
import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.directio.CryptoDirectIODirectory;
import org.opensearch.index.store.hybrid.HybridCryptoDirectory;
import org.opensearch.index.store.iv.IndexKeyResolverRegistry;
import org.opensearch.index.store.iv.KeyIvResolver;
import org.opensearch.index.store.niofs.CryptoNIOFSDirectory;
import org.opensearch.index.store.pool.MemorySegmentPool;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.index.store.read_ahead.impl.QueuingWorker;
import org.opensearch.plugins.IndexStorePlugin;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

@SuppressForbidden(reason = "temporary")
/**
 * Factory for creating encrypted filesystem directories with support for various storage types.
 *
 * Supports:
 * - NIOFS: NIO-based encrypted file system
 * - HYBRIDFS: Hybrid directory with Direct I/O and block caching
 * - MMAPFS: Not supported (throws AssertionError)
 *
 * The factory maintains node-level shared resources (pool and cache) for efficient
 * memory utilization across all encrypted directories.
 */
public class CryptoDirectoryFactory implements IndexStorePlugin.DirectoryFactory {

    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectoryFactory.class);

    /**
     * Shared pool of RefCountedMemorySegments for Direct I/O operations.
     * Initialized once per node and shared across all CryptoDirectIODirectory instances.
     */
    private static volatile Pool<RefCountedMemorySegment> sharedSegmentPool;

    /**
     * Shared block cache for decrypted data blocks.
     * Initialized once per node and shared across all CryptoDirectIODirectory instances.
     */
    private static volatile BlockCache<RefCountedMemorySegment> sharedBlockCache;

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
     * Specifies the Key management plugin type to be used. The desired KMS
     * plugin should be installed.
     */
    public static final Setting<String> INDEX_KMS_TYPE_SETTING = new Setting<>("index.store.kms.type", "", Function.identity(), (s) -> {
        if (s == null || s.isEmpty()) {
            throw new SettingsException("index.store.kms.type must be set");
        }
    }, Property.NodeScope, Property.IndexScope);

    /**
     * Specifies the node-level TTL for data keys in seconds. 
     * Default is 3600 seconds (1 hour).
     * Set to -1 to disable key refresh (keys are loaded once and cached forever).
     * This setting applies globally to all indices.
     */
    public static final Setting<Integer> NODE_DATA_KEY_TTL_SECONDS_SETTING = Setting
        .intSetting(
            "node.store.data_key_ttl_seconds",
            3600,  // default: 3600 seconds (1 hour)
            -1,    // minimum: -1 means never refresh
            (value) -> {
                if (value != -1 && value < 1) {
                    throw new IllegalArgumentException("node.store.data_key_ttl_seconds must be -1 (never refresh) or a positive value");
                }
            },
            Property.NodeScope
        );

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
        BlockLoader<RefCountedMemorySegment> loader = new CryptoDirectIOBlockLoader(sharedSegmentPool, keyIvResolver);

        // Wrap the shared cache with directory-specific loader
        long maxBlocks = RESEVERED_POOL_SIZE_IN_BYTES / CACHE_BLOCK_SIZE;
        BlockCache<RefCountedMemorySegment> directoryCache = new CaffeineBlockCache<>(
            ((CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment>) sharedBlockCache).getCache(),
            loader,
            maxBlocks
        );

        // Create read-ahead worker for asynchronous prefetching
        int threads = Math.max(4, Runtime.getRuntime().availableProcessors() / 4);
        Worker readaheadWorker = new QueuingWorker(READ_AHEAD_QUEUE_SIZE, threads, directoryCache);

        return new CryptoDirectIODirectory(
            location,
            lockFactory,
            provider,
            keyIvResolver,
            sharedSegmentPool,
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
     * - Uses double-checked locking for initialization
     * - Safe to call multiple times (idempotent)
     *
     * Cache Removal Strategy:
     * - Uses removalListener to handle all removal causes (SIZE, EXPLICIT, REPLACED, etc.)
     * - Calls value.close() which atomically: (1) sets retired=true, (2) calls decRef()
     * - When refCount reaches 0, segment is returned to pool for reuse
     *
     * Note on evictionListener vs removalListener:
     * - evictionListener only fires for SIZE-based evictions
     * - removalListener fires for ALL removals (including explicit invalidation)
     * - We use removalListener to ensure retired flag is set for all removal paths
     */
    public static void initializeSharedPool() {
        if (sharedSegmentPool == null || sharedBlockCache == null) {
            synchronized (initLock) {
                if (sharedSegmentPool == null || sharedBlockCache == null) {
                    long maxBlocks = RESEVERED_POOL_SIZE_IN_BYTES / CACHE_BLOCK_SIZE;

                    // Initialize shared memory pool with warmup
                    sharedSegmentPool = new MemorySegmentPool(RESEVERED_POOL_SIZE_IN_BYTES, CACHE_BLOCK_SIZE);
                    LOGGER
                        .info(
                            "Creating shared pool with sizeBytes={}, segmentSize={}, totalSegments={}",
                            RESEVERED_POOL_SIZE_IN_BYTES,
                            CACHE_BLOCK_SIZE,
                            maxBlocks
                        );
                    sharedSegmentPool.warmUp((long) (maxBlocks * WARM_UP_PERCENTAGE));

                    @SuppressWarnings("resource")
                    ThreadPoolExecutor removalExec = new ThreadPoolExecutor(4, 8, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), r -> {
                        Thread t = new Thread(r, "block-cache-maint");
                        t.setDaemon(true);
                        return t;
                    });

                    // Initialize shared cache with removal listener
                    Cache<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> cache = Caffeine
                        .newBuilder()
                        .initialCapacity(CACHE_INITIAL_SIZE)
                        .recordStats()
                        .maximumSize(maxBlocks)
                        .removalListener((BlockCacheKey key, BlockCacheValue<RefCountedMemorySegment> value, RemovalCause cause) -> {
                            if (value != null) {
                                removalExec.execute(() -> {
                                    try {
                                        value.close();
                                    } catch (Throwable t) {
                                        LOGGER.warn("Failed to close cached value during removal {}", key, t);
                                    }
                                });
                            }
                        })
                        .build();

                    sharedBlockCache = new CaffeineBlockCache<>(cache, null, maxBlocks);

                    LOGGER.info("Creating shared block cache with maxSize={}, poolSize={}", maxBlocks, maxBlocks);

                    startTelemetry();
                }
            }
        }
    }

    /**
     * Publishes pool statistics to the logger for monitoring and debugging.
     */
    private static void publishPoolStats() {
        try {
            LOGGER.info("{}", sharedSegmentPool.poolStats());
        } catch (Exception e) {
            LOGGER.warn("Failed to log cache/pool stats", e);
        }
    }

    /**
     * Starts a background daemon thread that periodically logs pool statistics.
     * Logs every 5 minutes to help monitor memory usage and pool health.
     */
    private static void startTelemetry() {
        Thread loggerThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(Duration.ofMinutes(5));
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

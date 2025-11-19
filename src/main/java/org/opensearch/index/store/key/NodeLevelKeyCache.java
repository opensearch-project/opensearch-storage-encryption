/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.key;

import java.security.Key;
import java.util.Objects;
import java.util.concurrent.CompletionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.store.CryptoDirectoryFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

/**
 * Node-level cache for encryption keys used across all shards.
 * Provides centralized key management with global TTL configuration.
 * 
 * <p>This cache focuses solely on caching functionality, delegating health
 * monitoring and block management to {@link MasterKeyHealthMonitor}.
 * 
 * <p>Failure Handling Strategy:
 * <ul>
 *   <li>Keys are refreshed in background at TTL intervals (default: 1 hour)</li>
 *   <li>On refresh failure, old key is retained temporarily</li>
 *   <li>Failures are reported to MasterKeyHealthMonitor for block management</li>
 *   <li>System automatically recovers when Master Key Provider is restored</li>
 * </ul>
 * 
 * @opensearch.internal
 */
public class NodeLevelKeyCache {

    private static final Logger logger = LogManager.getLogger(NodeLevelKeyCache.class);

    private static NodeLevelKeyCache INSTANCE;

    private final LoadingCache<ShardCacheKey, Key> keyCache;
    private final long refreshDuration;
    private final long keyExpiryDuration;
    private final MasterKeyHealthMonitor healthMonitor;

    /**
     * Initializes the singleton instance with node-level settings and health monitor.
     * This should be called once during plugin initialization, after MasterKeyHealthMonitor
     * has been initialized.
     * 
     * @param nodeSettings the node settings containing global TTL configuration
     * @param healthMonitor the health monitor for failure/success reporting
     */
    public static synchronized void initialize(Settings nodeSettings, MasterKeyHealthMonitor healthMonitor) {
        if (INSTANCE == null) {
            TimeValue refreshInterval = CryptoDirectoryFactory.NODE_KEY_REFRESH_INTERVAL_SETTING.get(nodeSettings);
            TimeValue expiryInterval = CryptoDirectoryFactory.NODE_KEY_EXPIRY_INTERVAL_SETTING.get(nodeSettings);

            // Convert to seconds for internal use, handling negative values (disabled refresh/expiry)
            long refreshDuration = refreshInterval.getSeconds();
            long keyExpiryDuration = expiryInterval.getSeconds();

            INSTANCE = new NodeLevelKeyCache(refreshDuration, keyExpiryDuration, healthMonitor);

            if (refreshDuration < 0) {
                logger.info("Initialized NodeLevelKeyCache with refresh disabled");
            } else {
                logger
                    .info("Initialized NodeLevelKeyCache with refresh interval: {}, expiry interval: {}", refreshInterval, expiryInterval);
            }
        }
    }

    /**
     * Gets the singleton instance.
     * 
     * @return the NodeLevelKeyCache instance
     * @throws IllegalStateException if the cache has not been initialized
     */
    public static NodeLevelKeyCache getInstance() {
        if (INSTANCE == null) {
            throw new IllegalStateException("NodeLevelKeyCache not initialized.");
        }
        return INSTANCE;
    }

    /**
     * Constructs the cache with global TTL and expiration configuration.
     * <p>
     * This implements a cache with asynchronous refresh:
     * <ul>
     *  <li>When a key is first requested, it is loaded synchronously from the MasterKey Provider.</li>
     * 
     *  <li>After the key has been in the cache for the refresh TTL duration, 
     *      the next access triggers an asynchronous reload in the background.</li>
     * 
     *  <li>While the reload is in progress, it continues to return the 
     *      previously cached (stale) value to avoid blocking operations.</li>
     * 
     *  <li>If the reload fails, an exception is thrown (not suppressed), allowing Caffeine to track failures.</li>
     * 
     *  <li>Failures are reported to MasterKeyHealthMonitor which handles block management.</li>
     * </ul>
     * 
     * @param refreshDuration the refresh duration in seconds (-1 or 0 means never refresh)
     * @param keyExpiryDuration expiration duration in seconds (-1 or 0 means never expire)
     * @param healthMonitor the health monitor for failure/success reporting
     */
    private NodeLevelKeyCache(long refreshDuration, long keyExpiryDuration, MasterKeyHealthMonitor healthMonitor) {
        this.refreshDuration = refreshDuration;
        this.keyExpiryDuration = keyExpiryDuration;
        this.healthMonitor = Objects.requireNonNull(healthMonitor, "healthMonitor cannot be null");

        // Suppress Caffeine's internal logging to reduce log spam during key reload failures
        // This prevents duplicate exception logging from Caffeine's BoundedLocalCache
        java.util.logging.Logger.getLogger("com.github.benmanes.caffeine.cache").setLevel(java.util.logging.Level.SEVERE);

        // Check if refresh is disabled (negative or zero means disabled)
        if (refreshDuration <= 0) {
            // Create cache without refresh
            this.keyCache = Caffeine
                .newBuilder()
                // No refreshAfterWrite - keys are loaded once and cached forever
                .build(new CacheLoader<ShardCacheKey, Key>() {
                    @Override
                    public Key load(ShardCacheKey key) throws Exception {
                        return loadKey(key);
                    }
                    // No reload method needed since refresh is disabled
                });
        } else {
            // Create cache with refresh and expiration policy
            // Keys refresh at intervals, expire after specified duration on consecutive failures
            Caffeine<Object, Object> builder = Caffeine
                .newBuilder()
                .refreshAfterWrite(refreshDuration, java.util.concurrent.TimeUnit.SECONDS);

            // Only set expireAfterWrite if keyExpiryDuration is positive
            if (keyExpiryDuration > 0) {
                builder.expireAfterWrite(keyExpiryDuration, java.util.concurrent.TimeUnit.SECONDS);
            }

            this.keyCache = builder.build(new CacheLoader<ShardCacheKey, Key>() {
                @Override
                public Key load(ShardCacheKey key) throws Exception {
                    return loadKey(key);
                }

                @Override
                public Key reload(ShardCacheKey key, Key oldValue) throws Exception {
                    String indexUuid = key.getIndexUuid();
                    String indexName = key.getIndexName();

                    try {
                        KeyResolver resolver = ShardKeyResolverRegistry.getResolver(indexUuid, key.getShardId(), indexName);
                        Key newKey = ((DefaultKeyResolver) resolver).loadKeyFromMasterKeyProvider();

                        // Success: Report to health monitor
                        healthMonitor.reportSuccess(indexUuid, indexName);
                        return newKey;

                    } catch (Exception e) {
                        // Failure: Report to health monitor (will apply blocks)
                        healthMonitor.reportFailure(indexUuid, indexName, e);

                        // If it's already a KeyCacheException with clean message, just rethrow
                        if (e instanceof KeyCacheException) {
                            throw e;
                        }
                        // Only wrap unexpected exceptions
                        throw new KeyCacheException(
                            "Failed to reload key for index: " + indexName + ". Error: " + e.getMessage(),
                            null,  // No cause - eliminates ~40 lines of AWS SDK stack trace
                            true
                        );
                    }
                }
            });
        }
    }

    /**
     * Loads a key from Master Key Provider and reports status to health monitor.
     * 
     * @param key the shard cache key
     * @return the loaded encryption key
     * @throws Exception if key loading fails
     */
    private Key loadKey(ShardCacheKey key) throws Exception {
        String indexUuid = key.getIndexUuid();
        String indexName = key.getIndexName();

        // Get resolver from registry
        KeyResolver resolver = ShardKeyResolverRegistry.getResolver(indexUuid, key.getShardId(), indexName);
        if (resolver == null) {
            throw new IllegalStateException("No resolver registered for shard: " + key);
        }

        try {
            Key loadedKey = ((DefaultKeyResolver) resolver).loadKeyFromMasterKeyProvider();

            // Success: Report to health monitor
            healthMonitor.reportSuccess(indexUuid, indexName);
            return loadedKey;

        } catch (Exception e) {
            // Failure: Report to health monitor (will apply blocks)
            healthMonitor.reportFailure(indexUuid, indexName, e);

            // If it's already a KeyCacheException with clean message, just rethrow
            if (e instanceof KeyCacheException) {
                throw e;
            }
            // Only wrap unexpected exceptions
            throw new KeyCacheException("Failed to load key for index: " + indexName + ". Error: " + e.getMessage(), null, true);
        }
    }

    /**
     * Gets a key from the cache, loading it if necessary.
     * 
     * @param indexUuid the index UUID
     * @param shardId   the shard ID
     * @param indexName the index name
     * @return the encryption key
     * @throws Exception if key loading fails
     */
    public Key get(String indexUuid, int shardId, String indexName) throws Exception {
        Objects.requireNonNull(indexUuid, "indexUuid cannot be null");
        Objects.requireNonNull(indexName, "indexName cannot be null");

        try {
            return keyCache.get(new ShardCacheKey(indexUuid, shardId, indexName));
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            } else {
                throw new RuntimeException("Failed to get key from cache", cause);
            }
        }
    }

    /**
     * Evicts a key from the cache.
     * This should be called when a shard is closed.
     * 
     * @param indexUuid the index UUID
     * @param shardId   the shard ID
     * @param indexName the index name
     */
    public void evict(String indexUuid, int shardId, String indexName) {
        Objects.requireNonNull(indexUuid, "indexUuid cannot be null");
        Objects.requireNonNull(indexName, "indexName cannot be null");
        keyCache.invalidate(new ShardCacheKey(indexUuid, shardId, indexName));
    }

    /**
     * Gets the number of cached keys.
     * Useful for monitoring and testing.
     * 
     * @return the number of cached keys
     */
    public long size() {
        return keyCache.estimatedSize();
    }

    /**
     * Clears all cached keys.
     * This method is primarily for testing purposes.
     */
    public void clear() {
        keyCache.invalidateAll();
    }

    /**
     * Resets the singleton instance completely.
     * This method is primarily for testing purposes where complete cleanup is needed.
     */
    public static synchronized void reset() {
        if (INSTANCE != null) {
            INSTANCE.clear();
            INSTANCE = null;
        }
    }
}

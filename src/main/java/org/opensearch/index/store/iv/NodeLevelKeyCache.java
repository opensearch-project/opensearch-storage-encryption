/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.iv;

import java.security.Key;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

/**
 * Node-level cache for encryption keys used across all indices.
 * Provides centralized key management with global TTL configuration.
 * 
 * This cache replaces the per-resolver Caffeine caches to reduce memory overhead
 * and provide better cache utilization across indices.
 * 
 * @opensearch.internal
 */
public class NodeLevelKeyCache {

    private static final Logger logger = LogManager.getLogger(NodeLevelKeyCache.class);

    private static NodeLevelKeyCache INSTANCE;

    private final LoadingCache<CacheKey, Key> keyCache;
    private final long globalTtlMillis;

    /**
     * Cache key that includes index UUID and resolver reference.
     * The resolver is used for callbacks to load keys from MasterKeyProvider.
     */
    static class CacheKey {
        final String indexUuid;
        final DefaultKeyIvResolver resolver; // For callback to load key

        CacheKey(String indexUuid, DefaultKeyIvResolver resolver) {
            this.indexUuid = Objects.requireNonNull(indexUuid, "indexUuid cannot be null");
            this.resolver = resolver;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof CacheKey))
                return false;
            CacheKey that = (CacheKey) o;
            return Objects.equals(indexUuid, that.indexUuid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexUuid);
        }

        @Override
        public String toString() {
            return "CacheKey[indexUuid=" + indexUuid + "]";
        }
    }

    /**
     * Initializes the singleton instance with node-level settings.
     * This should be called once during plugin initialization.
     * 
     * @param nodeSettings the node settings containing global TTL configuration
     */
    public static synchronized void initialize(Settings nodeSettings) {
        if (INSTANCE == null) {
            int globalTtlSeconds = nodeSettings.getAsInt("node.store.data_key_ttl_seconds", 3600);
            INSTANCE = new NodeLevelKeyCache(globalTtlSeconds * 1000L);
            logger.info("Initialized NodeLevelKeyCache with global TTL: {} seconds", globalTtlSeconds);
        } else {
            logger.warn("NodeLevelKeyCache already initialized, ignoring duplicate initialization");
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
            throw new IllegalStateException("NodeLevelKeyCache not initialized. Call initialize() first.");
        }
        return INSTANCE;
    }

    /**
     * Constructs the cache with global TTL configuration.
     * 
     * @param globalTtlMillis the global TTL in milliseconds
     */
    private NodeLevelKeyCache(long globalTtlMillis) {
        this.globalTtlMillis = globalTtlMillis;

        // Create cache with refresh-only policy (no expiry)
        this.keyCache = Caffeine
            .newBuilder()
            // Only refresh keys at TTL - they never expire
            .refreshAfterWrite(globalTtlMillis, TimeUnit.MILLISECONDS)
            .build(new CacheLoader<CacheKey, Key>() {
                @Override
                public Key load(CacheKey key) throws Exception {
                    return loadKey(key);
                }

                @Override
                public Key reload(CacheKey key, Key oldValue) throws Exception {
                    logger.debug("Background refresh triggered for index: {}", key.indexUuid);

                    try {
                        Key newKey = key.resolver.loadKeyFromMasterKeyProvider();

                        return newKey;
                    } catch (Exception e) {
                        logger
                            .warn(
                                "MasterKey Provider access failed during background refresh for index {}. Using cached key. Error: {}",
                                key.indexUuid,
                                e.getMessage()
                            );
                        return oldValue;
                    }
                }
            });
    }

    /**
     * Loads a key by delegating to the resolver's loadKeyFromMasterKeyProvider method.
     * 
     * @param cacheKey the cache key
     * @return the loaded encryption key
     * @throws Exception if key loading fails
     */
    private Key loadKey(CacheKey cacheKey) throws Exception {
        if (cacheKey.resolver == null) {
            throw new IllegalStateException("Cannot load key without resolver");
        }

        logger.info("Loading key for index: {}", cacheKey.indexUuid);
        return cacheKey.resolver.loadKeyFromMasterKeyProvider();
    }

    /**
     * Gets a key from the cache, loading it if necessary.
     * 
     * @param indexUuid the index UUID
     * @param resolver the resolver to use for loading
     * @return the encryption key
     * @throws Exception if key loading fails
     */
    public Key get(String indexUuid, DefaultKeyIvResolver resolver) throws Exception {
        Objects.requireNonNull(indexUuid, "indexUuid cannot be null");
        Objects.requireNonNull(resolver, "resolver cannot be null");

        try {
            return keyCache.get(new CacheKey(indexUuid, resolver));
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
     * This should be called when an index is deleted.
     * @param indexUuid the index UUID
     */
    public void evict(String indexUuid) {
        Objects.requireNonNull(indexUuid, "indexUuid cannot be null");
        keyCache.invalidate(new CacheKey(indexUuid, null));
        logger.info("Evicted key for index: {}", indexUuid);
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
        logger.info("Cleared all cached keys");
    }

    /**
     * Resets the singleton instance.
     * This method is primarily for testing purposes.
     */
    public static synchronized void reset() {
        if (INSTANCE != null) {
            INSTANCE.clear();
            INSTANCE = null;
            logger.info("Reset NodeLevelKeyCache singleton");
        }
    }
}

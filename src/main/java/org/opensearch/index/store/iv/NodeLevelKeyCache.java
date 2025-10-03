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
    private final long globalTtlSeconds;

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

            INSTANCE = new NodeLevelKeyCache((long) globalTtlSeconds);

            if (globalTtlSeconds == -1) {
                logger.info("Initialized NodeLevelKeyCache with refresh disabled (TTL: -1)");
            } else {
                logger.info("Initialized NodeLevelKeyCache with global TTL: {} seconds", globalTtlSeconds);
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
     * Constructs the cache with global TTL configuration.
     * 
     * @param globalTtlSeconds the global TTL in seconds (-1 means never refresh)
     * This implements a non-expiring cache with asynchronous refresh semantics:
     * 
     *  - When a key is first requested, it is loaded synchronously from the MasterKey Provider.
     * 
     *  - After the key has been in the cache for the configured TTL duration, 
     *    the next access will trigger an asynchronous reload in the background.
     * 
     *  - While the reload is in progress, it continues to return the 
     *   previously cached (stale) value to avoid blocking operations.
     * 
     *  - If the reload fails due to any exception (e.g., MasterKeyProvider unavailable), 
     *   the cache retains and continues to serve the old value instead of 
     *   evicting it, ensuring operations can continue with the last known good key.
     * 
     */
    private NodeLevelKeyCache(long globalTtlSeconds) {
        this.globalTtlSeconds = globalTtlSeconds;

        // Check if refresh is disabled
        if (globalTtlSeconds == -1L) {
            // Create cache without refresh
            this.keyCache = Caffeine
                .newBuilder()
                // No refreshAfterWrite - keys are loaded once and cached forever
                .build(new CacheLoader<CacheKey, Key>() {
                    @Override
                    public Key load(CacheKey key) throws Exception {
                        return loadKey(key);
                    }
                    // No reload method needed since refresh is disabled
                });
        } else {
            // Create cache with refresh-only policy (no expiry)
            this.keyCache = Caffeine
                .newBuilder()
                // Only refresh keys at TTL - they never expire
                .refreshAfterWrite(globalTtlSeconds, TimeUnit.SECONDS)
                .build(new CacheLoader<CacheKey, Key>() {
                    @Override
                    public Key load(CacheKey key) throws Exception {
                        return loadKey(key);
                    }

                    @Override
                    public Key reload(CacheKey key, Key oldValue) throws Exception {
                        try {
                            Key newKey = key.resolver.loadKeyFromMasterKeyProvider();

                            return newKey;
                        } catch (Exception e) {
                            return oldValue;
                        }
                    }
                });
        }
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
     * Resets the singleton instance.
     * This method is primarily for testing purposes.
     */
    public static synchronized void reset() {
        if (INSTANCE != null) {
            INSTANCE.clear();
            INSTANCE = null;
        }
    }
}

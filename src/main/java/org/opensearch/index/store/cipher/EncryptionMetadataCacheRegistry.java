/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.cipher;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.opensearch.index.store.key.ShardCacheKey;

/**
 * Registry that ensures only one EncryptionMetadataCache instance exists per shard.
 * Uses shard-level granularity for better cache lifecycle management and isolation.
 * Mirrors the pattern used by ShardKeyResolverRegistry for consistent architecture.
 * 
 * @opensearch.internal
 */
public class EncryptionMetadataCacheRegistry {

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private EncryptionMetadataCacheRegistry() {}

    private static final ConcurrentMap<ShardCacheKey, EncryptionMetadataCache> cacheRegistry = new ConcurrentHashMap<>();

    /**
     * Gets or creates an EncryptionMetadataCache for the specified shard.
     * If a cache already exists for this shard, returns the existing instance.
     * Otherwise, creates a new cache and caches it.
     * 
     * <p>This method is thread-safe and prevents race conditions during cache creation.
     * 
     * @param indexUuid the unique identifier for the index
     * @param shardId   the shard ID
     * @return the EncryptionMetadataCache instance for this shard
     */
    public static EncryptionMetadataCache getOrCreateCache(String indexUuid, int shardId, String indexName) {
        ShardCacheKey key = new ShardCacheKey(indexUuid, shardId, indexName);
        return cacheRegistry.computeIfAbsent(key, k -> new EncryptionMetadataCache());
    }

    /**
     * Removes the cached EncryptionMetadataCache for the specified shard.
     * This should be called when a shard is closed to prevent memory leaks.
     * 
     * @param indexUuid the unique identifier for the index
     * @param shardId   the shard ID
     * @return the removed cache, or null if no cache was registered for this shard
     */
    public static EncryptionMetadataCache removeCache(String indexUuid, int shardId, String indexName) {
        return cacheRegistry.remove(new ShardCacheKey(indexUuid, shardId, indexName));
    }

    /**
     * Gets the number of cached instances.
     * Useful for monitoring and testing.
     * 
     * @return the number of cached EncryptionMetadataCache instances
     */
    public static int getCacheSize() {
        return cacheRegistry.size();
    }

    /**
     * Clears all cached instances.
     * This method is primarily for testing purposes.
     */
    public static void clearAll() {
        cacheRegistry.clear();
    }

    /**
     * Checks if a cache is registered for the specified shard.
     * 
     * @param indexUuid the unique identifier for the index
     * @param shardId   the shard ID
     * @return true if a cache is registered for this shard, false otherwise
     */
    public static boolean hasCache(String indexUuid, int shardId, String indexName) {
        return cacheRegistry.containsKey(new ShardCacheKey(indexUuid, shardId, indexName));
    }
}

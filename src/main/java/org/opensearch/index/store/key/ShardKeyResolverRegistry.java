/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.key;

import java.security.Provider;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.opensearch.common.crypto.MasterKeyProvider;

/**
 * Registry that ensures only one KeyResolver instance exists per shard.
 * This prevents race conditions when multiple components try to create resolvers
 * for the same shard simultaneously.
 * 
 * <p>Uses shard-level granularity (indexUuid + shardId) for precise lifecycle management
 * and cache isolation between shards.
 *
 * @opensearch.internal
 */
public class ShardKeyResolverRegistry {

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private ShardKeyResolverRegistry() {}

    private static final Logger logger = LogManager.getLogger(ShardKeyResolverRegistry.class);

    // Thread-safe cache of resolvers by shard
    private static final ConcurrentMap<ShardCacheKey, KeyResolver> resolverCache = new ConcurrentHashMap<>();

    /**
     * Gets or creates a KeyResolver for the specified shard.
     * If a resolver already exists for this shard, returns the existing instance.
     * Otherwise, creates a new resolver and caches it.
     * 
     * <p>This method is thread-safe and prevents race conditions during resolver creation.
     *
     * @param indexUuid      the unique identifier for the index
     * @param indexDirectory the directory where encryption keys are stored
     * @param provider       the JCE provider for cryptographic operations
     * @param keyProvider    the master key provider
     * @param shardId        the shard ID
     * @return the KeyResolver instance for this shard
     * @throws RuntimeException if resolver creation fails
     */
    public static KeyResolver getOrCreateResolver(
        String indexUuid,
        Directory indexDirectory,
        Provider provider,
        MasterKeyProvider keyProvider,
        int shardId,
        String indexName
    ) {
        ShardCacheKey key = new ShardCacheKey(indexUuid, shardId, indexName);
        return resolverCache.computeIfAbsent(key, k -> {
            try {
                return new DefaultKeyResolver(indexUuid, indexName, indexDirectory, provider, keyProvider, shardId);
            } catch (KeyCacheException e) {
                // KeyCacheException already has clean, actionable error message - just rethrow
                throw e;
            } catch (Exception e) {
                // Unexpected error - wrap with context
                throw new RuntimeException("Failed to create KeyResolver for shard: " + k, e);
            }
        });
    }

    /**
     * Gets the cached resolver for the specified shard.
     *
     * @param indexUuid the unique identifier for the index
     * @param shardId   the shard ID
     * @param indexName the index name
     * @return the KeyResolver instance for this shard, or null if none exists
     */
    public static KeyResolver getResolver(String indexUuid, int shardId, String indexName) {
        return resolverCache.get(new ShardCacheKey(indexUuid, shardId, indexName));
    }

    /**
     * Removes the cached resolver for the specified shard.
     * This should be called when a shard is closed to prevent memory leaks.
     * Also evicts the key from the node-level cache.
     *
     * @param indexUuid the unique identifier for the index
     * @param shardId   the shard ID
     * @param indexName the index name
     * @return the removed resolver, or null if no resolver was cached for this shard
     */
    public static KeyResolver removeResolver(String indexUuid, int shardId, String indexName) {
        ShardCacheKey key = new ShardCacheKey(indexUuid, shardId, indexName);
        KeyResolver removed = resolverCache.remove(key);
        if (removed != null) {
            // Evict from node-level cache when shard is removed
            try {
                NodeLevelKeyCache.getInstance().evict(indexUuid, shardId, indexName);
            } catch (IllegalStateException e) {
                logger.debug("Could not evict from NodeLevelKeyCache: {}", e.getMessage());
            }
        }
        return removed;
    }

    /**
     * Gets the number of cached resolvers.
     * Useful for monitoring and testing.
     *
     * @return the number of cached KeyResolver instances
     */
    public static int getCacheSize() {
        return resolverCache.size();
    }

    /**
     * Clears all cached resolvers.
     * This method is primarily for testing purposes.
     *
     * @return the number of resolvers that were removed
     */
    public static int clearCache() {
        int size = resolverCache.size();
        resolverCache.clear();
        return size;
    }

    /**
     * Checks if a resolver is cached for the specified shard.
     *
     * @param indexUuid the unique identifier for the index
     * @param shardId   the shard ID
     * @param indexName the index name
     * @return true if a resolver is cached for this shard, false otherwise
     */
    public static boolean hasResolver(String indexUuid, int shardId, String indexName) {
        return resolverCache.containsKey(new ShardCacheKey(indexUuid, shardId, indexName));
    }

    /**
     * Gets any resolver for the specified index UUID that exists on this node.
     * Since all shards of an index share the same master key (stored at index level),
     * any shard's resolver can be used to check key availability.
     * 
     * This is useful for operations that need a resolver but don't know which
     * specific shards exist locally (e.g., health checks, key availability checks).
     * 
     * @param indexUuid the unique identifier for the index
     * @return any KeyResolver for this index, or null if no shards exist on this node
     */
    public static KeyResolver getAnyResolverForIndex(String indexUuid) {
        for (Map.Entry<ShardCacheKey, KeyResolver> entry : resolverCache.entrySet()) {
            if (entry.getKey().getIndexUuid().equals(indexUuid)) {
                return entry.getValue();  // Return first match - all shards share same master key
            }
        }
        return null;  // No shards for this index on this node
    }

    /**
     * Gets any shard ID for the specified index UUID that exists on this node.
     * Since all shards of an index share the same master key, any shard ID can be used
     * for cache lookups.
     * 
     * @param indexUuid the unique identifier for the index
     * @return any shard ID for this index, or -1 if no shards exist on this node
     */
    public static int getAnyShardIdForIndex(String indexUuid) {
        for (ShardCacheKey key : resolverCache.keySet()) {
            if (key.getIndexUuid().equals(indexUuid)) {
                return key.getShardId();  // Return first match - all shards share same master key
            }
        }
        return -1;  // No shards for this index on this node
    }

    /**
     * Gets all unique index UUIDs that have encrypted shards on this node.
     * This deduplicates the shard-level entries to return index-level UUIDs.
     * 
     * <p>This is the definitive list of encrypted indices on this node because:
     * <ul>
     *   <li>Only encrypted indices have resolvers</li>
     *   <li>Only shards on this node get registered</li>
     *   <li>The registry is the single source of truth</li>
     * </ul>
     * 
     * <p>Useful for proactive health checks and monitoring operations that need to
     * iterate over all encrypted indices present on this node.
     * 
     * @return a set of all unique index UUIDs with cached resolvers on this node
     */
    public static Set<String> getAllIndexUuids() {
        Set<String> indexUuids = new HashSet<>();
        for (ShardCacheKey key : resolverCache.keySet()) {
            indexUuids.add(key.getIndexUuid());
        }
        return indexUuids;
    }
}

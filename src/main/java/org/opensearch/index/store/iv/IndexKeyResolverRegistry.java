/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.iv;

import java.io.IOException;
import java.security.Provider;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.opensearch.common.crypto.MasterKeyProvider;

/**
 * Registry that ensures only one KeyIvResolver instance exists per index UUID.
 * This prevents race conditions when both CryptoDirectoryFactory and CryptoEngineFactory
 * try to create resolvers for the same index simultaneously.
 * 
 * @opensearch.internal
 */
public class IndexKeyResolverRegistry {

    private static final Logger logger = LogManager.getLogger(IndexKeyResolverRegistry.class);

    // Thread-safe cache of resolvers by index UUID
    private static final ConcurrentMap<String, KeyIvResolver> resolverCache = new ConcurrentHashMap<>();

    /**
     * Gets or creates a KeyIvResolver for the specified index UUID.
     * If a resolver already exists for this index, returns the existing instance.
     * Otherwise, creates a new resolver and caches it.
     * 
     * This method is thread-safe and prevents race conditions during resolver creation.
     * 
     * @param indexUuid the unique identifier for the index
     * @param indexDirectory the directory where encryption keys are stored
     * @param provider the JCE provider for cryptographic operations
     * @param keyProvider the master key provider for KMS operations
     * @return the KeyIvResolver instance for this index
     * @throws RuntimeException if resolver creation fails
     */
    public static KeyIvResolver getOrCreateResolver(
        String indexUuid,
        Directory indexDirectory,
        Provider provider,
        MasterKeyProvider keyProvider
    ) {
        return resolverCache.computeIfAbsent(indexUuid, uuid -> {
            try {
                logger.debug("Creating new KeyIvResolver for index: {}", uuid);
                return new DefaultKeyIvResolver(indexUuid, indexDirectory, provider, keyProvider);
            } catch (IOException e) {
                logger.error("Failed to create KeyIvResolver for index: {}", uuid, e);
                throw new RuntimeException("Failed to create KeyIvResolver for index: " + uuid, e);
            }
        });
    }

    /**
     * Removes the cached resolver for the specified index UUID.
     * This should be called when an index is deleted to prevent memory leaks.
     * Also evicts the key from the node-level cache.
     * 
     * @param indexUuid the unique identifier for the index
     * @return the removed resolver, or null if no resolver was cached for this index
     */
    public static KeyIvResolver removeResolver(String indexUuid) {
        KeyIvResolver removed = resolverCache.remove(indexUuid);
        if (removed != null) {
            // Evict from node-level cache when index is removed
            try {
                NodeLevelKeyCache.getInstance().evict(indexUuid);
            } catch (IllegalStateException e) {
                // NodeLevelKeyCache might not be initialized in tests
                logger.debug("Could not evict from NodeLevelKeyCache: {}", e.getMessage());
            }
            logger.debug("Removed cached KeyIvResolver and evicted key for index: {}", indexUuid);
        }
        return removed;
    }

    /**
     * Gets the number of cached resolvers.
     * Useful for monitoring and testing.
     * 
     * @return the number of cached KeyIvResolver instances
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
        logger.debug("Cleared {} cached KeyIvResolver instances", size);
        return size;
    }

    /**
     * Checks if a resolver is cached for the specified index UUID.
     * 
     * @param indexUuid the unique identifier for the index
     * @return true if a resolver is cached for this index, false otherwise
     */
    public static boolean hasResolver(String indexUuid) {
        return resolverCache.containsKey(indexUuid);
    }
}

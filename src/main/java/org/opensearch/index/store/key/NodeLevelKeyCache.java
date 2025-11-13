/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.key;

import java.security.Key;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.store.CryptoDirectoryFactory;
import org.opensearch.transport.client.Client;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

/**
 * Node-level cache for encryption keys used across all shards.
 * Provides centralized key management with global TTL configuration.
 * 
 * This cache replaces the per-resolver Caffeine caches to reduce memory overhead
 * and provide better cache utilization across shards.
 * and provide better cache utilization across indices.
 * 
 * <p>Failure Handling Strategy:
 * <ul>
 *   <li>Keys are refreshed in background at TTL intervals (default: 1 hour)</li>
 *   <li>On refresh failure, old key is retained temporarily</li>
 *   <li>After multiple consecutive failures (default: 3), keys expire</li>
 *   <li>Load retries are throttled to prevent DOS (default: 5 minutes between attempts)</li>
 *   <li>System automatically recovers when Master Key Provider is restored</li>
 * </ul>
 * 
 * @opensearch.internal
 */
public class NodeLevelKeyCache implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(NodeLevelKeyCache.class);

    private static NodeLevelKeyCache INSTANCE;

    private final LoadingCache<ShardCacheKey, Key> keyCache;
    private final long refreshDuration;
    private final long keyExpiryDuration;
    private final Client client;
    private final ClusterService clusterService;

    // Track failures per index to implement write block protection
    private final ConcurrentHashMap<String, FailureState> failureTracker;

    // O(1) UUID to index name cache for efficient lookups
    // Updated automatically via ClusterStateListener when metadata changes
    // AtomicReference ensures race-free atomic swap during cache rebuilds
    private final AtomicReference<Map<String, String>> uuidToNameCache;

    // Health monitoring for automatic recovery when KMS is restored
    private final ScheduledExecutorService healthCheckExecutor;
    private volatile ScheduledFuture<?> healthCheckTask = null;
    private static final long HEALTH_CHECK_INTERVAL_SECONDS = 30;

    /**
     * Tracks failure state for an index and block status.
     */
    static class FailureState {
        final AtomicLong lastFailureTimeMillis;
        final AtomicReference<Exception> lastException;
        volatile boolean blocksApplied = false;

        FailureState(Exception exception) {
            this.lastFailureTimeMillis = new AtomicLong(System.currentTimeMillis());
            this.lastException = new AtomicReference<>(exception);
        }

        void recordFailure(Exception exception) {
            lastFailureTimeMillis.set(System.currentTimeMillis());
            lastException.set(exception);
        }
    }

    /**
     * Initializes the singleton instance with node-level settings, client, and cluster service.
     * This should be called once during plugin initialization.
     * 
     * @param nodeSettings the node settings containing global TTL configuration
     * @param client the client for cluster state updates (write block operations)
     * @param clusterService the cluster service for looking up index metadata
     */
    public static synchronized void initialize(Settings nodeSettings, Client client, ClusterService clusterService) {
        if (INSTANCE == null) {
            TimeValue refreshInterval = CryptoDirectoryFactory.NODE_KEY_REFRESH_INTERVAL_SETTING.get(nodeSettings);
            TimeValue expiryInterval = CryptoDirectoryFactory.NODE_KEY_EXPIRY_INTERVAL_SETTING.get(nodeSettings);

            // Convert to seconds for internal use, handling negative values (disabled refresh/expiry)
            long refreshDuration = refreshInterval.getSeconds();
            long keyExpiryDuration = expiryInterval.getSeconds();

            INSTANCE = new NodeLevelKeyCache(refreshDuration, keyExpiryDuration, client, clusterService);

            // Register as cluster state listener to keep UUID->name cache synchronized
            if (clusterService != null) {
                clusterService.addListener(INSTANCE);
            }

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
     * This implements a cache with asynchronous refresh and proactive failure monitoring:
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
     *  <li>On first failure (load or reload), read+write blocks are applied immediately to protect data.</li>
     * 
     *  <li>A proactive health monitoring thread (always running) checks failed indices every 30 seconds.</li>
     * 
     *  <li>When Master Key Provider is restored, blocks are automatically removed and shards retry.</li>
     * </ul>
     * 
     * @param refreshDuration the refresh duration in seconds (-1 or 0 means never refresh)
     * @param keyExpiryDuration expiration duration in seconds (-1 or 0 means never expire)
     */
    private NodeLevelKeyCache(long refreshDuration, long keyExpiryDuration, Client client, ClusterService clusterService) {
        this.refreshDuration = refreshDuration;
        this.keyExpiryDuration = keyExpiryDuration;

        // Validate required dependencies
        this.client = Objects.requireNonNull(client, "client cannot be null");
        this.clusterService = Objects.requireNonNull(clusterService, "clusterService cannot be null");

        this.failureTracker = new ConcurrentHashMap<>();
        this.uuidToNameCache = new AtomicReference<>(Collections.emptyMap());

        // Initialize UUID -> name cache from current cluster state
        if (clusterService.state() != null) {
            rebuildUuidToNameCache(clusterService.state().metadata());
        }

        // Initialize and start health check executor (always running for proactive monitoring)
        this.healthCheckExecutor = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "encryption-key-health-check"));
        // Start proactive health monitoring (always running)
        logger.info("Starting proactive KMS health monitoring (checking every {} seconds)", HEALTH_CHECK_INTERVAL_SECONDS);
        healthCheckTask = healthCheckExecutor
            .scheduleAtFixedRate(
                this::checkKmsHealthAndRecover,
                HEALTH_CHECK_INTERVAL_SECONDS,
                HEALTH_CHECK_INTERVAL_SECONDS,
                TimeUnit.SECONDS
            );

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
            Caffeine<Object, Object> builder = Caffeine.newBuilder().refreshAfterWrite(refreshDuration, TimeUnit.SECONDS);

            // Only set expireAfterWrite if keyExpiryDuration is positive
            if (keyExpiryDuration > 0) {
                builder.expireAfterWrite(keyExpiryDuration, TimeUnit.SECONDS);
            }

            this.keyCache = builder.build(new CacheLoader<ShardCacheKey, Key>() {
                @Override
                public Key load(ShardCacheKey key) throws Exception {
                    return loadKey(key);
                }

                @Override
                public Key reload(ShardCacheKey key, Key oldValue) throws Exception {
                    try {
                        KeyResolver resolver = ShardKeyResolverRegistry.getResolver(key.getIndexUuid(), key.getShardId());
                        Key newKey = ((DefaultKeyResolver) resolver).loadKeyFromMasterKeyProvider();

                        // Success: Remove blocks if they were applied, then clear failure state
                        String indexUuid = key.getIndexUuid();
                        FailureState state = failureTracker.remove(indexUuid);
                        if (state != null && state.blocksApplied && hasBlocks(indexUuid)) {
                            removeBlocks(indexUuid);
                            logger.info("Removed blocks from index after successful key reload: {}", indexUuid);
                        }
                        logger.info("Successfully reloaded key for index: {}", indexUuid);
                        return newKey;

                    } catch (Exception e) {
                        String indexUuid = key.getIndexUuid();

                        // Get or create failure state
                        FailureState state = failureTracker.computeIfAbsent(indexUuid, k -> new FailureState(e));
                        state.recordFailure(e);

                        // Apply both blocks on FIRST failure
                        if (!state.blocksApplied) {
                            applyBlocks(indexUuid);
                            state.blocksApplied = true;
                            logger.warn("Applied read+write blocks on refresh failure: {}", indexUuid);
                        }

                        throw new KeyCacheException(
                            "Failed to reload key for index: " + indexUuid + ". Error: " + e.getMessage(),
                            null,  // No cause - eliminates ~40 lines of AWS SDK stack trace
                            true
                        );
                    }
                }
            });
        }

    }

    /**
     * Loads a key from Master Key Provider and handles failures by applying blocks.
     * 
     * @param key the shard cache key
     * @return the loaded encryption key
     * @throws Exception if key loading fails
     */
    private Key loadKey(ShardCacheKey key) throws Exception {
        // Get resolver from registry
        KeyResolver resolver = ShardKeyResolverRegistry.getResolver(key.getIndexUuid(), key.getShardId());
        if (resolver == null) {
            throw new IllegalStateException("No resolver registered for shard: " + key);
        }

        try {
            Key loadedKey = ((DefaultKeyResolver) resolver).loadKeyFromMasterKeyProvider();

            // Clear failure state on successful load
            failureTracker.remove(key.getIndexUuid());

            logger.info("Successfully loaded key for index: {}", key.getIndexUuid());
            return loadedKey;

        } catch (Exception e) {
            String indexUuid = key.getIndexUuid();

            // Check if blocks already applied (fail fast)
            FailureState state = failureTracker.get(indexUuid);
            if (state != null && state.blocksApplied) {
                throw new KeyCacheException("Index blocked due to key unavailability: " + indexUuid, null, true);
            }

            // First load failure: create state and apply blocks
            if (state == null) {
                state = new FailureState(e);
                failureTracker.put(indexUuid, state);
            } else {
                state.recordFailure(e);
            }

            applyBlocks(indexUuid);
            state.blocksApplied = true;
            logger.error("Applied read+write blocks on load failure: {}", indexUuid);

            throw new KeyCacheException("Failed to load key for index: " + indexUuid + ". Error: " + e.getMessage(), null, true);
        }
    }

    /**
     * Called when the cluster state changes.
     * Rebuilds the UUID→name cache when metadata changes to keep it synchronized.
     * 
     * @param event the cluster change event
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.metadataChanged()) {
            rebuildUuidToNameCache(event.state().metadata());
        }
    }

    /**
     * Rebuilds the UUID→name cache from the current cluster metadata.
     * Uses atomic swap to eliminate race conditions during cache updates.
     * This provides O(1) lookups for index names by UUID.
     * 
     * @param metadata the cluster metadata
     */
    private void rebuildUuidToNameCache(Metadata metadata) {
        try {
            if (metadata == null) {
                return;
            }

            // Build new immutable cache from current metadata
            Map<String, String> newCache = new HashMap<>();
            for (IndexMetadata indexMetadata : metadata.indices().values()) {
                newCache.put(indexMetadata.getIndexUUID(), indexMetadata.getIndex().getName());
            }

            uuidToNameCache.set(Collections.unmodifiableMap(newCache));
        } catch (Exception e) {
            logger.error("Failed to rebuild UUID→name cache", e);
        }
    }

    /**
     * Gets the index name for a given UUID using the O(1) atomic cache.
     * Returns null if UUID not found in cache (index doesn't exist or was deleted).
     * 
     * @param indexUuid the index UUID
     * @return the index name, or null if not found
     */
    private String getIndexNameFromUuid(String indexUuid) {
        return uuidToNameCache.get().get(indexUuid);
    }

    /**
     * Checks if read or write blocks are currently applied to the index.
     * Uses O(1) cache lookup for index name, then O(1) metadata lookup.
     * 
     * @param indexUuid the index UUID
     * @return true if either read or write blocks are applied, false otherwise
     */
    private boolean hasBlocks(String indexUuid) {
        try {
            // Get index name from O(1) cache
            String indexName = getIndexNameFromUuid(indexUuid);
            ClusterState clusterState = clusterService.state();
            IndexMetadata indexMetadata = clusterState.metadata().index(indexName);

            if (indexName == null || clusterState == null || indexMetadata == null) {
                return false;
            }

            Settings indexSettings = indexMetadata.getSettings();

            // Check for read or write blocks
            boolean readBlock = indexSettings.getAsBoolean("index.blocks.read", false);
            boolean writeBlock = indexSettings.getAsBoolean("index.blocks.write", false);

            return readBlock || writeBlock;
        } catch (Exception e) {
            logger.warn("Failed to check blocks for index UUID: {}", indexUuid, e);
            return false; // Assume no blocks on error
        }
    }

    /**
     * Applies read and write blocks to the specified index to prevent all operations 
     * when encryption key is unavailable.
     * 
     * @param indexUuid the index UUID
     */
    private void applyBlocks(String indexUuid) {
        try {
            // Get index name from UUID via cluster state
            String indexName = getIndexNameFromUuid(indexUuid);
            if (indexName == null) {
                logger.warn("Cannot apply blocks: index name not found for UUID: {}", indexUuid);
                return;
            }

            // Apply both read and write blocks
            Settings settings = Settings.builder().put("index.blocks.read", true).put("index.blocks.write", true).build();

            UpdateSettingsRequest request = new UpdateSettingsRequest(settings, indexName);
            client.admin().indices().updateSettings(request).actionGet();

            logger.info("Successfully applied read+write blocks to index: {}", indexName);
        } catch (Exception e) {
            logger.error("Failed to apply blocks to index UUID: {}, error: {}", indexUuid, e.getMessage(), e);
        }
    }

    /**
     * Removes read and write blocks from the specified index when the encryption key 
     * becomes available again. This restores full access after key recovery.
     * 
     * @param indexUuid the index UUID
     */
    private void removeBlocks(String indexUuid) {
        try {
            // Get index name from UUID via cluster state
            String indexName = getIndexNameFromUuid(indexUuid);
            if (indexName == null) {
                logger.warn("Cannot remove blocks: index name not found for UUID: {}", indexUuid);
                return;
            }

            // Remove both read and write blocks
            Settings settings = Settings.builder().putNull("index.blocks.read").putNull("index.blocks.write").build();

            UpdateSettingsRequest request = new UpdateSettingsRequest(settings, indexName);
            client.admin().indices().updateSettings(request).actionGet();

            logger.info("Successfully removed read+write blocks from index: {}", indexName);
        } catch (Exception e) {
            logger.error("Failed to remove blocks from index UUID: {}, error: {}", indexUuid, e.getMessage(), e);
        }
    }

    /**
     * Gets a key from the cache, loading it if necessary.
     * 
     * @param indexUuid the index UUID
     * @param shardId   the shard ID
     * @return the encryption key
     * @throws Exception if key loading fails
     */
    public Key get(String indexUuid, int shardId) throws Exception {
        Objects.requireNonNull(indexUuid, "indexUuid cannot be null");

        try {
            return keyCache.get(new ShardCacheKey(indexUuid, shardId));
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
     */
    public void evict(String indexUuid, int shardId) {
        Objects.requireNonNull(indexUuid, "indexUuid cannot be null");
        keyCache.invalidate(new ShardCacheKey(indexUuid, shardId));
        failureTracker.remove(indexUuid);
        logger.debug("Evicted key and cleared failure state for index: {}", indexUuid);
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
     * Clears all cached keys and failure states.
     * This method is primarily for testing purposes.
     */
    public void clear() {
        keyCache.invalidateAll();
        failureTracker.clear();
    }

    /**
     * Triggers cluster reroute with retry_failed to recover shards that failed 
     * due to unavailable encryption keys. This allows RED indices to automatically
     * recover once keys become available again.
     * 
     * @param recoveredCount number of indices recovered
     */
    private void triggerShardRetry(int recoveredCount) {
        try {
            ClusterRerouteRequest request = new ClusterRerouteRequest();
            request.setRetryFailed(true);

            client.admin().cluster().reroute(request).actionGet();

            logger.info("Triggered shard retry for {} recovered indices", recoveredCount);
        } catch (Exception e) {
            logger.warn("Failed to trigger shard retry: {}", e.getMessage());
            // Non-fatal - shards will recover on next allocation round
        }
    }

    /**
     * Proactive health check that attempts to recover blocked indices by trying to load their keys.
     * This runs continuously every 30 seconds regardless of failure state (proactive monitoring).
     * 
     * For each index in the failure tracker with blocks applied:
     * 1. Attempts to load the key for that specific index
     * 2. If successful, removes blocks and clears failure state
     * 3. If still failing, keeps blocks and continues monitoring
     * 4. After all checks, triggers shard retry to recover RED indices
     * 
     * This runs on a single thread and checks all blocked indices managed by this cache.
     */
    private void checkKmsHealthAndRecover() {
        try {
            // Get snapshot of blocked indices (only those we blocked)
            Set<String> blockedIndices = new HashSet<>(failureTracker.keySet());

            if (blockedIndices.isEmpty()) {
                return;
            }

            logger.info("Proactive KMS health check for {} indices with blocks", blockedIndices.size());

            int recoveredCount = 0;

            // Check each index individually (different keys!)
            for (String indexUuid : blockedIndices) {
                FailureState state = failureTracker.get(indexUuid);
                if (state == null || !state.blocksApplied) {
                    continue;
                }

                try {
                    // Get any resolver for THIS specific index (all shards share the same master key)
                    KeyResolver resolver = ShardKeyResolverRegistry.getAnyResolverForIndex(indexUuid);
                    if (resolver == null) {
                        // Index deleted or no shards on this node, clean up
                        failureTracker.remove(indexUuid);
                        logger.info("Removed deleted index from failure tracker: {}", indexUuid);
                        continue;
                    }

                    // Try to load THIS index's specific key
                    Key key = ((DefaultKeyResolver) resolver).loadKeyFromMasterKeyProvider();

                    // Success for THIS index! Remove blocks
                    if (hasBlocks(indexUuid)) {
                        removeBlocks(indexUuid);
                        logger.info("Removed blocks from recovered index: {}", indexUuid);
                    }

                    failureTracker.remove(indexUuid);
                    recoveredCount++;

                } catch (Exception e) {
                    // This index's key still unavailable, continue monitoring
                    logger.debug("Key still unavailable for index {}: {}", indexUuid, e.getMessage());
                }
            }

            // After removing blocks, trigger shard retry to recover RED indices
            if (recoveredCount > 0) {
                logger.info("Recovered {} indices, triggered shard retry", recoveredCount);
                triggerShardRetry(recoveredCount);
            }

        } catch (Exception e) {
            logger.error("Error during proactive KMS health check", e);
            // Keep monitoring thread running even on error
        }
    }

    /**
     * Resets the singleton instance.
     * This method is primarily for testing purposes.
     */
    public static synchronized void reset() {
        if (INSTANCE != null) {
            // Unregister cluster state listener
            if (INSTANCE.clusterService != null) {
                INSTANCE.clusterService.removeListener(INSTANCE);
                logger.info("Unregistered NodeLevelKeyCache as ClusterStateListener");
            }

            INSTANCE.clear();

            // Shutdown health check executor and cancel scheduled task
            if (INSTANCE.healthCheckTask != null) {
                INSTANCE.healthCheckTask.cancel(false);
            }
            if (INSTANCE.healthCheckExecutor != null) {
                INSTANCE.healthCheckExecutor.shutdownNow();
            }

            INSTANCE = null;
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.key;

import java.security.Key;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.transport.client.Client;

/**
 * Health monitor for encryption keys across all encrypted indices on a node.
 * Runs proactive background checks to detect MasterKey Provider issues early and automatically
 * recovers indices when keys become available again.
 * 
 * <p>Responsibilities:
 * <ul>
 *   <li>Proactive health checks: Validates ALL encrypted indices periodically</li>
 *   <li>Failure tracking: Tracks which indices have key availability issues</li>
 *   <li>Block management: Applies/removes read+write blocks for protection</li>
 *   <li>Automatic recovery: Removes blocks and triggers shard retry when MasterKey Provider restored</li>
 * </ul>
 * 
 * @opensearch.internal
 */
public class MasterKeyHealthMonitor {

    private static final Logger logger = LogManager.getLogger(MasterKeyHealthMonitor.class);

    private static MasterKeyHealthMonitor INSTANCE;

    private final Client client;
    private final ClusterService clusterService;

    // Track failures per index to implement write block protection
    private final ConcurrentHashMap<String, FailureState> failureTracker;

    // Health monitoring for automatic recovery when MasterKey Provider is restored
    private final ScheduledExecutorService healthCheckExecutor;
    private volatile ScheduledFuture<?> healthCheckTask = null;

    // Random initial delay (0-300 seconds) to prevent thundering herd when cluster starts
    private static final long HEALTH_CHECK_INITIAL_DELAY_MAX_SECONDS = 300;
    private static final long HEALTH_CHECK_INTERVAL_SECONDS = 3600;

    // Grace period between write and read block (allows in-flight reads to complete)
    private static final long BLOCK_GRACE_PERIOD_SECONDS = 10;

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
     * Initializes the singleton instance with client and cluster service.
     * This should be called once during plugin initialization.
     * 
     * @param settings the node settings
     * @param client the client for cluster state updates (block operations)
     * @param clusterService the cluster service for looking up index metadata
     */
    public static synchronized void initialize(Settings settings, Client client, ClusterService clusterService) {
        if (INSTANCE == null) {
            INSTANCE = new MasterKeyHealthMonitor(client, clusterService);
            logger.info("Initialized MasterKeyHealthMonitor");
        }
    }

    /**
     * Gets the singleton instance.
     * 
     * @return the MasterKeyHealthMonitor instance
     * @throws IllegalStateException if the monitor has not been initialized
     */
    public static MasterKeyHealthMonitor getInstance() {
        if (INSTANCE == null) {
            throw new IllegalStateException("MasterKeyHealthMonitor not initialized.");
        }
        return INSTANCE;
    }

    /**
     * Private constructor.
     * Only creates the monitor instance without starting background threads.
     * Call {@link #start()} to begin health monitoring.
     */
    private MasterKeyHealthMonitor(Client client, ClusterService clusterService) {
        this.client = Objects.requireNonNull(client, "client cannot be null");
        this.clusterService = Objects.requireNonNull(clusterService, "clusterService cannot be null");
        this.failureTracker = new ConcurrentHashMap<>();

        // Initialize executor but don't start health check yet
        this.healthCheckExecutor = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "encryption-key-health-check"));
    }

    /**
     * Starts the proactive health monitoring background thread.
     * This should be called after both MasterKeyHealthMonitor and NodeLevelKeyCache
     * have been initialized to avoid race conditions.
     * 
     * This method is idempotent - calling it multiple times has no effect.
     * 
     * Uses random initial delay (0-300 seconds) to prevent thundering herd problem
     * where all nodes in a cluster simultaneously hit MasterKey Provider after restart.
     */
    public static synchronized void start() {
        if (INSTANCE == null) {
            throw new IllegalStateException("MasterKeyHealthMonitor not initialized.");
        }

        if (INSTANCE.healthCheckTask == null) {
            // Add random jitter to prevent all nodes hitting MasterKey Provider simultaneously
            long randomInitialDelay = ThreadLocalRandom.current().nextLong(HEALTH_CHECK_INITIAL_DELAY_MAX_SECONDS + 1);

            INSTANCE.healthCheckTask = INSTANCE.healthCheckExecutor
                .scheduleAtFixedRate(
                    INSTANCE::checkKmsHealthAndRecover,
                    randomInitialDelay,
                    HEALTH_CHECK_INTERVAL_SECONDS,
                    TimeUnit.SECONDS
                );
        }
    }

    /**
     * Reports a key load/reload failure for an index.
     * Applies blocks on first failure to protect data.
     * 
     * @param indexUuid the index UUID
     * @param indexName the index name
     * @param exception the failure exception
     */
    public void reportFailure(String indexUuid, String indexName, Exception exception) {
        FailureState state = failureTracker.get(indexUuid);

        if (state == null) {
            // First failure
            state = new FailureState(exception);
            failureTracker.put(indexUuid, state);

            // Apply blocks on first failure
            applyBlocks(indexName);
            state.blocksApplied = true;

        } else {
            // Subsequent failure
            state.recordFailure(exception);
        }
    }

    /**
     * Reports successful key load/reload for an index.
     * Removes blocks if they were previously applied.
     * 
     * @param indexUuid the index UUID
     * @param indexName the index name
     */
    public void reportSuccess(String indexUuid, String indexName) {
        FailureState state = failureTracker.remove(indexUuid);

        if (state != null && state.blocksApplied && hasBlocks(indexName)) {
            removeBlocks(indexName);
        }
    }

    /**
     * Checks if read or write blocks are currently applied to the index.
     * 
     * @param indexName the index name
     * @return true if either read or write blocks are applied, false otherwise
     */
    private boolean hasBlocks(String indexName) {
        try {
            if (indexName == null) {
                return false;
            }

            ClusterState clusterState = clusterService.state();
            IndexMetadata indexMetadata = clusterState.metadata().index(indexName);

            if (clusterState == null || indexMetadata == null) {
                return false;
            }

            Settings indexSettings = indexMetadata.getSettings();

            // Check for read or write blocks
            boolean readBlock = indexSettings.getAsBoolean("index.blocks.read", false);
            boolean writeBlock = indexSettings.getAsBoolean("index.blocks.write", false);

            return readBlock || writeBlock;
        } catch (Exception e) {
            return false; // Assume no blocks on error
        }
    }

    /**
     * Applies read and write blocks sequentially to allow graceful degradation.
     * First applies write block to prevent new data, schedules read block after a grace period
     * to allow in-flight reads to complete using cached keys.
     * 
     * Operations are async to avoid blocking the health check thread.
     * 
     * @param indexName the index name
     */
    private void applyBlocks(String indexName) {
        try {
            if (indexName == null) {
                return;
            }

            // Apply write block first - prevents new writes that would need encryption keys
            // Async: don't wait for cluster state update
            Settings writeBlockSettings = Settings.builder().put("index.blocks.write", true).build();
            UpdateSettingsRequest writeBlockRequest = new UpdateSettingsRequest(writeBlockSettings, indexName);
            client.admin().indices().updateSettings(writeBlockRequest);
            logger.info("Applied write block to index {}", indexName);

            // Step 2: Schedule read block after grace period on existing executor
            healthCheckExecutor.schedule(() -> {
                try {
                    Settings readBlockSettings = Settings.builder().put("index.blocks.read", true).build();
                    UpdateSettingsRequest readBlockRequest = new UpdateSettingsRequest(readBlockSettings, indexName);
                    client.admin().indices().updateSettings(readBlockRequest);
                } catch (Exception e) {
                    logger.error("Failed to apply read block to index {}: {}", indexName, e.getMessage());
                }
            }, BLOCK_GRACE_PERIOD_SECONDS, TimeUnit.SECONDS);

        } catch (Exception e) {
            logger.error("Failed to apply write block to index {}: {}", indexName, e.getMessage(), e);
        }
    }

    /**
     * Removes read and write blocks from the specified index when the encryption key 
     * becomes available again. This restores full access after key recovery.
     * 
     * Operations are async to avoid blocking the health check thread.
     * 
     * @param indexName the index name
     */
    private void removeBlocks(String indexName) {
        try {
            if (indexName == null) {
                return;
            }

            // Remove both read and write blocks
            // Async: don't wait for cluster state update
            Settings settings = Settings.builder().putNull("index.blocks.read").putNull("index.blocks.write").build();
            UpdateSettingsRequest request = new UpdateSettingsRequest(settings, indexName);
            client.admin().indices().updateSettings(request);

            logger.info("Removed blocks from index {}", indexName);

        } catch (Exception e) {
            logger.error("Failed to remove blocks from index {}: {}", indexName, e.getMessage(), e);
        }
    }

    /**
     * Triggers cluster reroute with retry_failed to recover shards that failed 
     * due to unavailable encryption keys. This allows RED indices to automatically
     * recover once keys become available again.
     * 
     * Uses virtual threads for non-blocking execution with exponential backoff retry.
     * 
     * @param recoveredCount number of indices recovered
     */
    private void triggerShardRetry(int recoveredCount) {
        // Use virtual thread - lightweight, no thread pool needed
        Thread.startVirtualThread(() -> {
            retryWithBackoff(() -> {
                ClusterRerouteRequest request = new ClusterRerouteRequest();
                request.setRetryFailed(true);
                client.admin().cluster().reroute(request).actionGet();
                logger.info("Successfully triggered shard retry for {} recovered indices", recoveredCount);
                return true;
            },
                3,  // max attempts
                1000,  // initial delay ms
                "shard retry"
            );
        });
    }

    /**
     * Retries an operation with exponential backoff.
     * 
     * @param operation the operation to retry
     * @param maxAttempts maximum number of retry attempts
     * @param initialDelayMs initial delay in milliseconds
     * @param operationName name for logging
     * @return true if operation succeeded, false otherwise
     */
    private boolean retryWithBackoff(
        java.util.function.Supplier<Boolean> operation,
        int maxAttempts,
        long initialDelayMs,
        String operationName
    ) {
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return operation.get();
            } catch (Exception e) {
                if (attempt == maxAttempts) {
                    logger.warn("Failed {} after {} attempts: {}", operationName, maxAttempts, e.getMessage());
                    return false;
                }

                // Exponential backoff: 1s, 2s, 4s, ...
                long delay = initialDelayMs * (1L << (attempt - 1));

                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return false;
    }

    /**
     * Proactive health check that validates encryption keys for ALL encrypted indices on this node.
     * This runs continuously every hour regardless of failure state (true proactive monitoring).
     * 
     * <p>For each encrypted index on this node:
     * <ol>
     *   <li>Attempts to load the encryption key</li>
     *   <li>If successful and blocks were applied: removes blocks and clears failure state</li>
     *   <li>If successful and no blocks: validates key is still accessible (early detection)</li>
     *   <li>If failed: tracks failure and applies blocks on first failure</li>
     * </ol>
     * 
     * <p>Benefits:
     * <ul>
     *   <li>Early detection: Identifies MasterKey Provider issues before they cause shard failures</li>
     *   <li>Automatic recovery: Removes blocks when MasterKey Provider is restored</li>
     *   <li>Comprehensive: Checks ALL encrypted indices, not just failed ones</li>
     * </ul>
     * 
     * This runs on a single thread and checks all encrypted indices with shards on this node.
     */
    private void checkKmsHealthAndRecover() {
        try {
            // Get ALL encrypted indices on this node (not just failed ones)
            Set<String> allIndexUuids = ShardKeyResolverRegistry.getAllIndexUuids();

            if (allIndexUuids.isEmpty()) {
                return;
            }

            int recoveredCount = 0;
            int healthyCount = 0;
            int failedCount = 0;

            // Check each index individually (each has its own key!)
            for (String indexUuid : allIndexUuids) {
                try {
                    // Get any resolver for THIS specific index (all shards share the same master key)
                    KeyResolver resolver = ShardKeyResolverRegistry.getAnyResolverForIndex(indexUuid);
                    if (resolver == null) {
                        // Index deleted or no shards on this node, clean up
                        failureTracker.remove(indexUuid);
                        continue;
                    }

                    // Get index name from resolver
                    String indexName = ((DefaultKeyResolver) resolver).getIndexName();
                    if (indexName == null) {
                        continue;
                    }

                    // Try to load THIS index's specific key (validates MasterKey Provider connectivity)
                    Key key = ((DefaultKeyResolver) resolver).loadKeyFromMasterKeyProvider();

                    // Success! Remove blocks if they exist
                    if (hasBlocks(indexName)) {
                        removeBlocks(indexName);
                        recoveredCount++;
                    } else {
                        healthyCount++;
                    }

                    // Clean up failure tracker if index was being tracked
                    failureTracker.remove(indexUuid);

                } catch (Exception e) {
                    // Key load failed for THIS index
                    failedCount++;

                    FailureState state = failureTracker.get(indexUuid);
                    if (state == null) {
                        // First failure detected by health check (early detection!)
                        state = new FailureState(e);
                        failureTracker.put(indexUuid, state);

                        // Get index name for logging
                        KeyResolver resolver = ShardKeyResolverRegistry.getAnyResolverForIndex(indexUuid);
                        String indexName = resolver != null ? ((DefaultKeyResolver) resolver).getIndexName() : indexUuid;

                        // Apply blocks to protect data
                        if (indexName != null && !indexName.equals(indexUuid)) {
                            applyBlocks(indexName);
                            state.blocksApplied = true;
                        }
                    } else {
                        // Still failing, update failure time
                        state.recordFailure(e);
                    }
                }
            }

            // After recovering indices, trigger shard retry to recover RED indices
            if (recoveredCount > 0) {
                triggerShardRetry(recoveredCount);
            }

        } catch (Exception e) {
            logger.error("Error during Master Key Provider health check", e);
            // Keep monitoring thread running even on error
        }
    }

    /**
     * Shuts down background tasks without resetting the singleton instance.
     * Used during plugin lifecycle cleanup to prevent thread leaks.
     */
    public static synchronized void shutdown() {
        if (INSTANCE != null) {
            // Shutdown health check executor and cancel scheduled task
            if (INSTANCE.healthCheckTask != null) {
                INSTANCE.healthCheckTask.cancel(true);
            }
            if (INSTANCE.healthCheckExecutor != null) {
                INSTANCE.healthCheckExecutor.shutdownNow();
                try {
                    if (!INSTANCE.healthCheckExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                        logger.warn("Health check executor did not terminate in time");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Interrupted while waiting for health check executor to terminate");
                }
            }
        }
    }

    /**
     * Resets the singleton instance completely.
     * This method is primarily for testing purposes.
     */
    public static synchronized void reset() {
        if (INSTANCE != null) {
            INSTANCE.failureTracker.clear();
            shutdown();
            INSTANCE = null;
        }
    }
}

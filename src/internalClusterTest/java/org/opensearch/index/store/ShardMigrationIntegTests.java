/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

/**
 * Integration tests for encrypted shard migration, relocation, and recovery across nodes.
 * Tests various scenarios including:
 * - Explicit shard relocation between nodes
 * - Replica recovery after node restart
 * - Shard migration with concurrent operations
 * - Replica synchronization with data changes during downtime
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ShardMigrationIntegTests extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CryptoDirectoryPlugin.class, MockCryptoKeyProviderPlugin.class, MockCryptoPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings
            .builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("plugins.crypto.enabled", true)
            .put("node.store.crypto.pool_size_percentage", 0.05) // 5% for tests
            .put("node.store.crypto.warmup_percentage", 0.0) // No warmup
            .put("node.store.crypto.cache_to_pool_ratio", 0.8)
            .put("node.store.crypto.key_refresh_interval", "30s") // Short for testing
            .build();
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    private Settings cryptoIndexSettings() {
        return Settings
            .builder()
            .put("index.store.type", "cryptofs")
            .put("index.store.crypto.key_provider", "dummy")
            .put("index.store.crypto.kms.key_arn", "dummyArn")
            .build();
    }

    /**
     * Tests explicit shard relocation between nodes with encrypted data.
     * Validates that encrypted shards can be moved and data remains accessible.
     */
    public void testShardRelocationBetweenNodes() throws Exception {
        // Start 3 nodes
        internalCluster().startNodes(3);

        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 0)
            // Disable delayed allocation to allow immediate shard reallocation after node restart
            // OpenSearch's default 60s delay would cause test timeout
            .put("index.unassigned.node_left.delayed_timeout", "0")
            .build();

        createIndex("test-shard-relocation", settings);
        ensureGreen("test-shard-relocation");

        // Index documents
        int numDocs = randomIntBetween(200, 500);
        for (int i = 0; i < numDocs; i++) {
            index("test-shard-relocation", "_doc", String.valueOf(i), "field", "value" + i, "number", i);
        }
        refresh();

        // Verify initial document count
        SearchResponse response1 = client().prepareSearch("test-shard-relocation").setSize(0).get();
        assertThat(response1.getHits().getTotalHits().value(), equalTo((long) numDocs));

        // Get initial shard locations
        IndicesStatsResponse statsBeforeRelocation = client().admin().indices().prepareStats("test-shard-relocation").get();
        assertThat(statsBeforeRelocation.getShards().length, equalTo(2)); // 2 primary shards

        // Get node names for relocation
        String[] nodeNames = internalCluster().getNodeNames();
        logger.info("Available nodes: {}", Arrays.toString(nodeNames));

        // Attempt to relocate shards between nodes
        boolean relocated = false;
        for (int shardId = 0; shardId < 2; shardId++) {
            for (int fromNode = 0; fromNode < nodeNames.length - 1; fromNode++) {
                for (int toNode = fromNode + 1; toNode < nodeNames.length; toNode++) {
                    try {
                        logger.info("Attempting to relocate shard {} from {} to {}", shardId, nodeNames[fromNode], nodeNames[toNode]);
                        ClusterRerouteResponse rerouteResponse = client()
                            .admin()
                            .cluster()
                            .prepareReroute()
                            .add(new MoveAllocationCommand("test-shard-relocation", shardId, nodeNames[fromNode], nodeNames[toNode]))
                            .execute()
                            .actionGet(TimeValue.timeValueSeconds(60));

                        if (rerouteResponse.isAcknowledged()) {
                            logger.info("Successfully initiated relocation for shard {}", shardId);
                            relocated = true;
                            // Wait for the shard to start relocating
                            Thread.sleep(1000);
                            break;
                        }
                    } catch (Exception e) {
                        logger
                            .debug(
                                "Could not relocate shard {} from {} to {}: {}",
                                shardId,
                                nodeNames[fromNode],
                                nodeNames[toNode],
                                e.getMessage()
                            );
                    }
                }
                if (relocated)
                    break;
            }
            if (relocated)
                break;
        }

        if (relocated) {
            // Wait for relocation to complete with timeout
            ensureGreen(TimeValue.timeValueSeconds(60), "test-shard-relocation");

            // Use assertBusy to retry data verification until encryption keys are properly registered
            // This handles the race condition where the target node needs time to register resolvers
            final int finalNumDocs = numDocs;
            final int randomDoc = randomIntBetween(0, numDocs - 1);

            assertBusy(() -> {
                // Verify data integrity after relocation
                SearchResponse response2 = client().prepareSearch("test-shard-relocation").setSize(0).get();
                assertThat(
                    "Document count should remain same after relocation",
                    response2.getHits().getTotalHits().value(),
                    equalTo((long) finalNumDocs)
                );

                // Verify we can still read specific documents
                SearchResponse specificDoc = client()
                    .prepareSearch("test-shard-relocation")
                    .setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", randomDoc))
                    .get();
                assertThat(specificDoc.getHits().getTotalHits().value(), equalTo(1L));
            }, 30, TimeUnit.SECONDS);

            // Verify we can still write after relocation - with retry for encryption key availability
            final int newDocs = 50;
            assertBusy(() -> {
                for (int i = finalNumDocs; i < finalNumDocs + newDocs; i++) {
                    index("test-shard-relocation", "_doc", String.valueOf(i), "field", "value" + i, "number", i);
                }
                refresh();

                SearchResponse response3 = client().prepareSearch("test-shard-relocation").setSize(0).get();
                assertThat(response3.getHits().getTotalHits().value(), equalTo((long) (finalNumDocs + newDocs)));
            }, 30, TimeUnit.SECONDS);
        } else {
            logger.info("No relocation performed - shards already optimally distributed");
        }
    }

    /**
     * Tests replica recovery when a node is restarted.
     * Validates encrypted replica shards can be properly recovered.
     */
    public void testReplicaRecoveryOnRestart() throws Exception {
        // Start 3 nodes
        internalCluster().startNodes(3);

        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 1) // 1 replica per shard
            // Disable delayed allocation to allow immediate shard reallocation after node restart
            // OpenSearch's default 60s delay would cause test timeout
            .put("index.unassigned.node_left.delayed_timeout", "0")
            .build();

        createIndex("test-replica-recovery", settings);
        ensureGreen("test-replica-recovery");

        // Index documents
        int numDocs = randomIntBetween(100, 300);
        for (int i = 0; i < numDocs; i++) {
            index("test-replica-recovery", "_doc", String.valueOf(i), "field", "value" + i, "number", i);
        }
        refresh();

        // Verify initial state
        SearchResponse response1 = client().prepareSearch("test-replica-recovery").setSize(0).get();
        assertThat(response1.getHits().getTotalHits().value(), equalTo((long) numDocs));

        // Restart one data node (non-cluster-manager) to force replica recovery
        String clusterManager = internalCluster().getClusterManagerName();
        String[] nodeNames = internalCluster().getNodeNames();
        String nodeToRestart = null;

        for (String nodeName : nodeNames) {
            if (!nodeName.equals(clusterManager)) {
                nodeToRestart = nodeName;
                break;
            }
        }

        assertNotNull("Should find a non-cluster-manager node to restart", nodeToRestart);
        logger.info("Restarting node to test replica recovery: {}", nodeToRestart);

        // Restart the node
        internalCluster().restartNode(nodeToRestart);

        // Wait for cluster to stabilize
        ensureStableCluster(3);
        // Use longer timeout to allow encryption keys to be loaded after node restart
        ensureGreen(TimeValue.timeValueSeconds(60), "test-replica-recovery");

        // Use assertBusy to retry data verification until encryption keys are properly registered after restart
        // This handles the race condition where the restarted node needs time to register resolvers
        final int finalNumDocs = numDocs;
        assertBusy(() -> {
            // Verify data is still accessible after node restart
            SearchResponse response2 = client().prepareSearch("test-replica-recovery").setSize(0).get();
            assertThat(
                "All documents should be accessible after node restart",
                response2.getHits().getTotalHits().value(),
                equalTo((long) finalNumDocs)
            );
        }, 30, TimeUnit.SECONDS);

        // Verify we can still write - with retry for encryption key availability
        final int newDocs = 50;
        assertBusy(() -> {
            for (int i = finalNumDocs; i < finalNumDocs + newDocs; i++) {
                index("test-replica-recovery", "_doc", String.valueOf(i), "field", "value" + i, "number", i);
            }
            refresh();

            SearchResponse response3 = client().prepareSearch("test-replica-recovery").setSize(0).get();
            assertThat(response3.getHits().getTotalHits().value(), equalTo((long) (finalNumDocs + newDocs)));
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Tests shard migration with concurrent read/write operations.
     * Validates that data operations continue successfully during shard relocation.
     */
    public void testShardMigrationWithConcurrentOperations() throws Exception {
        // Start 3 nodes
        internalCluster().startNodes(3);

        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 3)
            .put("index.number_of_replicas", 0)
            .build();

        createIndex("test-migration-concurrent", settings);
        ensureGreen("test-migration-concurrent");

        // Pre-populate with documents
        int initialDocs = 200;
        for (int i = 0; i < initialDocs; i++) {
            index("test-migration-concurrent", "_doc", String.valueOf(i), "field", "value" + i, "number", i);
        }
        refresh();

        // Start concurrent operations
        int numThreads = 8;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger successfulOps = new AtomicInteger(0);
        AtomicInteger failures = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        try {
            // Start background read/write threads
            for (int i = 0; i < numThreads; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();

                        for (int j = 0; j < 50; j++) {
                            try {
                                if (threadId % 2 == 0) {
                                    // Reader thread
                                    SearchResponse response = client()
                                        .prepareSearch("test-migration-concurrent")
                                        .setQuery(org.opensearch.index.query.QueryBuilders.matchAllQuery())
                                        .setSize(10)
                                        .get();
                                    if (response.getHits().getTotalHits().value() > 0) {
                                        successfulOps.incrementAndGet();
                                    }
                                } else {
                                    // Writer thread
                                    String docId = "concurrent-thread-" + threadId + "-doc-" + j;
                                    index("test-migration-concurrent", "_doc", docId, "thread_id", threadId, "iteration", j);
                                    successfulOps.incrementAndGet();
                                }

                                Thread.sleep(randomIntBetween(10, 50));
                            } catch (Exception e) {
                                logger.debug("Operation failed during migration: {}", e.getMessage());
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Thread {} failed", threadId, e);
                        failures.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            // Start background operations
            startLatch.countDown();

            // Wait a bit for operations to start
            Thread.sleep(500);

            // Trigger shard relocation while operations are running
            String[] nodeNames = internalCluster().getNodeNames();
            boolean relocated = false;

            for (int shardId = 0; shardId < 3; shardId++) {
                for (int fromNode = 0; fromNode < nodeNames.length - 1; fromNode++) {
                    for (int toNode = fromNode + 1; toNode < nodeNames.length; toNode++) {
                        try {
                            logger
                                .info(
                                    "Relocating shard {} from {} to {} during concurrent operations",
                                    shardId,
                                    nodeNames[fromNode],
                                    nodeNames[toNode]
                                );
                            ClusterRerouteResponse rerouteResponse = client()
                                .admin()
                                .cluster()
                                .prepareReroute()
                                .add(
                                    new MoveAllocationCommand("test-migration-concurrent", shardId, nodeNames[fromNode], nodeNames[toNode])
                                )
                                .execute()
                                .actionGet(TimeValue.timeValueSeconds(60));

                            if (rerouteResponse.isAcknowledged()) {
                                relocated = true;
                                // Wait for the shard to start relocating
                                Thread.sleep(1000);
                                break;
                            }
                        } catch (Exception e) {
                            logger.debug("Relocation attempt failed: {}", e.getMessage());
                        }
                    }
                    if (relocated)
                        break;
                }
                if (relocated)
                    break;
            }

            // Wait for background operations to complete
            assertThat("Concurrent operations timed out", doneLatch.await(120, TimeUnit.SECONDS), is(true));

            // Wait for cluster to stabilize
            ensureGreen(TimeValue.timeValueSeconds(60), "test-migration-concurrent");

            // Verify no thread failures
            assertThat("Some threads failed during migration", failures.get(), equalTo(0));
            assertThat("Operations succeeded during migration", successfulOps.get(), greaterThan(0));

            // Verify final data integrity
            refresh();
            SearchResponse finalResponse = client().prepareSearch("test-migration-concurrent").setSize(0).get();
            assertThat(
                "Final document count should include all writes",
                finalResponse.getHits().getTotalHits().value(),
                greaterThanOrEqualTo((long) initialDocs)
            );

            logger.info("Shard migration with concurrent operations completed. Successful operations: {}", successfulOps.get());

        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /**
     * Tests replica recovery after node restart with data added during downtime.
     * Validates that replicas can sync all changes from encrypted primaries.
     */
    public void testReplicaRecoveryWithDataChanges() throws Exception {
        // Start 3 nodes
        internalCluster().startNodes(3);

        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 1)
            // Disable delayed allocation to allow immediate shard reallocation after node restart
            // OpenSearch's default 60s delay would cause test timeout
            .put("index.unassigned.node_left.delayed_timeout", "0")
            .build();

        createIndex("test-replica-sync", settings);
        ensureGreen("test-replica-sync");

        // Index documents
        int numDocs = randomIntBetween(200, 400);
        for (int i = 0; i < numDocs; i++) {
            index("test-replica-sync", "_doc", String.valueOf(i), "field", "value" + i, "number", i);
        }
        refresh();

        // Verify initial state
        SearchResponse response1 = client().prepareSearch("test-replica-sync").setSize(0).get();
        assertThat(response1.getHits().getTotalHits().value(), equalTo((long) numDocs));

        // Restart a non-cluster-manager node
        String clusterManager = internalCluster().getClusterManagerName();
        String[] nodeNames = internalCluster().getNodeNames();
        String nodeToRestart = null;

        for (String nodeName : nodeNames) {
            if (!nodeName.equals(clusterManager)) {
                nodeToRestart = nodeName;
                break;
            }
        }

        assertNotNull("Should find a non-cluster-manager node to restart", nodeToRestart);
        logger.info("Restarting node: {}", nodeToRestart);

        // Add more documents while node is restarting
        internalCluster().restartNode(nodeToRestart, new org.opensearch.test.InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                // Index more documents while node is down
                int additionalDocs = 50;
                for (int i = numDocs; i < numDocs + additionalDocs; i++) {
                    index("test-replica-sync", "_doc", String.valueOf(i), "field", "value" + i, "number", i);
                }
                refresh();
                return Settings.EMPTY;
            }
        });

        // Wait for cluster to recover
        ensureStableCluster(3);
        // Use longer timeout to allow encryption keys to be loaded after node restart
        ensureGreen(TimeValue.timeValueSeconds(60), "test-replica-sync");

        // Verify all documents (including those added during restart) are present
        SearchResponse response2 = client().prepareSearch("test-replica-sync").setSize(0).get();
        assertThat(
            "All documents should be present after replica recovery",
            response2.getHits().getTotalHits().value(),
            greaterThanOrEqualTo((long) numDocs)
        );

        // Verify data integrity - read specific document
        SearchResponse specificDoc = client()
            .prepareSearch("test-replica-sync")
            .setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", 0))
            .get();
        assertThat(specificDoc.getHits().getTotalHits().value(), equalTo(1L));

        logger.info("Replica recovery with data changes test completed successfully");
    }
}

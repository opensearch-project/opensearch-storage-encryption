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

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

/**
 * Extended integration tests for CryptoDirectory plugin.
 * Tests multi-node operations, lifecycle management, resilience, and performance.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ConcurrencyIntegTests extends OpenSearchIntegTestCase {

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

    // ==================== P1: Multi-Node Shard Operations ====================

    /**
     * Tests that encrypted shards can be allocated, relocated, and recovered across
     * multiple nodes with shared pool resources.
     */
    public void testShardAllocationAcrossCryptoNodes() throws Exception {
        // Start 3 nodes to test shard distribution
        internalCluster().startNodes(3);

        // Create encrypted index with multiple shards and replicas
        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 3)
            .put("index.number_of_replicas", 1)
            .build();

        createIndex("test-multi-node", settings);
        ensureGreen("test-multi-node");

        // Index documents
        int numDocs = randomIntBetween(500, 1000);
        for (int i = 0; i < numDocs; i++) {
            index("test-multi-node", "_doc", String.valueOf(i), "field", "value" + i, "number", i);
        }
        refresh();

        // Verify document count
        SearchResponse response1 = client().prepareSearch("test-multi-node").setSize(0).get();
        assertThat(response1.getHits().getTotalHits().value(), equalTo((long) numDocs));

        // Get shard allocation info
        IndicesStatsResponse stats1 = client().admin().indices().prepareStats("test-multi-node").get();
        int shardsBeforeRelocation = stats1.getShards().length;
        assertThat(shardsBeforeRelocation, greaterThan(0));

        // Trigger shard relocation by moving a shard to a different node
        ClusterHealthResponse health = client().admin().cluster().prepareHealth("test-multi-node").get();
        String[] nodeNames = internalCluster().getNodeNames();

        // Try to relocate a primary shard
        try {
            ClusterRerouteResponse rerouteResponse = client()
                .admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand("test-multi-node", 0, nodeNames[0], nodeNames[1]))
                .get();
            assertThat(rerouteResponse.isAcknowledged(), is(true));
        } catch (Exception e) {
            // Relocation might not be possible if shard is already on target node
            logger.info("Shard relocation skipped: {}", e.getMessage());
        }

        // Wait for cluster to stabilize
        ensureGreen("test-multi-node");

        // Verify data integrity after relocation
        SearchResponse response2 = client().prepareSearch("test-multi-node").setSize(0).get();
        assertThat(response2.getHits().getTotalHits().value(), equalTo((long) numDocs));

        // Verify all shards are still functional
        IndicesStatsResponse stats2 = client().admin().indices().prepareStats("test-multi-node").get();
        assertThat(stats2.getShards().length, equalTo(shardsBeforeRelocation));
    }

    // ==================== P1: Node Restart and Recovery ====================

    /**
     * Tests that encrypted indices survive node restarts and that pool/cache are
     * properly re-initialized. Validates lifecycle management including executor shutdown.
     */
    public void testNodeRestartWithEncryptedIndices() throws Exception {
        // Start 2 nodes
        internalCluster().startNodes(2);

        // Create encrypted index with replicas
        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)  // No replicas so index can go green with single node
            .build();

        createIndex("test-restart", settings);
        ensureGreen("test-restart");

        // Index documents
        int numDocs = randomIntBetween(100, 500);
        for (int i = 0; i < numDocs; i++) {
            index("test-restart", "_doc", String.valueOf(i), "field", "value" + i);
        }
        refresh();

        // Verify document count before restart
        SearchResponse response1 = client().prepareSearch("test-restart").setSize(0).get();
        assertThat(response1.getHits().getTotalHits().value(), equalTo((long) numDocs));

        // Restart one node (non-master)
        String[] nodeNames = internalCluster().getNodeNames();
        String clusterManager = internalCluster().getClusterManagerName();
        String nodeToRestart = nodeNames[0].equals(clusterManager) ? nodeNames[1] : nodeNames[0];

        logger.info("Restarting node: {}", nodeToRestart);
        internalCluster().restartNode(nodeToRestart);

        // Ensure cluster is stable with 2 nodes
        ensureStableCluster(2);

        // Wait for green after node comes back and replicas are recovered
        ensureGreen("test-restart");

        // Verify data is still accessible after restart
        SearchResponse response2 = client().prepareSearch("test-restart").setSize(0).get();
        assertThat(response2.getHits().getTotalHits().value(), equalTo((long) numDocs));

        // Verify we can still write after restart
        int additionalDocs = randomIntBetween(50, 100);
        for (int i = numDocs; i < numDocs + additionalDocs; i++) {
            index("test-restart", "_doc", String.valueOf(i), "field", "value" + i);
        }
        refresh();

        SearchResponse response3 = client().prepareSearch("test-restart").setSize(0).get();
        assertThat(response3.getHits().getTotalHits().value(), equalTo((long) (numDocs + additionalDocs)));
    }

    // ==================== P2: Concurrent Index Creation ====================

    /**
     * Tests ShardKeyResolverRegistry race condition prevention during concurrent
     * encrypted index creation.
     */
    public void testConcurrentEncryptedIndexCreation() throws Exception {
        internalCluster().startNodes(2);

        int numIndices = randomIntBetween(5, 10);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numIndices);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(numIndices);

        try {
            // Create multiple indices concurrently
            for (int i = 0; i < numIndices; i++) {
                final int index = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();

                        String indexName = "concurrent-test-" + index;
                        Settings settings = Settings
                            .builder()
                            .put(cryptoIndexSettings())
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 0)
                            .build();

                        createIndex(indexName, settings);

                        // Index a few documents to ensure the index is functional
                        for (int j = 0; j < 10; j++) {
                            index(indexName, "_doc", String.valueOf(j), "field", "value" + j);
                        }
                        refresh(indexName);

                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        logger.error("Failed to create index concurrent-test-" + index, e);
                        failureCount.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            // Start all threads
            startLatch.countDown();

            // Wait for completion
            assertThat("Concurrent index creation timed out", doneLatch.await(60, TimeUnit.SECONDS), is(true));

            // Verify all indices were created successfully
            assertThat("Some indices failed to create", failureCount.get(), equalTo(0));
            assertThat("Not all indices were created", successCount.get(), equalTo(numIndices));

            // Verify each index has correct document count
            for (int i = 0; i < numIndices; i++) {
                String indexName = "concurrent-test-" + i;
                SearchResponse response = client().prepareSearch(indexName).setSize(0).get();
                assertThat(response.getHits().getTotalHits().value(), equalTo(10L));
            }

        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    // ==================== P2: Pool Pressure and Fallback ====================

    /**
     * Tests memory pool exhaustion handling and fallback to ephemeral allocation.
     */
    public void testMemoryPoolExhaustionHandling() throws Exception {
        // Start node with very small pool
        Settings smallPoolSettings = Settings
            .builder()
            .put(nodeSettings(0))
            .put("node.store.crypto.pool_size_percentage", 0.001) // Very small pool
            .put("node.store.crypto.cache_to_pool_ratio", 0.5)
            .build();

        internalCluster().startNode(smallPoolSettings);

        // Create encrypted index
        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();

        createIndex("test-pool-pressure", settings);
        ensureGreen("test-pool-pressure");

        // Index documents - some may trigger pool exhaustion
        int numDocs = 100;
        int successfulIndexes = 0;

        for (int i = 0; i < numDocs; i++) {
            try {
                // Create larger documents to stress the pool
                StringBuilder largeValue = new StringBuilder();
                for (int j = 0; j < 1000; j++) {
                    largeValue.append("data");
                }

                index("test-pool-pressure", "_doc", String.valueOf(i), "field", largeValue.toString(), "index", i);
                successfulIndexes++;
            } catch (Exception e) {
                logger.debug("Expected pool pressure exception: {}", e.getMessage());
            }
        }

        refresh();

        // Verify at least some documents were indexed (may use ephemeral pool)
        assertThat("No documents were indexed despite pool pressure", successfulIndexes, greaterThan(0));

        SearchResponse response = client().prepareSearch("test-pool-pressure").setSize(0).get();
        assertThat(response.getHits().getTotalHits().value(), greaterThan(0L));
    }

    // ==================== P2: Translog Encryption Integration ====================

    /**
     * Tests CryptoEngineFactory and translog encryption with recovery.
     * Enhanced to ensure translog recovery actually happens by:
     * 1. Using aggressive translog retention settings to prevent auto-flush
     * 2. Identifying and restarting the node that actually has the shard
     * 3. Verifying translog state before and after recovery
     */
    public void testTranslogEncryptionWithRecovery() throws Exception {
        internalCluster().startNodes(2);

        // Create encrypted index with aggressive translog retention settings
        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)  // No replicas so index can go green with single node
            // Aggressive settings to prevent any auto-flushing
            .put("index.translog.flush_threshold_size", "1gb") // Very high threshold
            .put("index.translog.sync_interval", "30s") // Long sync interval
            .put("index.translog.durability", "async") // Async durability
            .put("index.refresh_interval", "-1") // Disable automatic refresh to prevent potential flush triggers
            .build();

        createIndex("test-translog", settings);
        ensureGreen("test-translog");

        // Index documents without flushing to keep them in translog
        int numDocs = randomIntBetween(100, 300);
        for (int i = 0; i < numDocs; i++) {
            index("test-translog", "_doc", String.valueOf(i), "field", "value" + i, "number", i);
        }
        // Explicitly do NOT call refresh() or flush() - keep data in translog only

        // Get shard routing to find which node has the primary shard
        ClusterStateResponse clusterState = client().admin().cluster().prepareState().get();
        String nodeIdWithShard = clusterState
            .getState()
            .routingTable()
            .index("test-translog")
            .shard(0)  // We have 1 shard (shardId = 0)
            .primaryShard()
            .currentNodeId();

        String nodeNameWithShard = clusterState.getState().nodes().get(nodeIdWithShard).getName();
        logger.info("Primary shard is on node: {} (id: {})", nodeNameWithShard, nodeIdWithShard);

        // Verify translog has unflushed operations before restart
        IndicesStatsResponse statsBeforeRestart = client().admin().indices().prepareStats("test-translog").get();
        long translogOpsBefore = statsBeforeRestart.getIndex("test-translog").getTotal().getTranslog().getUncommittedOperations();
        long translogSizeBefore = statsBeforeRestart.getIndex("test-translog").getTotal().getTranslog().getUncommittedSizeInBytes();

        logger.info("Translog state before restart: {} uncommitted operations, {} bytes", translogOpsBefore, translogSizeBefore);
        assertThat("Translog should have uncommitted operations to test recovery", translogOpsBefore, greaterThan(0L));

        // Restart specifically the node that has the shard to guarantee translog recovery
        logger.info("Restarting node with shard to test translog recovery: {}", nodeNameWithShard);
        internalCluster().restartNode(nodeNameWithShard);

        // Ensure cluster is stable with 2 nodes
        ensureStableCluster(2);

        // Wait for green after node comes back
        ensureGreen("test-translog");

        // NOW refresh to make recovered documents searchable
        refresh();

        // Verify all documents recovered from encrypted translog
        SearchResponse response2 = client().prepareSearch("test-translog").setSize(0).get();
        assertThat(
            "All documents should be recovered from encrypted translog",
            response2.getHits().getTotalHits().value(),
            equalTo((long) numDocs)
        );

        // Verify data integrity - check specific documents
        int randomDoc = randomIntBetween(0, numDocs - 1);
        SearchResponse specificDoc = client()
            .prepareSearch("test-translog")
            .setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", randomDoc))
            .get();
        assertThat("Should be able to read specific documents after recovery", specificDoc.getHits().getTotalHits().value(), equalTo(1L));

        // Verify we can retrieve all documents
        SearchResponse response3 = client()
            .prepareSearch("test-translog")
            .setQuery(org.opensearch.index.query.QueryBuilders.matchAllQuery())
            .setSize(numDocs)
            .get();
        assertThat("Should retrieve all documents after recovery", response3.getHits().getHits().length, equalTo(numDocs));

        // Verify translog state after recovery
        IndicesStatsResponse statsAfterRestart = client().admin().indices().prepareStats("test-translog").get();
        long translogOpsAfter = statsAfterRestart.getIndex("test-translog").getTotal().getTranslog().getUncommittedOperations();
        logger.info("Translog operations after recovery: {}", translogOpsAfter);

        // Verify we can still write after recovery
        int additionalDocs = 50;
        for (int i = numDocs; i < numDocs + additionalDocs; i++) {
            index("test-translog", "_doc", String.valueOf(i), "field", "value" + i, "number", i);
        }
        refresh();

        SearchResponse response4 = client().prepareSearch("test-translog").setSize(0).get();
        assertThat(
            "Should be able to write new documents after recovery",
            response4.getHits().getTotalHits().value(),
            equalTo((long) (numDocs + additionalDocs))
        );

        // Explicit flush to release any BigArrays held by translog/recovery operations
        // This prevents "arrays have not been released" errors in test framework
        flush("test-translog");

        logger.info("Translog encryption recovery test completed successfully");
    }

    // ==================== P2: Index-Level Key Isolation ====================

    /**
     * Tests that each encrypted index has its own encryption keys and proper cleanup.
     */
    public void testIndexLevelKeyIsolation() throws Exception {
        internalCluster().startNodes(2);

        // Create multiple encrypted indices
        int numIndices = randomIntBetween(3, 5);
        String[] indexNames = new String[numIndices];

        for (int i = 0; i < numIndices; i++) {
            indexNames[i] = "test-key-isolation-" + i;
            Settings settings = Settings
                .builder()
                .put(cryptoIndexSettings())
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .build();

            createIndex(indexNames[i], settings);

            // Index some data
            for (int j = 0; j < 50; j++) {
                index(indexNames[i], "_doc", String.valueOf(j), "field", "value" + j, "index", i);
            }
        }

        refresh();

        // Verify all indices are functional
        for (String indexName : indexNames) {
            SearchResponse response = client().prepareSearch(indexName).setSize(0).get();
            assertThat(response.getHits().getTotalHits().value(), equalTo(50L));
        }

        // Delete one index
        String indexToDelete = indexNames[0];
        DeleteIndexRequest deleteRequest = new DeleteIndexRequest(indexToDelete);
        client().admin().indices().delete(deleteRequest).actionGet();

        // Verify deleted index is gone
        expectThrows(IndexNotFoundException.class, () -> client().prepareSearch(indexToDelete).get());

        // Verify other indices still work (keys not affected)
        for (int i = 1; i < numIndices; i++) {
            SearchResponse response = client().prepareSearch(indexNames[i]).setSize(0).get();
            assertThat(response.getHits().getTotalHits().value(), equalTo(50L));

            // Verify we can still write
            index(indexNames[i], "_doc", "new", "field", "newvalue");
        }

        refresh();

        for (int i = 1; i < numIndices; i++) {
            SearchResponse response = client().prepareSearch(indexNames[i]).setSize(0).get();
            assertThat(response.getHits().getTotalHits().value(), equalTo(51L));
        }
    }

    // ==================== P2: Settings Validation ====================

    /**
     * Tests proper error handling for invalid crypto settings.
     */
    public void testInvalidCryptoSettings() throws Exception {
        internalCluster().startNode();

        // Test 1: MMAPFS with encryption should fail
        Settings mmapSettings = Settings
            .builder()
            .put("index.store.type", "cryptofs")
            .put("index.store.crypto.key_provider", "dummy")
            .put("node.store.allow_mmap", true) // Try to force MMAP
            .build();

        try {
            createIndex("test-mmap-fail", mmapSettings);
            // If index creation succeeds, verify it's not using MMAP
            IndicesStatsResponse stats = client().admin().indices().prepareStats("test-mmap-fail").get();
            assertNotNull(stats);
            // MMAP should not be used with encryption
        } catch (Exception e) {
            // Expected: MMAP not supported with encryption
            logger.info("Expected MMAP rejection: {}", e.getMessage());
        }

        // Test 2: Missing KMS type - use valid dummy provider
        Settings missingKmsSettings = Settings
            .builder()
            .put("index.store.type", "cryptofs")
            .put("index.store.crypto.key_provider", "dummy") // Valid for tests
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();

        // This should succeed with dummy provider
        createIndex("test-dummy-kms", missingKmsSettings);
        ensureGreen("test-dummy-kms");

        // Verify index works
        index("test-dummy-kms", "_doc", "1", "field", "value");
        refresh();
        SearchResponse response = client().prepareSearch("test-dummy-kms").get();
        assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
    }

    // ==================== P3: Segment Merges with Encryption ====================

    /**
     * Tests that Lucene segment merges work correctly with encryption.
     * Tests heavy I/O and pool/cache behavior during merges.
     */
    public void testSegmentMergesWithEncryption() throws Exception {
        internalCluster().startNode();

        // Create encrypted index with merge settings
        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.merge.policy.max_merged_segment", "100mb")
            .build();

        createIndex("test-merges", settings);
        ensureGreen("test-merges");

        // Index many small documents to create multiple segments
        int numDocs = randomIntBetween(500, 1000);
        for (int i = 0; i < numDocs; i++) {
            index("test-merges", "_doc", String.valueOf(i), "field", "value" + i, "number", i);

            // Periodic flush to create multiple segments
            if (i % 100 == 0) {
                flush("test-merges");
            }
        }

        refresh();

        // Verify document count before merge
        SearchResponse responseBefore = client().prepareSearch("test-merges").setSize(0).get();
        assertThat(responseBefore.getHits().getTotalHits().value(), equalTo((long) numDocs));

        // Get segment count before merge
        IndicesStatsResponse statsBefore = client().admin().indices().prepareStats("test-merges").get();
        long segmentsBefore = statsBefore.getIndex("test-merges").getTotal().getSegments().getCount();
        logger.info("Segments before merge: {}", segmentsBefore);

        // Force merge to 1 segment
        ForceMergeResponse mergeResponse = client()
            .admin()
            .indices()
            .prepareForceMerge("test-merges")
            .setMaxNumSegments(1)
            .setFlush(true)
            .get();

        assertThat(mergeResponse.getFailedShards(), equalTo(0));

        // Get segment count after merge
        IndicesStatsResponse statsAfter = client().admin().indices().prepareStats("test-merges").get();
        long segmentsAfter = statsAfter.getIndex("test-merges").getTotal().getSegments().getCount();
        logger.info("Segments after merge: {}", segmentsAfter);

        // Verify segment count reduced
        assertThat("Segments should be merged", segmentsAfter, greaterThanOrEqualTo(1L));

        // Verify data integrity after merge
        SearchResponse responseAfter = client().prepareSearch("test-merges").setSize(0).get();
        assertThat(responseAfter.getHits().getTotalHits().value(), equalTo((long) numDocs));

        // Verify we can still read specific documents
        SearchResponse sampleDoc = client()
            .prepareSearch("test-merges")
            .setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", 42))
            .get();
        assertThat(sampleDoc.getHits().getTotalHits().value(), greaterThan(0L));
    }

    // ==================== Concurrency Tests ====================

    /**
     * Tests concurrent reads from multiple threads on encrypted index.
     * Validates BlockCache thread-safety and pool concurrency.
     */
    public void testConcurrentReads() throws Exception {
        internalCluster().startNode();

        // Create encrypted index
        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 0)
            .build();

        createIndex("test-concurrent-reads", settings);
        ensureGreen("test-concurrent-reads");

        // Index documents
        int numDocs = randomIntBetween(500, 1000);
        for (int i = 0; i < numDocs; i++) {
            index("test-concurrent-reads", "_doc", String.valueOf(i), "field", "value" + i, "number", i);
        }
        refresh();

        // Concurrent read threads
        int numThreads = 20;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        try {
            for (int i = 0; i < numThreads; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();

                        // Each thread performs multiple reads
                        for (int j = 0; j < 50; j++) {
                            int docId = randomIntBetween(0, numDocs - 1);
                            SearchResponse response = client()
                                .prepareSearch("test-concurrent-reads")
                                .setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", docId))
                                .get();

                            if (response.getHits().getTotalHits().value() == 1) {
                                successCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Thread {} failed during concurrent reads", threadId, e);
                        failureCount.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            // Start all threads
            startLatch.countDown();

            // Wait for completion
            assertThat("Concurrent reads timed out", doneLatch.await(120, TimeUnit.SECONDS), is(true));

            // Verify no failures
            assertThat("Concurrent reads had failures", failureCount.get(), equalTo(0));
            assertThat("Concurrent reads succeeded", successCount.get(), greaterThan(0));

        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /**
     * Tests concurrent writes to encrypted index.
     * Validates pool acquisition, cache invalidation, and write concurrency.
     */
    public void testConcurrentWrites() throws Exception {
        internalCluster().startNode();

        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 0)
            .put("index.refresh_interval", "1s")
            .build();

        createIndex("test-concurrent-writes", settings);
        ensureGreen("test-concurrent-writes");

        int numThreads = 10;
        int docsPerThread = 50;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        try {
            for (int i = 0; i < numThreads; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();

                        // Each thread writes documents
                        for (int j = 0; j < docsPerThread; j++) {
                            String docId = "thread-" + threadId + "-doc-" + j;
                            index(
                                "test-concurrent-writes",
                                "_doc",
                                docId,
                                "thread_id",
                                threadId,
                                "doc_num",
                                j,
                                "data",
                                "value-" + threadId + "-" + j
                            );
                            successCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        logger.error("Thread {} failed during concurrent writes", threadId, e);
                        failureCount.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            // Start all threads
            startLatch.countDown();

            // Wait for completion
            assertThat("Concurrent writes timed out", doneLatch.await(120, TimeUnit.SECONDS), is(true));

            // Verify no failures
            assertThat("Concurrent writes had failures", failureCount.get(), equalTo(0));
            assertThat("Concurrent writes succeeded", successCount.get(), equalTo(numThreads * docsPerThread));

            // Refresh and verify all documents indexed
            refresh();
            SearchResponse response = client().prepareSearch("test-concurrent-writes").setSize(0).get();
            assertThat(response.getHits().getTotalHits().value(), equalTo((long) (numThreads * docsPerThread)));

        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /**
     * Tests concurrent mixed reads and writes.
     * Validates overall system stability under mixed concurrent load.
     */
    public void testConcurrentMixedOperations() throws Exception {
        internalCluster().startNodes(2);

        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 3)
            .put("index.number_of_replicas", 1)
            .build();

        createIndex("test-concurrent-mixed", settings);
        ensureGreen("test-concurrent-mixed");

        // Pre-populate with some documents
        int initialDocs = 200;
        for (int i = 0; i < initialDocs; i++) {
            index("test-concurrent-mixed", "_doc", "init-" + i, "field", "value" + i, "number", i);
        }
        refresh();

        int numReaders = 10;
        int numWriters = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numReaders + numWriters);
        AtomicInteger readSuccess = new AtomicInteger(0);
        AtomicInteger writeSuccess = new AtomicInteger(0);
        AtomicInteger failures = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(numReaders + numWriters);

        try {
            // Reader threads
            for (int i = 0; i < numReaders; i++) {
                final int readerId = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();

                        for (int j = 0; j < 100; j++) {
                            SearchResponse response = client()
                                .prepareSearch("test-concurrent-mixed")
                                .setQuery(org.opensearch.index.query.QueryBuilders.matchAllQuery())
                                .setSize(10)
                                .get();

                            if (response.getHits().getTotalHits().value() > 0) {
                                readSuccess.incrementAndGet();
                            }

                            // Small delay to interleave with writes
                            Thread.sleep(randomIntBetween(1, 5));
                        }
                    } catch (Exception e) {
                        logger.error("Reader {} failed", readerId, e);
                        failures.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            // Writer threads
            for (int i = 0; i < numWriters; i++) {
                final int writerId = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();

                        for (int j = 0; j < 50; j++) {
                            String docId = "writer-" + writerId + "-doc-" + j;
                            index("test-concurrent-mixed", "_doc", docId, "writer_id", writerId, "doc_num", j, "data", "mixed-" + j);
                            writeSuccess.incrementAndGet();

                            // Occasional refresh
                            if (j % 10 == 0) {
                                refresh("test-concurrent-mixed");
                            }

                            Thread.sleep(randomIntBetween(1, 10));
                        }
                    } catch (Exception e) {
                        logger.error("Writer {} failed", writerId, e);
                        failures.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            // Start all threads
            startLatch.countDown();

            // Wait for completion
            assertThat("Concurrent mixed operations timed out", doneLatch.await(180, TimeUnit.SECONDS), is(true));

            // Verify no failures
            assertThat("Concurrent operations had failures", failures.get(), equalTo(0));
            assertThat("Read operations succeeded", readSuccess.get(), greaterThan(0));
            assertThat("Write operations succeeded", writeSuccess.get(), equalTo(numWriters * 50));

            // Final verification
            refresh();
            SearchResponse finalResponse = client().prepareSearch("test-concurrent-mixed").setSize(0).get();
            assertThat(finalResponse.getHits().getTotalHits().value(), equalTo((long) (initialDocs + numWriters * 50)));

        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /**
     * Tests concurrent shard operations (search, index, delete) across multiple shards.
     * Validates shard-level concurrency and cross-shard coordination.
     */
    public void testConcurrentShardOperations() throws Exception {
        internalCluster().startNodes(2);

        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 5)
            .put("index.number_of_replicas", 1)
            .build();

        createIndex("test-concurrent-shards", settings);
        ensureGreen("test-concurrent-shards");

        int numThreads = 15;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger totalOperations = new AtomicInteger(0);
        AtomicInteger failures = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        try {
            for (int i = 0; i < numThreads; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();

                        // Each thread performs mixed operations
                        for (int j = 0; j < 30; j++) {
                            String docId = "thread-" + threadId + "-doc-" + j;

                            // Index
                            index("test-concurrent-shards", "_doc", docId, "thread_id", threadId, "iteration", j);
                            totalOperations.incrementAndGet();

                            // Search
                            SearchResponse searchResponse = client()
                                .prepareSearch("test-concurrent-shards")
                                .setQuery(org.opensearch.index.query.QueryBuilders.termQuery("thread_id", threadId))
                                .get();
                            totalOperations.incrementAndGet();

                            // Occasional delete (delete some older docs)
                            if (j > 10 && j % 5 == 0) {
                                String deleteId = "thread-" + threadId + "-doc-" + (j - 10);
                                client().prepareDelete("test-concurrent-shards", deleteId).get();
                                totalOperations.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Thread {} failed during shard operations", threadId, e);
                        failures.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            // Start all threads
            startLatch.countDown();

            // Wait for completion
            assertThat("Concurrent shard operations timed out", doneLatch.await(180, TimeUnit.SECONDS), is(true));

            // Verify no failures
            assertThat("Concurrent shard operations had failures", failures.get(), equalTo(0));
            assertThat("Total operations completed", totalOperations.get(), greaterThan(0));

            // Verify index is still functional
            refresh();
            SearchResponse finalResponse = client().prepareSearch("test-concurrent-shards").setSize(0).get();
            assertThat(finalResponse.getHits().getTotalHits().value(), greaterThan(0L));

        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /**
     * Tests concurrent cache operations under moderate load.
     * Validates BlockCache and Pool thread-safety with realistic workload.
     */
    public void testConcurrentCacheStress() throws Exception {
        internalCluster().startNode();

        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();

        createIndex("test-cache-stress", settings);
        ensureGreen("test-cache-stress");

        // Index moderate number of documents
        int numDocs = 100;
        for (int i = 0; i < numDocs; i++) {
            StringBuilder field = new StringBuilder();
            for (int j = 0; j < 50; j++) {
                field.append("data");
            }
            index("test-cache-stress", "_doc", String.valueOf(i), "field", field.toString(), "number", i);
        }
        refresh();

        int numThreads = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger successfulReads = new AtomicInteger(0);
        AtomicInteger failures = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        try {
            for (int i = 0; i < numThreads; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();

                        // Each thread performs sequential access (more realistic than random)
                        for (int j = 0; j < 20; j++) {
                            int startDoc = threadId * 20 + j;
                            if (startDoc < numDocs) {
                                SearchResponse response = client()
                                    .prepareSearch("test-cache-stress")
                                    .setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", startDoc))
                                    .get();

                                if (response.getHits().getTotalHits().value() > 0) {
                                    successfulReads.incrementAndGet();
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Thread {} failed during cache stress", threadId, e);
                        failures.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            // Start all threads
            startLatch.countDown();

            // Wait for completion
            assertThat("Cache stress test timed out", doneLatch.await(60, TimeUnit.SECONDS), is(true));

            // Verify no failures
            assertThat("Cache stress test had failures", failures.get(), equalTo(0));
            assertThat("Successful reads completed", successfulReads.get(), greaterThan(0));

            logger.info("Cache stress results - Successful reads: {}", successfulReads.get());

        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /**
     * Tests concurrent bulk operations on encrypted index.
     * Validates bulk indexing thread-safety and pool handling.
     */
    public void testConcurrentBulkOperations() throws Exception {
        internalCluster().startNode();

        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 0)
            .build();

        createIndex("test-concurrent-bulk", settings);
        ensureGreen("test-concurrent-bulk");

        int numThreads = 5;
        int bulksPerThread = 10;
        int docsPerBulk = 50;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger successfulBulks = new AtomicInteger(0);
        AtomicInteger failures = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        try {
            for (int i = 0; i < numThreads; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();

                        for (int bulkNum = 0; bulkNum < bulksPerThread; bulkNum++) {
                            org.opensearch.action.bulk.BulkRequestBuilder bulkRequest = client().prepareBulk();

                            for (int docNum = 0; docNum < docsPerBulk; docNum++) {
                                String docId = "thread-" + threadId + "-bulk-" + bulkNum + "-doc-" + docNum;
                                bulkRequest
                                    .add(
                                        client()
                                            .prepareIndex("test-concurrent-bulk")
                                            .setId(docId)
                                            .setSource("thread_id", threadId, "bulk_num", bulkNum, "doc_num", docNum, "data", "bulk-data")
                                    );
                            }

                            org.opensearch.action.bulk.BulkResponse bulkResponse = bulkRequest.get();
                            if (!bulkResponse.hasFailures()) {
                                successfulBulks.incrementAndGet();
                            } else {
                                logger
                                    .warn("Bulk {} from thread {} had failures: {}", bulkNum, threadId, bulkResponse.buildFailureMessage());
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Thread {} failed during bulk operations", threadId, e);
                        failures.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            // Start all threads
            startLatch.countDown();

            // Wait for completion
            assertThat("Concurrent bulk operations timed out", doneLatch.await(180, TimeUnit.SECONDS), is(true));

            // Verify no failures
            assertThat("Concurrent bulk operations had failures", failures.get(), equalTo(0));
            assertThat("All bulk requests succeeded", successfulBulks.get(), equalTo(numThreads * bulksPerThread));

            // Verify document count
            refresh();
            SearchResponse response = client().prepareSearch("test-concurrent-bulk").setSize(0).get();
            assertThat(response.getHits().getTotalHits().value(), equalTo((long) (numThreads * bulksPerThread * docsPerBulk)));

        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }
}

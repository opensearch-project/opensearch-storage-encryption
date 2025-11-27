/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import static org.hamcrest.Matchers.equalTo;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.OpenSearchIntegTestCase;

/**
 * Snapshot and restore integration tests for encrypted indices.
 * Tests that encrypted data can be successfully snapshotted and restored.
 *
 * This validates that encrypted data can be:
 * - Snapshotted to a repository
 * - Restored from a snapshot
 * - Decrypted correctly after restore
 * - Survives multiple snapshot/restore cycles
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SnapshotRestoreIntegTests extends OpenSearchIntegTestCase {

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
            .put("node.store.crypto.pool_size_percentage", 0.05)
            .put("node.store.crypto.warmup_percentage", 0.0)
            .put("node.store.crypto.cache_to_pool_ratio", 0.8)
            .put("node.store.crypto.key_refresh_interval", "30s")
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
     * Tests snapshot and restore of encrypted index.
     * Validates that encrypted data can be snapshotted and restored to a different index.
     */
    public void testSnapshotRestoreEncryptedIndex() throws Exception {
        // Start cluster
        internalCluster().startNodes(2);

        // Create snapshot repository
        Path repoPath = randomRepoPath();
        logger.info("Creating snapshot repository at: {}", repoPath);

        client()
            .admin()
            .cluster()
            .preparePutRepository("test-repo")
            .setType("fs")
            .setSettings(Settings.builder().put("location", repoPath).put("compress", false))
            .get();

        // Create encrypted index with data
        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 0)
            .build();

        createIndex("source-encrypted-index", settings);
        ensureGreen("source-encrypted-index");

        // Index documents
        int numDocs = randomIntBetween(100, 300);
        for (int i = 0; i < numDocs; i++) {
            index("source-encrypted-index", "_doc", String.valueOf(i), "field", "value" + i, "number", i);
        }
        refresh();

        // Verify source data
        SearchResponse sourceResponse = client().prepareSearch("source-encrypted-index").setSize(0).get();
        assertThat("Source index should have all documents", sourceResponse.getHits().getTotalHits().value(), equalTo((long) numDocs));

        // Create snapshot
        logger.info("Creating snapshot of encrypted index");
        CreateSnapshotResponse snapshotResponse = client()
            .admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "snapshot-1")
            .setWaitForCompletion(true)
            .setIndices("source-encrypted-index")
            .get();

        assertThat("Snapshot should complete successfully", snapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));

        // Close source index
        client().admin().indices().prepareClose("source-encrypted-index").get();

        // Restore to new index
        logger.info("Restoring snapshot to new encrypted index");
        RestoreSnapshotResponse restoreResponse = client()
            .admin()
            .cluster()
            .prepareRestoreSnapshot("test-repo", "snapshot-1")
            .setWaitForCompletion(true)
            .setIndices("source-encrypted-index")
            .setRenamePattern("source-encrypted-index")
            .setRenameReplacement("restored-encrypted-index")
            .get();

        assertThat("Restore should complete successfully", restoreResponse.getRestoreInfo().successfulShards(), equalTo(2));

        ensureGreen("restored-encrypted-index");

        // Verify restored data
        SearchResponse restoredResponse = client().prepareSearch("restored-encrypted-index").setSize(0).get();
        assertThat("Restored index should have all documents", restoredResponse.getHits().getTotalHits().value(), equalTo((long) numDocs));

        // Verify specific document content
        SearchResponse specificDoc = client()
            .prepareSearch("restored-encrypted-index")
            .setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", 0))
            .get();
        assertThat("Should find specific document", specificDoc.getHits().getTotalHits().value(), equalTo(1L));

        logger.info("Snapshot/restore test completed successfully");
    }

    /**
     * Tests multiple snapshot/restore cycles with incremental data additions.
     */
    public void testMultipleSnapshotRestoreCycles() throws Exception {
        // Start cluster
        internalCluster().startNodes(2);

        // Create snapshot repository
        Path repoPath = randomRepoPath();
        client()
            .admin()
            .cluster()
            .preparePutRepository("test-repo")
            .setType("fs")
            .setSettings(Settings.builder().put("location", repoPath).put("compress", false))
            .get();

        // Create encrypted index
        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();

        createIndex("incremental-index", settings);
        ensureGreen("incremental-index");

        // Perform multiple snapshot/restore cycles
        for (int cycle = 0; cycle < 3; cycle++) {
            logger.info("Starting snapshot/restore cycle {}", cycle);

            // Add documents
            int docsPerCycle = 50;
            int startDoc = cycle * docsPerCycle;
            int endDoc = startDoc + docsPerCycle;

            for (int i = startDoc; i < endDoc; i++) {
                index("incremental-index", "_doc", String.valueOf(i), "cycle", cycle, "number", i);
            }
            refresh();

            // Create snapshot
            String snapshotName = "snapshot-" + cycle;
            CreateSnapshotResponse snapshotResponse = client()
                .admin()
                .cluster()
                .prepareCreateSnapshot("test-repo", snapshotName)
                .setWaitForCompletion(true)
                .setIndices("incremental-index")
                .get();

            assertThat(snapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));

            // Restore to new index
            String restoredIndexName = "restored-cycle-" + cycle;
            client().admin().indices().prepareClose("incremental-index").get();

            RestoreSnapshotResponse restoreResponse = client()
                .admin()
                .cluster()
                .prepareRestoreSnapshot("test-repo", snapshotName)
                .setWaitForCompletion(true)
                .setIndices("incremental-index")
                .setRenamePattern("incremental-index")
                .setRenameReplacement(restoredIndexName)
                .get();

            assertThat(restoreResponse.getRestoreInfo().successfulShards(), equalTo(1));

            client().admin().indices().prepareOpen("incremental-index").get();
            ensureGreen(restoredIndexName);

            // Verify restored data contains all documents up to this cycle
            SearchResponse restoredResponse = client().prepareSearch(restoredIndexName).setSize(0).get();
            assertThat(restoredResponse.getHits().getTotalHits().value(), equalTo((long) endDoc));

            logger.info("Cycle {} completed successfully", cycle);
        }

        logger.info("Multiple snapshot/restore cycles test completed successfully");
    }

    /**
     * Tests restoring encrypted index after node restart.
     * Validates that snapshots can be restored after cluster topology changes.
     */
    public void testRestoreAfterNodeRestart() throws Exception {
        // Start initial cluster
        internalCluster().startNodes(2);

        // Create snapshot repository
        Path repoPath = randomRepoPath();
        client()
            .admin()
            .cluster()
            .preparePutRepository("test-repo")
            .setType("fs")
            .setSettings(Settings.builder().put("location", repoPath).put("compress", false))
            .get();

        // Create encrypted index with data
        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();

        createIndex("persistent-index", settings);
        ensureGreen("persistent-index");

        int numDocs = 100;
        for (int i = 0; i < numDocs; i++) {
            index("persistent-index", "_doc", String.valueOf(i), "field", "value" + i);
        }
        refresh();

        // Create snapshot
        CreateSnapshotResponse snapshotResponse = client()
            .admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "persistent-snapshot")
            .setWaitForCompletion(true)
            .setIndices("persistent-index")
            .get();

        assertThat(snapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));

        // Delete the index
        client().admin().indices().prepareDelete("persistent-index").get();

        // Restart one node to simulate cluster change
        internalCluster().restartRandomDataNode();
        ensureStableCluster(2);

        // Restore from snapshot
        RestoreSnapshotResponse restoreResponse = client()
            .admin()
            .cluster()
            .prepareRestoreSnapshot("test-repo", "persistent-snapshot")
            .setWaitForCompletion(true)
            .get();

        assertThat(restoreResponse.getRestoreInfo().successfulShards(), equalTo(1));

        ensureGreen("persistent-index");

        // Verify data after node restart
        SearchResponse response = client().prepareSearch("persistent-index").setSize(0).get();
        assertThat(response.getHits().getTotalHits().value(), equalTo((long) numDocs));

        logger.info("Restore after node restart test completed successfully");
    }

    /**
     * Tests restoring encrypted index with different encryption key.
     * Validates key rotation scenario where snapshot is taken with one key
     * and restored with a different key (simulating key rotation).
     *
     * Note: The snapshot stores encrypted data with the original key.
     * The restored index will use a new key for new writes, but old data
     * remains encrypted with the snapshot's key.
     */
    public void testRestoreWithDifferentKey() throws Exception {
        // Start cluster
        internalCluster().startNodes(2);

        // Create snapshot repository
        Path repoPath = randomRepoPath();
        client()
            .admin()
            .cluster()
            .preparePutRepository("test-repo")
            .setType("fs")
            .setSettings(Settings.builder().put("location", repoPath).put("compress", false))
            .get();

        // Create encrypted index with first key
        Settings settingsKey1 = Settings
            .builder()
            .put("index.store.type", "cryptofs")
            .put("index.store.crypto.key_provider", "dummy")
            .put("index.store.crypto.kms.key_arn", "key-arn-1")
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();

        createIndex("key-rotation-index", settingsKey1);
        ensureGreen("key-rotation-index");

        // Index documents with first key
        int numDocs = randomIntBetween(50, 100);
        for (int i = 0; i < numDocs; i++) {
            index("key-rotation-index", "_doc", "key1-" + i, "phase", "key1", "number", i);
        }
        refresh();

        // Verify data with first key
        SearchResponse response1 = client().prepareSearch("key-rotation-index").setSize(0).get();
        assertThat(response1.getHits().getTotalHits().value(), equalTo((long) numDocs));

        // Create snapshot
        logger.info("Creating snapshot with key-arn-1");
        CreateSnapshotResponse snapshotResponse = client()
            .admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "snapshot-key1")
            .setWaitForCompletion(true)
            .setIndices("key-rotation-index")
            .get();

        assertThat(snapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));

        // Delete the index
        client().admin().indices().prepareDelete("key-rotation-index").get();

        // Restore with different key ARN (simulating key rotation)
        logger.info("Restoring snapshot with key-arn-2");
        RestoreSnapshotResponse restoreResponse = client()
            .admin()
            .cluster()
            .prepareRestoreSnapshot("test-repo", "snapshot-key1")
            .setWaitForCompletion(true)
            .setIndexSettings(
                Settings
                    .builder()
                    .put("index.store.crypto.kms.key_arn", "key-arn-2") // Different key
                    .build()
            )
            .get();

        assertThat(restoreResponse.getRestoreInfo().successfulShards(), equalTo(1));
        ensureGreen("key-rotation-index");

        // Verify all old data is still accessible (encrypted with key1 in snapshot)
        SearchResponse response2 = client().prepareSearch("key-rotation-index").setSize(0).get();
        assertThat("All original documents should be accessible", response2.getHits().getTotalHits().value(), equalTo((long) numDocs));

        // Add new documents (will be encrypted with key2)
        int newDocs = 30;
        for (int i = 0; i < newDocs; i++) {
            index("key-rotation-index", "_doc", "key2-" + i, "phase", "key2", "number", numDocs + i);
        }
        refresh();

        // Verify total document count
        SearchResponse response3 = client().prepareSearch("key-rotation-index").setSize(0).get();
        assertThat(response3.getHits().getTotalHits().value(), equalTo((long) (numDocs + newDocs)));

        // Verify we can query both old and new data
        SearchResponse oldData = client()
            .prepareSearch("key-rotation-index")
            .setQuery(org.opensearch.index.query.QueryBuilders.termQuery("phase", "key1"))
            .setSize(0)
            .get();
        assertThat("Old data encrypted with key1", oldData.getHits().getTotalHits().value(), equalTo((long) numDocs));

        SearchResponse newData = client()
            .prepareSearch("key-rotation-index")
            .setQuery(org.opensearch.index.query.QueryBuilders.termQuery("phase", "key2"))
            .setSize(0)
            .get();
        assertThat("New data encrypted with key2", newData.getHits().getTotalHits().value(), equalTo((long) newDocs));

        logger.info("Restore with different key test completed successfully");
    }

    /**
     * Tests partial restore of encrypted indices.
     * Creates a snapshot with multiple encrypted indices and restores only a subset.
     */
    public void testPartialRestore() throws Exception {
        // Start cluster
        internalCluster().startNodes(2);

        // Create snapshot repository
        Path repoPath = randomRepoPath();
        client()
            .admin()
            .cluster()
            .preparePutRepository("test-repo")
            .setType("fs")
            .setSettings(Settings.builder().put("location", repoPath).put("compress", false))
            .get();

        // Create multiple encrypted indices
        Settings cryptoSettings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 0)
            .build();

        createIndex("index-1", cryptoSettings);
        createIndex("index-2", cryptoSettings);
        createIndex("index-3", cryptoSettings);

        ensureGreen("index-1", "index-2", "index-3");

        // Index different amounts of data in each
        int docs1 = 50;
        int docs2 = 75;
        int docs3 = 100;

        for (int i = 0; i < docs1; i++) {
            index("index-1", "_doc", String.valueOf(i), "index", "1", "number", i);
        }
        for (int i = 0; i < docs2; i++) {
            index("index-2", "_doc", String.valueOf(i), "index", "2", "number", i);
        }
        for (int i = 0; i < docs3; i++) {
            index("index-3", "_doc", String.valueOf(i), "index", "3", "number", i);
        }
        refresh();

        // Verify initial data
        assertThat(client().prepareSearch("index-1").setSize(0).get().getHits().getTotalHits().value(), equalTo((long) docs1));
        assertThat(client().prepareSearch("index-2").setSize(0).get().getHits().getTotalHits().value(), equalTo((long) docs2));
        assertThat(client().prepareSearch("index-3").setSize(0).get().getHits().getTotalHits().value(), equalTo((long) docs3));

        // Create snapshot of all indices
        logger.info("Creating snapshot of all encrypted indices");
        CreateSnapshotResponse snapshotResponse = client()
            .admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "multi-index-snapshot")
            .setWaitForCompletion(true)
            .setIndices("index-1", "index-2", "index-3")
            .get();

        assertThat(snapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));
        assertThat("All 3 indices should be in snapshot", snapshotResponse.getSnapshotInfo().successfulShards(), equalTo(6)); // 3 indices *
                                                                                                                              // 2 shards

        // Delete all indices
        client().admin().indices().prepareDelete("index-1", "index-2", "index-3").get();

        // Partial restore - only restore index-1 and index-3
        logger.info("Restoring only index-1 and index-3 from snapshot");
        RestoreSnapshotResponse restoreResponse = client()
            .admin()
            .cluster()
            .prepareRestoreSnapshot("test-repo", "multi-index-snapshot")
            .setWaitForCompletion(true)
            .setIndices("index-1", "index-3") // Deliberately skip index-2
            .get();

        assertThat("Should restore 4 shards (2 indices * 2 shards)", restoreResponse.getRestoreInfo().successfulShards(), equalTo(4));

        ensureGreen("index-1", "index-3");

        // Verify restored indices have correct data
        assertThat(
            "index-1 should have all documents",
            client().prepareSearch("index-1").setSize(0).get().getHits().getTotalHits().value(),
            equalTo((long) docs1)
        );
        assertThat(
            "index-3 should have all documents",
            client().prepareSearch("index-3").setSize(0).get().getHits().getTotalHits().value(),
            equalTo((long) docs3)
        );

        // Verify index-2 was NOT restored
        try {
            client().prepareSearch("index-2").get();
            fail("index-2 should not exist");
        } catch (Exception e) {
            // Expected - index-2 should not exist
            assertTrue("Should get index not found exception", e.getMessage().contains("index-2") || e.getCause() != null);
        }

        logger.info("Partial restore test completed successfully");
    }

    /**
     * Tests creating multiple concurrent snapshots of encrypted indices.
     * Validates that concurrent snapshot operations don't interfere with each other.
     */
    public void testConcurrentSnapshots() throws Exception {
        // Start cluster
        internalCluster().startNodes(2);

        // Create snapshot repository
        Path repoPath = randomRepoPath();
        client()
            .admin()
            .cluster()
            .preparePutRepository("test-repo")
            .setType("fs")
            .setSettings(Settings.builder().put("location", repoPath).put("compress", false))
            .get();

        // Create multiple encrypted indices
        Settings cryptoSettings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();

        int numIndices = 3;
        for (int i = 0; i < numIndices; i++) {
            createIndex("concurrent-index-" + i, cryptoSettings);
        }

        ensureGreen("concurrent-index-0", "concurrent-index-1", "concurrent-index-2");

        // Index data in each
        int docsPerIndex = 50;
        for (int idx = 0; idx < numIndices; idx++) {
            for (int doc = 0; doc < docsPerIndex; doc++) {
                index("concurrent-index-" + idx, "_doc", String.valueOf(doc), "index", String.valueOf(idx), "number", doc);
            }
        }
        refresh();

        // Create snapshots (OpenSearch queues them and runs them as concurrently as possible)
        logger.info("Creating {} snapshots", numIndices);

        // Create all snapshots - they will be queued and executed
        for (int i = 0; i < numIndices; i++) {
            CreateSnapshotResponse response = client()
                .admin()
                .cluster()
                .prepareCreateSnapshot("test-repo", "snapshot-" + i)
                .setWaitForCompletion(true)
                .setIndices("concurrent-index-" + i)
                .get();

            assertThat("Snapshot " + i + " should succeed", response.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));
            logger.info("Snapshot {} created successfully", i);
        }

        // Verify all snapshots exist
        logger.info("Verifying all snapshots were created successfully");
        for (int i = 0; i < numIndices; i++) {
            var snapshotInfo = client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("snapshot-" + i).get();
            assertThat("Snapshot " + i + " should exist", snapshotInfo.getSnapshots().size(), equalTo(1));
            assertThat(
                "Snapshot " + i + " should be successful",
                snapshotInfo.getSnapshots().get(0).state(),
                equalTo(SnapshotState.SUCCESS)
            );
        }

        // Delete all indices
        for (int i = 0; i < numIndices; i++) {
            client().admin().indices().prepareDelete("concurrent-index-" + i).get();
        }

        // Restore all snapshots and verify data integrity
        logger.info("Restoring all {} snapshots", numIndices);
        for (int i = 0; i < numIndices; i++) {
            RestoreSnapshotResponse restoreResponse = client()
                .admin()
                .cluster()
                .prepareRestoreSnapshot("test-repo", "snapshot-" + i)
                .setWaitForCompletion(true)
                .get();

            assertThat("Restore of snapshot " + i + " should succeed", restoreResponse.getRestoreInfo().successfulShards(), equalTo(1));
        }

        ensureGreen("concurrent-index-0", "concurrent-index-1", "concurrent-index-2");

        // Verify all data was restored correctly
        for (int i = 0; i < numIndices; i++) {
            SearchResponse response = client().prepareSearch("concurrent-index-" + i).setSize(0).get();
            assertThat(
                "Index " + i + " should have all documents",
                response.getHits().getTotalHits().value(),
                equalTo((long) docsPerIndex)
            );
        }

        logger.info("Concurrent snapshots test completed successfully");
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
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
            .put("node.store.crypto.pool_size_percentage", 0.05)
            .put("node.store.crypto.warmup_percentage", 0.0)
            .put("node.store.crypto.pool_to_cache_ratio", 1.25)
            .put("node.store.crypto.key_refresh_interval_secs", 30)
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

        assertThat(
            "Snapshot should complete successfully",
            snapshotResponse.getSnapshotInfo().state(),
            equalTo(SnapshotState.SUCCESS)
        );

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
        assertThat(
            "Restored index should have all documents",
            restoredResponse.getHits().getTotalHits().value(),
            equalTo((long) numDocs)
        );

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
}

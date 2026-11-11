/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import static org.hamcrest.Matchers.instanceOf;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

import java.util.Arrays;
import java.util.Collection;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.engine.NRTReplicationEngine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

/**
 * Verifies that cryptofs indices using segment replication give replicas an
 * {@link NRTReplicationEngine} and the primary an {@link InternalEngine}.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
@ThreadLeakFilters(filters = CaffeineThreadLeakFilter.class)
public class SegmentReplicationIntegTests extends OpenSearchIntegTestCase {

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
            .build();
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testSegmentReplicationReplicaUsesNrtReplicationEngine() throws Exception {
        internalCluster().startNodes(2);
        String index = "segrep-encrypted";
        createIndex(
            index,
            Settings
                .builder()
                .put("index.store.type", "cryptofs")
                .put("index.store.crypto.key_provider", "dummy")
                .put("index.store.crypto.kms.key_arn", "dummyArn")
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build()
        );
        ensureGreen(index);

        int numDocs = randomIntBetween(10, 50);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex(index).setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        refresh(index);

        int primaries = 0;
        int replicas = 0;
        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            IndexService indexService = indicesService.indexService(resolveIndex(index));
            if (indexService == null) {
                continue;
            }
            for (IndexShard shard : indexService) {
                Engine engine = IndexShardTestCase.getEngine(shard);
                if (shard.routingEntry().primary()) {
                    assertThat(engine, instanceOf(InternalEngine.class));
                    primaries++;
                } else {
                    assertThat(engine, instanceOf(NRTReplicationEngine.class));
                    replicas++;
                }
            }
        }
        assertEquals(1, primaries);
        assertEquals(1, replicas);

        // Replica must serve the documents it received through segment replication
        assertBusy(() -> {
            for (String node : internalCluster().getNodeNames()) {
                assertHitCount(client(node).prepareSearch(index).setPreference("_only_local").setSize(0).get(), numDocs);
            }
        });
    }
}

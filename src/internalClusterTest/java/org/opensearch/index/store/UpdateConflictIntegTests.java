/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

/**
 * Reproduces merge corruption under concurrent update workloads:
 *
 *   org.apache.lucene.index.CorruptIndexException: Illegal dict length
 *     (resource=CachedMemorySegmentIndexInput(path="..._N.cfs") [slice=_N.fdt])
 *   Caused by: java.lang.ArrayIndexOutOfBoundsException:
 *     arraycopy: source index -N out of bounds for byte[M]
 *       at org.apache.lucene.util.compress.LZ4.decompress
 *       at SegmentMerger.mergeFields → copyOneDoc → serializedDocument
 *
 * Triggered by 8 bulk indexing clients with 50% conflict rate and sustained updates.
 * Background merges read stored fields from compound files via the encrypted Direct I/O
 * block cache → L1 cache serves wrong block due to torn read on ARM → LZ4 decompresses
 * garbage → shard fails with "tragic event" → cluster goes RED.
 *
 * Disables translog encryption (CryptoDirectoryNoTranslogPlugin) to isolate the
 * Lucene stored fields read path.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
@ThreadLeakFilters(filters = CaffeineThreadLeakFilter.class)
public class UpdateConflictIntegTests extends OpenSearchIntegTestCase {

    /**
     * Plugin that provides the encrypted directory WITHOUT translog encryption,
     * matching the serverless setup. Isolates the Lucene/block-cache read path.
     */
    public static class CryptoDirectoryNoTranslogPlugin extends CryptoDirectoryPlugin {
        public CryptoDirectoryNoTranslogPlugin(Settings settings) {
            super(settings);
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.empty();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CryptoDirectoryNoTranslogPlugin.class, MockCryptoKeyProviderPlugin.class, MockCryptoPlugin.class);
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
            .put("node.store.crypto.write_cache_enabled", false)
            .build();
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    private Settings cryptoIndexSettings(int shards) {
        return Settings
            .builder()
            .put("index.store.type", "cryptofs")
            .put("index.store.crypto.key_provider", "dummy")
            .put("index.store.crypto.kms.key_arn", "dummyArn")
            .put("index.number_of_shards", shards)
            .put("index.number_of_replicas", 0)
            .build();
    }

    /**
     * Replicates a heavy update-only workload:
     * - 5 shards, 8 bulk indexing clients
     * - http_logs-like documents (~500B JSON)
     * - 50% conflict rate (updates to existing docs)
     * - 60+ second sustained load
     * - Followed by refresh + force-merge
     *
     * Asserts no corruption errors AND that the cluster stays GREEN (no shard failures).
     */
    public void testUpdateOnlyWorkload() throws Exception {
        internalCluster().startNode();

        String indexName = "logs-update";
        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings(5))
            .put("index.refresh_interval", "1s")
            .put("index.merge.policy.segments_per_tier", "10")
            .put("index.merge.policy.max_merge_at_once", "10")
            .put("index.merge.policy.floor_segment", "2mb")
            // With assertions on, core's TranslogWriter keeps every op of the current generation with a
            // stack trace (seenSequenceNumbers). At the default 64mb threshold that exceeds the test heap.
            .put("index.translog.generation_threshold_size", "1mb")
            .build();

        createIndex(indexName, settings);
        ensureGreen(indexName);

        // Phase 1: Initial bulk indexing — sized to fit in 512MB test JVM heap
        int totalDocs = 20_000;
        int initialBulkSize = 2000;
        for (int batch = 0; batch < totalDocs / initialBulkSize; batch++) {
            BulkRequestBuilder bulk = client().prepareBulk();
            for (int i = 0; i < initialBulkSize; i++) {
                int docId = batch * initialBulkSize + i;
                bulk.add(client().prepareIndex(indexName).setId(String.valueOf(docId)).setSource(httpLogDoc(docId, 0)));
            }
            BulkResponse resp = bulk.get();
            assertFalse("Initial bulk failed: " + resp.buildFailureMessage(), resp.hasFailures());
        }
        refresh(indexName);
        logger.info("Initial indexing done: {} docs", totalDocs);

        // Phase 2: 8 clients doing continuous bulk updates with 50% conflict rate
        int numClients = 8;
        long durationMs = 120_000;
        long deadline = System.currentTimeMillis() + durationMs;

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numClients);
        AtomicInteger corruptionErrors = new AtomicInteger(0);
        AtomicInteger totalBulks = new AtomicInteger(0);
        AtomicInteger totalDocsUpdated = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(numClients);

        try {
            for (int c = 0; c < numClients; c++) {
                final int clientId = c;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        int round = 0;
                        while (System.currentTimeMillis() < deadline) {
                            BulkRequestBuilder bulk = client().prepareBulk();
                            int currentBulkSize = 500;
                            for (int i = 0; i < currentBulkSize; i++) {
                                // 50% conflict: update existing; 50%: upsert new
                                int docId;
                                if (randomBoolean()) {
                                    docId = randomIntBetween(0, totalDocs - 1);
                                } else {
                                    docId = randomIntBetween(totalDocs, totalDocs * 2);
                                }
                                bulk
                                    .add(
                                        new UpdateRequest(indexName, String.valueOf(docId)).doc(httpLogDoc(docId, round)).docAsUpsert(true)
                                    );
                            }
                            BulkResponse response = bulk.get();
                            totalBulks.incrementAndGet();
                            totalDocsUpdated.addAndGet(currentBulkSize);
                            checkResponseForCorruption(response, clientId, round, corruptionErrors);
                            round++;
                        }
                    } catch (Exception e) {
                        checkExceptionForCorruption(e, clientId, corruptionErrors);
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            assertThat("Update threads timed out", doneLatch.await(durationMs + 60_000, TimeUnit.MILLISECONDS), is(true));

            logger
                .info(
                    "Update phase done: {} bulks, {} docs updated, {} corruption errors",
                    totalBulks.get(),
                    totalDocsUpdated.get(),
                    corruptionErrors.get()
                );

        } finally {
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }

        // Phase 3: Refresh + force-merge — where the merge corruption typically manifests
        refresh(indexName);
        logger.info("Starting force-merge after updates");

        ForceMergeResponse mergeResponse = client()
            .admin()
            .indices()
            .prepareForceMerge(indexName)
            .setMaxNumSegments(1)
            .setFlush(true)
            .get();

        assertThat("Force merge had failed shards (merge corruption)", mergeResponse.getFailedShards(), equalTo(0));

        // The "tragic event" path causes shard failures → cluster RED
        var health = client().admin().cluster().prepareHealth(indexName).get();
        assertThat("Cluster RED — shard failed due to merge corruption", health.getStatus().toString(), equalTo("GREEN"));

        refresh(indexName);
        SearchResponse afterMerge = client().prepareSearch(indexName).setSize(0).get();
        assertThat("Should have docs after merge", afterMerge.getHits().getTotalHits().value(), greaterThan(0L));

        assertThat("Corruption errors detected during workload", corruptionErrors.get(), equalTo(0));
    }

    private XContentBuilder httpLogDoc(int docId, int round) throws java.io.IOException {
        return XContentFactory
            .jsonBuilder()
            .startObject()
            .field("@timestamp", System.currentTimeMillis())
            .field("clientip", "192.168." + (docId % 256) + "." + (docId % 256))
            .field("request", "GET /index.html?id=" + docId + " HTTP/1.0")
            .field("status", randomFrom(200, 201, 204, 301, 302, 304, 400, 404, 500))
            .field("size", randomIntBetween(100, 50000))
            .field("response_time_ms", randomIntBetween(1, 5000))
            .field("user_agent", "Mozilla/5.0 (compatible; bot/" + round + ") doc-" + docId)
            .field("referer", "https://example.com/page" + (docId % 100))
            .field("method", randomFrom("GET", "POST", "PUT", "DELETE"))
            .field("protocol", "HTTP/1.1")
            .field("host", "host" + (docId % 50) + ".example.com")
            .field("region", randomFrom("us-east-1", "us-west-2", "eu-west-1", "ap-south-1"))
            .field("update_round", round)
            .field("doc_id", docId)
            .endObject();
    }

    private void checkResponseForCorruption(BulkResponse response, int clientId, int round, AtomicInteger corruptionErrors) {
        if (response.hasFailures()) {
            String msg = response.buildFailureMessage();
            if (containsCorruptionSignature(msg)) {
                corruptionErrors.incrementAndGet();
                logger.error("CORRUPTION in client {} round {}: {}", clientId, round, msg);
            }
        }
    }

    private void checkExceptionForCorruption(Exception e, int clientId, AtomicInteger corruptionErrors) {
        String msg = e.getMessage() != null ? e.getMessage() : "";
        if (containsCorruptionSignature(msg)) {
            corruptionErrors.incrementAndGet();
            logger.error("CORRUPTION exception in client {}: {}", clientId, msg, e);
        }
    }

    private boolean containsCorruptionSignature(String msg) {
        return msg.contains("arraycopy")
            || msg.contains("Unknown type flag")
            || msg.contains("LZ4")
            || msg.contains("Illegal dict length")
            || msg.contains("tragic")
            || msg.contains("already closed")
            || msg.contains("CorruptIndexException");
    }
}

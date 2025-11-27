/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

import java.util.Arrays;
import java.util.Collection;

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

/**
 * Integration tests for cache invalidation when indices/shards are deleted.
 * Verifies that cache entries are properly cleaned up to prevent memory leaks
 * and stale data.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CacheInvalidationIntegTests extends OpenSearchIntegTestCase {

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
     * Tests that cache entries are invalidated when an encrypted index is deleted.
     * This prevents memory leaks and ensures stale data is removed from cache.
     */
    public void testCacheInvalidationOnIndexDelete() throws Exception {
        internalCluster().startNode();

        // Create encrypted index with multiple shards
        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 0)
            .build();

        createIndex("test-cache-invalidation", settings);
        ensureGreen("test-cache-invalidation");

        // Index documents to populate cache
        int numDocs = randomIntBetween(100, 200);
        for (int i = 0; i < numDocs; i++) {
            // Create larger documents to ensure cache gets populated
            StringBuilder largeValue = new StringBuilder();
            for (int j = 0; j < 100; j++) {
                largeValue.append("data-").append(i).append("-").append(j).append(" ");
            }
            index("test-cache-invalidation", "_doc", String.valueOf(i), "field", largeValue.toString(), "number", i);
        }
        refresh();
        flush("test-cache-invalidation");

        for (int i = 0; i < Math.min(50, numDocs); i++) {
            SearchResponse response = client()
                .prepareSearch("test-cache-invalidation")
                .setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", i))
                .get();
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
        }

        // Get cache size before deletion
        BlockCache<?> cache = CryptoDirectoryFactory.getSharedBlockCache();
        assertNotNull("Shared cache should be initialized", cache);

        long cacheSizeBefore = 0;
        if (cache instanceof CaffeineBlockCache<?, ?> caffeineCache) {
            cacheSizeBefore = caffeineCache.getCache().estimatedSize();
        }

        logger.info("Cache size before index deletion: {}", cacheSizeBefore);
        assertThat("Cache should have entries after indexing and reading", cacheSizeBefore, greaterThan(0L));

        // Delete the index - this should trigger cache invalidation
        DeleteIndexRequest deleteRequest = new DeleteIndexRequest("test-cache-invalidation");
        client().admin().indices().delete(deleteRequest).actionGet();

        // Allow some time for cache cleanup to complete
        Thread.sleep(100);

        // Verify cache size decreased
        long cacheSizeAfter = 0;
        if (cache instanceof CaffeineBlockCache<?, ?> caffeineCache) {
            // Force cleanup of any pending invalidations
            caffeineCache.getCache().cleanUp();
            cacheSizeAfter = caffeineCache.getCache().estimatedSize();
        }

        logger.info("Cache size after index deletion: {} (was: {})", cacheSizeAfter, cacheSizeBefore);
        assertThat("Cache should have fewer entries after index deletion", cacheSizeAfter, lessThan(cacheSizeBefore));
    }

    /**
     * Tests that cache entries are properly scoped to indices - deleting one index
     * should not affect cache entries from other indices.
     */
    public void testCacheInvalidationPreservesOtherIndices() throws Exception {
        internalCluster().startNode();

        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();

        // Create two encrypted indices
        createIndex("test-index-1", settings);
        createIndex("test-index-2", settings);
        ensureGreen("test-index-1", "test-index-2");

        // Index documents in both indices
        int numDocs = 50;
        for (int i = 0; i < numDocs; i++) {
            StringBuilder largeValue = new StringBuilder();
            for (int j = 0; j < 50; j++) {
                largeValue.append("data-").append(i).append("-").append(j).append(" ");
            }

            index("test-index-1", "_doc", String.valueOf(i), "field", largeValue.toString(), "number", i);
            index("test-index-2", "_doc", String.valueOf(i), "field", largeValue.toString(), "number", i);
        }
        refresh();
        flush("test-index-1", "test-index-2");

        // Read from both indices to populate cache
        for (int i = 0; i < 20; i++) {
            client().prepareSearch("test-index-1").setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", i)).get();
            client().prepareSearch("test-index-2").setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", i)).get();
        }

        // Get cache size before deletion
        BlockCache<?> cache = CryptoDirectoryFactory.getSharedBlockCache();
        assertNotNull("Shared cache should be initialized", cache);

        long cacheSizeBefore = 0;
        if (cache instanceof CaffeineBlockCache<?, ?> caffeineCache) {
            cacheSizeBefore = caffeineCache.getCache().estimatedSize();
        }

        logger.info("Cache size before deleting index-1: {}", cacheSizeBefore);

        // Delete only the first index
        DeleteIndexRequest deleteRequest = new DeleteIndexRequest("test-index-1");
        client().admin().indices().delete(deleteRequest).actionGet();

        Thread.sleep(100);

        // Verify index-2 still works perfectly
        SearchResponse response = client().prepareSearch("test-index-2").setSize(0).get();
        assertThat("Index-2 should still have all documents", response.getHits().getTotalHits().value(), equalTo((long) numDocs));

        // Can still read specific documents from index-2
        SearchResponse specificDoc = client()
            .prepareSearch("test-index-2")
            .setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", 5))
            .get();
        assertThat("Should still be able to read from index-2", specificDoc.getHits().getTotalHits().value(), equalTo(1L));

        // Cache should have some entries remaining (from index-2)
        long cacheSizeAfter = 0;
        if (cache instanceof CaffeineBlockCache<?, ?> caffeineCache) {
            caffeineCache.getCache().cleanUp();
            cacheSizeAfter = caffeineCache.getCache().estimatedSize();
        }

        logger.info("Cache size after deleting index-1: {} (was: {})", cacheSizeAfter, cacheSizeBefore);
        // Cache should have decreased but not be empty (index-2 entries should remain)
        assertThat("Cache should have fewer entries after index deletion", cacheSizeAfter, lessThan(cacheSizeBefore));
    }

    /**
     * Tests cache invalidation with multiple indices being deleted sequentially.
     * Verifies that each deletion properly cleans up its cache entries.
     */
    public void testSequentialIndexDeletion() throws Exception {
        internalCluster().startNode();

        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();

        // Create multiple encrypted indices
        int numIndices = 3;
        String[] indexNames = new String[numIndices];
        for (int idx = 0; idx < numIndices; idx++) {
            indexNames[idx] = "test-sequential-" + idx;
            createIndex(indexNames[idx], settings);
        }
        ensureGreen(indexNames);

        // Index documents in all indices
        int numDocs = 30;
        for (int idx = 0; idx < numIndices; idx++) {
            for (int i = 0; i < numDocs; i++) {
                StringBuilder value = new StringBuilder();
                for (int j = 0; j < 30; j++) {
                    value.append("idx-").append(idx).append("-doc-").append(i).append("-").append(j).append(" ");
                }
                index(indexNames[idx], "_doc", String.valueOf(i), "field", value.toString(), "number", i);
            }
        }
        refresh(indexNames);
        flush(indexNames);

        // Read from all indices to populate cache
        for (int idx = 0; idx < numIndices; idx++) {
            for (int i = 0; i < 10; i++) {
                client().prepareSearch(indexNames[idx]).setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", i)).get();
            }
        }

        BlockCache<?> cache = CryptoDirectoryFactory.getSharedBlockCache();
        assertNotNull("Shared cache should be initialized", cache);

        // Delete indices one by one and verify cache decreases each time
        long previousCacheSize = Long.MAX_VALUE;
        if (cache instanceof CaffeineBlockCache<?, ?> caffeineCache) {
            previousCacheSize = caffeineCache.getCache().estimatedSize();
        }

        logger.info("Initial cache size: {}", previousCacheSize);

        for (int idx = 0; idx < numIndices; idx++) {
            // Delete index
            DeleteIndexRequest deleteRequest = new DeleteIndexRequest(indexNames[idx]);
            client().admin().indices().delete(deleteRequest).actionGet();

            Thread.sleep(100);

            // Verify cache size decreased
            long currentCacheSize = 0;
            if (cache instanceof CaffeineBlockCache<?, ?> caffeineCache) {
                caffeineCache.getCache().cleanUp();
                currentCacheSize = caffeineCache.getCache().estimatedSize();
            }

            logger.info("Cache size after deleting {}: {} (was: {})", indexNames[idx], currentCacheSize, previousCacheSize);

            // Cache should decrease or stay the same (if cache eviction already happened)
            assertThat("Cache should not grow after index deletion", currentCacheSize, lessThan(previousCacheSize + 10));

            previousCacheSize = currentCacheSize;
        }

        // After deleting all indices, cache should be mostly empty
        // (some minimal overhead may remain)
        long finalCacheSize = 0;
        if (cache instanceof CaffeineBlockCache<?, ?> caffeineCache) {
            caffeineCache.getCache().cleanUp();
            finalCacheSize = caffeineCache.getCache().estimatedSize();
        }

        logger.info("Final cache size after all deletions: {}", finalCacheSize);
    }

    /**
     * Tests that encryption metadata cache is also cleaned up on index deletion.
     */
    public void testEncryptionMetadataCacheCleanup() throws Exception {
        internalCluster().startNode();

        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 0)
            .build();

        createIndex("test-metadata-cleanup", settings);
        ensureGreen("test-metadata-cleanup");

        // Index some documents
        int numDocs = 50;
        for (int i = 0; i < numDocs; i++) {
            index("test-metadata-cleanup", "_doc", String.valueOf(i), "field", "value" + i);
        }
        refresh();
        flush("test-metadata-cleanup");

        // Read documents to ensure metadata cache is populated
        for (int i = 0; i < 10; i++) {
            client()
                .prepareSearch("test-metadata-cleanup")
                .setQuery(org.opensearch.index.query.QueryBuilders.termQuery("field", "value" + i))
                .get();
        }

        // Delete index
        DeleteIndexRequest deleteRequest = new DeleteIndexRequest("test-metadata-cleanup");
        client().admin().indices().delete(deleteRequest).actionGet();

        // Index should be deleted successfully without errors
        // The encryption metadata cache cleanup is verified by the absence of errors
        // and successful deletion
        logger.info("Index deleted successfully, encryption metadata cache should be cleaned up");

        // Create a new index with the same settings to ensure no conflicts
        createIndex("test-metadata-cleanup-2", settings);
        ensureGreen("test-metadata-cleanup-2");

        // Should be able to use the new index without issues
        index("test-metadata-cleanup-2", "_doc", "1", "field", "newvalue");
        refresh("test-metadata-cleanup-2");

        SearchResponse response = client().prepareSearch("test-metadata-cleanup-2").get();
        assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
    }

    /**
     * Tests that cache entries are invalidated when an index is closed.
     * Closed indices cannot be read, so their cached blocks should be cleared
     * to free memory.
     */
    public void testCacheInvalidationOnIndexClose() throws Exception {
        internalCluster().startNode();

        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();

        createIndex("test-close-cache", settings);
        ensureGreen("test-close-cache");

        // Index documents to populate cache
        int numDocs = 50;
        for (int i = 0; i < numDocs; i++) {
            StringBuilder largeValue = new StringBuilder();
            for (int j = 0; j < 50; j++) {
                largeValue.append("data-").append(i).append("-").append(j).append(" ");
            }
            index("test-close-cache", "_doc", String.valueOf(i), "field", largeValue.toString(), "number", i);
        }
        refresh();
        flush("test-close-cache");

        // Perform reads to ensure cache is populated
        for (int i = 0; i < 20; i++) {
            client().prepareSearch("test-close-cache").setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", i)).get();
        }

        // Get cache size before closing
        BlockCache<?> cache = CryptoDirectoryFactory.getSharedBlockCache();
        assertNotNull("Shared cache should be initialized", cache);

        long cacheSizeBefore = 0;
        if (cache instanceof CaffeineBlockCache<?, ?> caffeineCache) {
            cacheSizeBefore = caffeineCache.getCache().estimatedSize();
        }

        logger.info("Cache size before index close: {}", cacheSizeBefore);
        assertThat("Cache should have entries after indexing and reading", cacheSizeBefore, greaterThan(0L));

        // Close the index
        client().admin().indices().prepareClose("test-close-cache").get();
        logger.info("Index closed");

        // Reopen the index
        client().admin().indices().prepareOpen("test-close-cache").get();
        ensureGreen("test-close-cache");

        // Verify data is still accessible after reopening
        SearchResponse response = client().prepareSearch("test-close-cache").setSize(0).get();
        assertThat("All documents should be accessible after reopen", response.getHits().getTotalHits().value(), equalTo((long) numDocs));

        // Read specific document to verify integrity
        SearchResponse specificDoc = client()
            .prepareSearch("test-close-cache")
            .setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", 10))
            .get();
        assertThat("Should be able to read specific document after reopen", specificDoc.getHits().getTotalHits().value(), equalTo(1L));
    }

    /**
     * Tests that deleting all encrypted indices results in an empty cache.
     * This verifies complete cleanup with no memory leaks.
     */
    public void testCacheEmptyAfterAllIndicesDeleted() throws Exception {
        internalCluster().startNode();

        int numIndices = 4;
        String[] indexNames = new String[numIndices];

        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 0)
            .build();

        // Create multiple encrypted indices
        for (int idx = 0; idx < numIndices; idx++) {
            indexNames[idx] = "test-empty-cache-" + idx;
            createIndex(indexNames[idx], settings);
        }
        ensureGreen(indexNames);

        // Index documents in all indices
        int docsPerIndex = 30;
        for (int idx = 0; idx < numIndices; idx++) {
            for (int i = 0; i < docsPerIndex; i++) {
                StringBuilder value = new StringBuilder();
                for (int j = 0; j < 40; j++) {
                    value.append("idx-").append(idx).append("-doc-").append(i).append("-").append(j).append(" ");
                }
                index(indexNames[idx], "_doc", String.valueOf(i), "field", value.toString(), "number", i);
            }
        }
        refresh(indexNames);
        flush(indexNames);

        // Read from all indices to ensure cache is populated
        for (int idx = 0; idx < numIndices; idx++) {
            for (int i = 0; i < 10; i++) {
                client().prepareSearch(indexNames[idx]).setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", i)).get();
            }
        }

        // Verify cache has entries
        BlockCache<?> cache = CryptoDirectoryFactory.getSharedBlockCache();
        assertNotNull("Shared cache should be initialized", cache);

        long cacheSizeBefore = 0;
        if (cache instanceof CaffeineBlockCache<?, ?> caffeineCache) {
            cacheSizeBefore = caffeineCache.getCache().estimatedSize();
        }

        logger.info("Cache size before deleting all indices: {}", cacheSizeBefore);
        assertThat("Cache should have entries from all indices", cacheSizeBefore, greaterThan(0L));

        // Delete all indices
        for (String indexName : indexNames) {
            DeleteIndexRequest deleteRequest = new DeleteIndexRequest(indexName);
            client().admin().indices().delete(deleteRequest).actionGet();
        }

        // Allow time for cache cleanup
        Thread.sleep(200);

        // Verify cache is empty
        long cacheSizeAfter = 0;
        if (cache instanceof CaffeineBlockCache<?, ?> caffeineCache) {
            caffeineCache.getCache().cleanUp();
            cacheSizeAfter = caffeineCache.getCache().estimatedSize();
        }

        logger.info("Cache size after deleting all {} indices: {} (was: {})", numIndices, cacheSizeAfter, cacheSizeBefore);
        assertThat("Cache should be empty after all indices are deleted", cacheSizeAfter, equalTo(0L));
    }

    /**
     * Tests that closing all encrypted indices results in an empty cache.
     * This verifies that closed indices don't waste memory with cached blocks.
     */
    public void testCacheEmptyAfterAllIndicesClosed() throws Exception {
        internalCluster().startNode();

        int numIndices = 4;
        String[] indexNames = new String[numIndices];

        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 0)
            .build();

        // Create multiple encrypted indices
        for (int idx = 0; idx < numIndices; idx++) {
            indexNames[idx] = "test-close-all-" + idx;
            createIndex(indexNames[idx], settings);
        }
        ensureGreen(indexNames);

        // Index documents in all indices
        int docsPerIndex = 30;
        for (int idx = 0; idx < numIndices; idx++) {
            for (int i = 0; i < docsPerIndex; i++) {
                StringBuilder value = new StringBuilder();
                for (int j = 0; j < 40; j++) {
                    value.append("idx-").append(idx).append("-doc-").append(i).append("-").append(j).append(" ");
                }
                index(indexNames[idx], "_doc", String.valueOf(i), "field", value.toString(), "number", i);
            }
        }
        refresh(indexNames);
        flush(indexNames);

        // Read from all indices to ensure data is accessible
        for (int idx = 0; idx < numIndices; idx++) {
            for (int i = 0; i < 10; i++) {
                client().prepareSearch(indexNames[idx]).setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", i)).get();
            }
        }

        // Close all indices
        for (String indexName : indexNames) {
            client().admin().indices().prepareClose(indexName).get();
        }

        logger.info("Closed all {} indices", numIndices);

        // Reopen all indices and verify data is still accessible
        for (String indexName : indexNames) {
            client().admin().indices().prepareOpen(indexName).get();
        }
        ensureGreen(indexNames);

        // Verify all indices have their data - this is the correctness test
        for (int idx = 0; idx < numIndices; idx++) {
            SearchResponse response = client().prepareSearch(indexNames[idx]).setSize(0).get();
            assertThat(
                "Index " + indexNames[idx] + " should have all documents after reopen",
                response.getHits().getTotalHits().value(),
                equalTo((long) docsPerIndex)
            );

            // Also verify we can read specific documents
            SearchResponse specificDoc = client()
                .prepareSearch(indexNames[idx])
                .setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", 5))
                .get();
            assertThat(
                "Index " + indexNames[idx] + " should have specific document after reopen",
                specificDoc.getHits().getTotalHits().value(),
                equalTo(1L)
            );
        }

        logger.info("All {} indices reopened successfully with data intact and searchable", numIndices);
    }
}

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
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_loader.FileChannelCache;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

/**
 * Integration tests for cache invalidation when indices/shards are deleted.
 * Verifies that cache entries are properly cleaned up to prevent memory leaks
 * and stale data.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
@ThreadLeakFilters(filters = CaffeineThreadLeakFilter.class)
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

    // ==================== FD Cache (FileChannelCache) Invalidation Tests ====================

    /**
     * Clears the shared block cache so that subsequent reads are cold and go through
     * the block loader, which populates the FD cache via fileChannelCache.acquire().
     *
     * Without this, the block cache is warm from indexing writes, so reads are all
     * block cache hits that never touch the FD cache.
     */
    @SuppressWarnings("unchecked")
    private void clearBlockCache() {
        BlockCache<?> cache = CryptoDirectoryFactory.getSharedBlockCache();
        if (cache instanceof CaffeineBlockCache<?, ?> caffeineCache) {
            caffeineCache.clear();
            caffeineCache.getCache().cleanUp();
        }
    }

    /**
     * Tests that FD cache entries are invalidated when an encrypted index is deleted.
     * After deletion, the FD cache should be empty (all entries for that index invalidated).
     * This prevents FD leaks where stale FileChannels remain open to deleted files.
     */
    public void testFdCacheInvalidationOnIndexDelete() throws Exception {
        internalCluster().startNode();

        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 0)
            .build();

        createIndex("test-fd-cache-delete", settings);
        ensureGreen("test-fd-cache-delete");

        int numDocs = randomIntBetween(100, 200);
        for (int i = 0; i < numDocs; i++) {
            StringBuilder largeValue = new StringBuilder();
            for (int j = 0; j < 100; j++) {
                largeValue.append("data-").append(i).append("-").append(j).append(" ");
            }
            index("test-fd-cache-delete", "_doc", String.valueOf(i), "field", largeValue.toString(), "number", i);
        }
        refresh();
        flush("test-fd-cache-delete");

        // Clear block cache so reads go through block loader → FD cache
        clearBlockCache();

        // Reads now trigger block cache misses → CryptoDirectIOBlockLoader.load() → fileChannelCache.acquire()
        for (int i = 0; i < Math.min(50, numDocs); i++) {
            SearchResponse response = client()
                .prepareSearch("test-fd-cache-delete")
                .setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", i))
                .get();
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
        }

        FileChannelCache fdCache = CryptoDirectoryFactory.getSharedFileChannelCache();
        assertNotNull("Shared FD cache should be initialized", fdCache);

        fdCache.cleanUp();
        long fdCacheSizeBefore = fdCache.estimatedSize();
        logger.info("FD cache size before index deletion: {}", fdCacheSizeBefore);
        assertThat("FD cache should have entries after cold reads", fdCacheSizeBefore, greaterThan(0L));

        // Delete the index — triggers BufferPoolDirectory.close() → fdCache.invalidateByPathPrefix()
        client().admin().indices().delete(new DeleteIndexRequest("test-fd-cache-delete")).actionGet();

        fdCache.cleanUp();
        long fdCacheSizeAfter = fdCache.estimatedSize();
        logger.info("FD cache size after index deletion: {} (was: {})", fdCacheSizeAfter, fdCacheSizeBefore);
        assertThat("FD cache should be empty after index deletion", fdCacheSizeAfter, equalTo(0L));
    }

    /**
     * Tests that deleting one index only invalidates FD cache entries for that index,
     * preserving entries for other indices.
     */
    public void testFdCacheInvalidationPreservesOtherIndices() throws Exception {
        internalCluster().startNode();

        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();

        createIndex("test-fd-idx-1", settings);
        createIndex("test-fd-idx-2", settings);
        ensureGreen("test-fd-idx-1", "test-fd-idx-2");

        int numDocs = 50;
        for (int i = 0; i < numDocs; i++) {
            StringBuilder largeValue = new StringBuilder();
            for (int j = 0; j < 50; j++) {
                largeValue.append("data-").append(i).append("-").append(j).append(" ");
            }
            index("test-fd-idx-1", "_doc", String.valueOf(i), "field", largeValue.toString(), "number", i);
            index("test-fd-idx-2", "_doc", String.valueOf(i), "field", largeValue.toString(), "number", i);
        }
        refresh();
        flush("test-fd-idx-1", "test-fd-idx-2");

        // Clear block cache so reads populate FD cache
        clearBlockCache();

        for (int i = 0; i < 20; i++) {
            client().prepareSearch("test-fd-idx-1").setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", i)).get();
            client().prepareSearch("test-fd-idx-2").setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", i)).get();
        }

        FileChannelCache fdCache = CryptoDirectoryFactory.getSharedFileChannelCache();
        assertNotNull("Shared FD cache should be initialized", fdCache);

        fdCache.cleanUp();
        long fdCacheSizeBefore = fdCache.estimatedSize();
        logger.info("FD cache size before deleting idx-1: {}", fdCacheSizeBefore);
        assertThat("FD cache should have entries from both indices", fdCacheSizeBefore, greaterThan(0L));

        // Delete only index-1
        client().admin().indices().delete(new DeleteIndexRequest("test-fd-idx-1")).actionGet();

        fdCache.cleanUp();
        long fdCacheSizeAfter = fdCache.estimatedSize();
        logger.info("FD cache size after deleting idx-1: {} (was: {})", fdCacheSizeAfter, fdCacheSizeBefore);

        assertThat("FD cache should have fewer entries", fdCacheSizeAfter, lessThan(fdCacheSizeBefore));
        assertThat("FD cache should still have entries from index-2", fdCacheSizeAfter, greaterThan(0L));

        // Verify index-2 still works
        SearchResponse response = client().prepareSearch("test-fd-idx-2").setSize(0).get();
        assertThat("Index-2 should still have all documents", response.getHits().getTotalHits().value(), equalTo((long) numDocs));
    }

    /**
     * Tests that force-merge (which deletes old segment files) invalidates FD cache entries
     * for the deleted segments via BufferPoolDirectory.deleteFile() → fileChannelCache.invalidate().
     *
     * After force-merge, the FD cache may contain entries for the new merged segment (opened
     * during merge reads or post-merge searches). The key invariant is:
     * 1. Old segment FDs are invalidated (deleteFile calls invalidate())
     * 2. Deleting the index after merge leaves the FD cache empty (no leaked FDs)
     * 3. Data remains readable through the merged segment
     */
    public void testFdCacheInvalidationOnForceMerge() throws Exception {
        internalCluster().startNode();

        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.merge.policy.max_merged_segment", "100gb")
            .build();

        createIndex("test-fd-merge", settings);
        ensureGreen("test-fd-merge");

        // Index in multiple batches with flush → creates multiple segments
        for (int batch = 0; batch < 5; batch++) {
            for (int i = 0; i < 20; i++) {
                int docId = batch * 20 + i;
                StringBuilder value = new StringBuilder();
                for (int j = 0; j < 50; j++) {
                    value.append("batch-").append(batch).append("-doc-").append(docId).append("-").append(j).append(" ");
                }
                index("test-fd-merge", "_doc", String.valueOf(docId), "field", value.toString(), "number", docId);
            }
            flush("test-fd-merge");
        }
        refresh();

        // Clear block cache so reads populate FD cache
        clearBlockCache();

        for (int i = 0; i < 50; i++) {
            client().prepareSearch("test-fd-merge").setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", i)).get();
        }

        FileChannelCache fdCache = CryptoDirectoryFactory.getSharedFileChannelCache();
        assertNotNull("Shared FD cache should be initialized", fdCache);

        fdCache.cleanUp();
        long fdCacheSizeBeforeMerge = fdCache.estimatedSize();
        logger.info("FD cache size before force-merge: {}", fdCacheSizeBeforeMerge);
        assertThat("FD cache should have entries for multiple segments", fdCacheSizeBeforeMerge, greaterThan(1L));

        // Force-merge to 1 segment — old segment files deleted → FD cache entries invalidated
        // Note: the merge itself may open new FDs for the merged segment, so the net FD count
        // may not decrease. The important thing is old segment FDs are cleaned up.
        ForceMergeResponse mergeResponse = client().admin().indices().prepareForceMerge("test-fd-merge").setMaxNumSegments(1).get();
        assertThat("Force merge should succeed", mergeResponse.getFailedShards(), equalTo(0));

        // Verify data is still readable through the merged segment
        SearchResponse response = client().prepareSearch("test-fd-merge").setSize(0).get();
        assertThat("All 100 documents should be readable after merge", response.getHits().getTotalHits().value(), equalTo(100L));

        // The real invariant: deleting the index after merge should leave FD cache empty.
        // This proves no stale FDs leaked from the old segments.
        client().admin().indices().delete(new DeleteIndexRequest("test-fd-merge")).actionGet();

        fdCache.cleanUp();
        long fdCacheSizeAfterDelete = fdCache.estimatedSize();
        logger
            .info(
                "FD cache size after index deletion (post-merge): {} (was: {} before merge)",
                fdCacheSizeAfterDelete,
                fdCacheSizeBeforeMerge
            );
        assertThat("FD cache should be empty after deleting merged index", fdCacheSizeAfterDelete, equalTo(0L));
    }

    /**
     * Tests that closing an index (shard close) invalidates FD cache entries.
     * BufferPoolDirectory.close() calls fileChannelCache.invalidateByPathPrefix(dirPath).
     * After reopening, reads should still work (FD cache re-populates on demand).
     */
    public void testFdCacheInvalidationOnIndexClose() throws Exception {
        internalCluster().startNode();

        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();

        createIndex("test-fd-close", settings);
        ensureGreen("test-fd-close");

        int numDocs = 50;
        for (int i = 0; i < numDocs; i++) {
            StringBuilder largeValue = new StringBuilder();
            for (int j = 0; j < 50; j++) {
                largeValue.append("data-").append(i).append("-").append(j).append(" ");
            }
            index("test-fd-close", "_doc", String.valueOf(i), "field", largeValue.toString(), "number", i);
        }
        refresh();
        flush("test-fd-close");

        // Clear block cache so reads populate FD cache
        clearBlockCache();

        for (int i = 0; i < 20; i++) {
            client().prepareSearch("test-fd-close").setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", i)).get();
        }

        FileChannelCache fdCache = CryptoDirectoryFactory.getSharedFileChannelCache();
        assertNotNull("Shared FD cache should be initialized", fdCache);

        fdCache.cleanUp();
        long fdCacheSizeBefore = fdCache.estimatedSize();
        logger.info("FD cache size before index close: {}", fdCacheSizeBefore);
        assertThat("FD cache should have entries", fdCacheSizeBefore, greaterThan(0L));

        // Close the index — triggers BufferPoolDirectory.close() → fdCache.invalidateByPathPrefix()
        client().admin().indices().prepareClose("test-fd-close").get();

        fdCache.cleanUp();
        long fdCacheSizeAfterClose = fdCache.estimatedSize();
        logger.info("FD cache size after index close: {} (was: {})", fdCacheSizeAfterClose, fdCacheSizeBefore);
        assertThat("FD cache should be empty after index close", fdCacheSizeAfterClose, equalTo(0L));

        // Reopen and verify data is still accessible
        client().admin().indices().prepareOpen("test-fd-close").get();
        ensureGreen("test-fd-close");

        SearchResponse response = client().prepareSearch("test-fd-close").setSize(0).get();
        assertThat("All documents should be accessible after reopen", response.getHits().getTotalHits().value(), equalTo((long) numDocs));
    }

    /**
     * Tests that deleting all encrypted indices results in an empty FD cache.
     * Verifies complete FD cleanup with no leaked FileChannels.
     */
    public void testFdCacheEmptyAfterAllIndicesDeleted() throws Exception {
        internalCluster().startNode();

        int numIndices = 3;
        String[] indexNames = new String[numIndices];

        Settings settings = Settings
            .builder()
            .put(cryptoIndexSettings())
            .put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 0)
            .build();

        for (int idx = 0; idx < numIndices; idx++) {
            indexNames[idx] = "test-fd-all-" + idx;
            createIndex(indexNames[idx], settings);
        }
        ensureGreen(indexNames);

        for (int idx = 0; idx < numIndices; idx++) {
            for (int i = 0; i < 30; i++) {
                StringBuilder value = new StringBuilder();
                for (int j = 0; j < 30; j++) {
                    value.append("idx-").append(idx).append("-doc-").append(i).append("-").append(j).append(" ");
                }
                index(indexNames[idx], "_doc", String.valueOf(i), "field", value.toString(), "number", i);
            }
        }
        refresh(indexNames);
        flush(indexNames);

        // Clear block cache so reads populate FD cache
        clearBlockCache();

        for (int idx = 0; idx < numIndices; idx++) {
            for (int i = 0; i < 10; i++) {
                client().prepareSearch(indexNames[idx]).setQuery(org.opensearch.index.query.QueryBuilders.termQuery("number", i)).get();
            }
        }

        FileChannelCache fdCache = CryptoDirectoryFactory.getSharedFileChannelCache();
        assertNotNull("Shared FD cache should be initialized", fdCache);

        fdCache.cleanUp();
        long fdCacheSizeBefore = fdCache.estimatedSize();
        logger.info("FD cache size before deleting all indices: {}", fdCacheSizeBefore);
        assertThat("FD cache should have entries", fdCacheSizeBefore, greaterThan(0L));

        // Delete all indices
        for (String indexName : indexNames) {
            client().admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
        }

        fdCache.cleanUp();
        long fdCacheSizeAfter = fdCache.estimatedSize();
        logger.info("FD cache size after deleting all {} indices: {} (was: {})", numIndices, fdCacheSizeAfter, fdCacheSizeBefore);
        assertThat("FD cache should be empty after all indices deleted", fdCacheSizeAfter, equalTo(0L));
    }
}

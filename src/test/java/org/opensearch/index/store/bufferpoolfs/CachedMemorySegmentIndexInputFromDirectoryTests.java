/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.opensearch.discovery.DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING;
import static org.opensearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.opensearch.index.store.CryptoDirectoryFactory.DEFAULT_CRYPTO_PROVIDER;

import java.io.IOException;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.After;
import org.junit.Before;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.store.CaffeineThreadLeakFilter;
import org.opensearch.index.store.CryptoDirectoryFactory;
import org.opensearch.index.store.DummyKeyProvider;
import org.opensearch.index.store.PanamaNativeAccess;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.cipher.EncryptionMetadataCacheRegistry;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.pool.PoolBuilder;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.indices.IndicesQueryCache;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.store.IndicesStore;
import org.opensearch.search.SearchService;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

@ThreadLeakFilters(filters = CaffeineThreadLeakFilter.class)
public class CachedMemorySegmentIndexInputFromDirectoryTests extends BaseIndexInputTests {

    BufferPoolDirectory bufferPoolDirectory;
    private PoolBuilder.PoolResources poolResources;
    private boolean originalWriteCacheEnabled;

    private Settings createNodeSettings() {
        return Settings
            .builder()
            .put(nodeSettings(1))
            .put("plugins.crypto.enabled", true)
            .put("node.store.crypto.pool_size_percentage", 0.05)  // 5% of off-heap for tests (smaller pool)
            .put("node.store.crypto.warmup_percentage", 0.0)      // No warmup to avoid pre-allocating memory
            .put("node.store.crypto.cache_to_pool_ratio", 0.8)
            .build();
    }

    private Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings
            .builder()
            // Default the watermarks to absurdly low to prevent the tests
            // from failing on nodes without enough disk space
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1b")
            // by default we never cache below 10k docs in a segment,
            // bypass this limit so that caching gets some testing in
            // integration tests that usually create few documents
            .put(IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), nodeOrdinal % 2 == 0)
            // wait short time for other active shards before actually deleting, default 30s not needed in tests
            .put(IndicesStore.INDICES_STORE_DELETE_SHARD_TIMEOUT.getKey(), new TimeValue(1, TimeUnit.SECONDS))
            // randomly enable low-level search cancellation to make sure it does not alter results
            .put(SearchService.LOW_LEVEL_CANCELLATION_SETTING.getKey(), randomBoolean())
            .putList(DISCOVERY_SEED_HOSTS_SETTING.getKey()) // empty list disables a port scan for other nodes
            .putList(DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), "file")
            // By default, for tests we will put the target slice count of 2. This will increase the probability of having multiple slices
            // when tests are run with concurrent segment search enabled
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_MAX_SLICE_COUNT_KEY, 2)
            // Set the field data cache clean interval setting to 1s so assertBusy() can ensure cache is cleared post-test within its
            // default 10s limit.
            .put(IndicesService.INDICES_CACHE_CLEAN_INTERVAL_SETTING.getKey(), "1s");
        return builder.build();
    }

    @Before
    public void Setup() throws Exception {
        super.setUp();
        // Save and disable write-through cache so reads go through the BlockLoader (Direct I/O)
        originalWriteCacheEnabled = CryptoDirectoryFactory.isWriteCacheEnabled();
        CryptoDirectoryFactory.setNodeSettings(Settings.builder().put("node.store.crypto.write_cache_enabled", false).build());
        // Simulate FS block size (4 KB) smaller than cache block size (8 KB)
        PanamaNativeAccess.setFileSystemBlockSizeOverride(4096);
        Path path = createTempDir();
        Provider provider = Security.getProvider(DEFAULT_CRYPTO_PROVIDER);
        MasterKeyProvider keyProvider = DummyKeyProvider.create();
        this.poolResources = PoolBuilder.build(createNodeSettings());
        Pool<RefCountedMemorySegment> segmentPool = poolResources.getSegmentPool();

        String indexUuid = randomAlphaOfLength(10);
        String indexName = randomAlphaOfLength(10);
        FSDirectory fsDirectory = FSDirectory.open(path);
        int shardId = 0;

        KeyResolver keyResolver = new TestKeyResolver(indexUuid, indexName, fsDirectory, provider, keyProvider, shardId);

        EncryptionMetadataCache encryptionMetadataCache = EncryptionMetadataCacheRegistry.getOrCreateCache(indexUuid, shardId, indexName);

        BlockLoader<RefCountedMemorySegment> loader = new CryptoDirectIOBlockLoader(segmentPool, keyResolver, encryptionMetadataCache);

        Worker worker = poolResources.getSharedReadaheadWorker();

        CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> sharedCaffeineCache =
            (CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment>) poolResources.getBlockCache();

        BlockCache<RefCountedMemorySegment> directoryCache = new CaffeineBlockCache<>(
            sharedCaffeineCache.getCache(),
            loader,
            poolResources.getMaxCacheBlocks()
        );

        this.bufferPoolDirectory = new BufferPoolDirectory(
            path,
            FSLockFactory.getDefault(),
            provider,
            keyResolver,
            segmentPool,
            directoryCache,
            loader,
            worker,
            encryptionMetadataCache
        );
    }

    @After
    public void close() throws IOException {
        PanamaNativeAccess.clearFileSystemBlockSizeOverride();
        CryptoDirectoryFactory
            .setNodeSettings(Settings.builder().put("node.store.crypto.write_cache_enabled", originalWriteCacheEnabled).build());
        this.poolResources.close();
        this.bufferPoolDirectory.close();
    }

    private byte[] randomByteArray(int length) {
        byte[] bytes = new byte[length];
        random().nextBytes(bytes);
        return bytes;
    }

    @Override
    protected IndexInput getIndexInput(byte[] bytes) throws IOException {
        String fileName = randomAlphaOfLength(10);
        IndexOutput output = this.bufferPoolDirectory.createOutput(fileName, IOContext.DEFAULT);
        int length = bytes.length;
        output.writeBytes(bytes, length);
        output.close();
        return this.bufferPoolDirectory.openInput(fileName, IOContext.DEFAULT);

    }
}

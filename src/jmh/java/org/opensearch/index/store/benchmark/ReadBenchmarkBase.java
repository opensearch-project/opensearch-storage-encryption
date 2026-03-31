/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import static org.opensearch.index.store.CryptoDirectoryFactory.DEFAULT_CRYPTO_PROVIDER;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;
import java.util.Random;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.CryptoDirectoryFactory;
import org.opensearch.index.store.block.RefCountedByteBuffer;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.bufferpoolfs.BufferPoolDirectory;
import org.opensearch.index.store.bufferpoolfs.StaticConfigs;
import org.opensearch.index.store.bufferpoolfs.TestKeyResolver;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.cipher.EncryptionMetadataCacheRegistry;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.pool.PoolBuilder;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry;

/**
 * Base state for read benchmarks. Sets up both BufferPool (encrypted) and MMap directories
 * with a shared test file, and provides range-generation utilities.
 *
 * <p>JMH parameters:
 * <ul>
 *   <li>{@code directoryType}: "bufferpool", "mmap", or "multisegment_mmap_impl"</li>
 *   <li>{@code fileSizeMB}: size of the test file in MB</li>
 * </ul>
 */
@State(Scope.Benchmark)
public class ReadBenchmarkBase {

    @Param({ "bufferpool", "mmap", "multisegment_mmap_impl" })
    public String directoryType;

    @Param({ "32" })
    public int fileSizeMB;

    @Param({ "1" })
    public int numFilesToRead;

    public int sequentialReadNumBytes;

    // ---- internal state ----
    protected Path tempDir;
    protected Path bufferPoolPath;
    protected Path mmapPath;

    protected BufferPoolDirectory bufferPoolDirectory;
    protected MMapDirectory mmapDirectory;
    protected PoolBuilder.PoolResources poolResources;

    protected static final String FILE_NAME_PREFIX = "bench_data";
    protected int fileSize;
    protected byte[] fileData;
    protected IndexInput[] indexInputs;

    protected String[] fileNames;
    private Random random;
    protected long[] blockStartOffsets;
    protected long[] randomReadByteOffsets;
    private int numBlocks;

    /**
     * Initializes directories, writes test files, and generates ranges.
     * Subclasses must call this from their own {@code @Setup(Level.Trial)} method.
     */
    public void setupTrial() throws Exception {
        // Initialize metrics service with no-op registry (normally done during node startup)
        CryptoMetricsService.initialize(NoopMetricsRegistry.INSTANCE);

        final int blockSize = StaticConfigs.CACHE_BLOCK_SIZE;
        fileSize = fileSizeMB * 1024 * 1024 + 3; // Always add a partial block
        numBlocks = (fileSize + blockSize - 1) / blockSize;
        fileData = BenchmarkConfig.buildDeterministicPattern(fileSize);
        random = new Random(BenchmarkConfig.RANGE_SEED);
        sequentialReadNumBytes = random.nextInt(0, blockSize - 3);

        // Allocate arrays now that numFilesToRead and numBlocks are known
        fileNames = new String[numFilesToRead];
        indexInputs = new IndexInput[numFilesToRead];
        blockStartOffsets = new long[numBlocks];

        tempDir = Files.createTempDirectory(Path.of(System.getProperty("java.io.tmpdir")), "jmh-read-bench");
        bufferPoolPath = tempDir.resolve("bufferpool");
        mmapPath = tempDir.resolve("mmap");
        Files.createDirectories(bufferPoolPath);
        Files.createDirectories(mmapPath);

        // Disable write-through cache so cold path tests are meaningful
        CryptoDirectoryFactory.setNodeSettings(Settings.builder().put("node.store.crypto.write_cache_enabled", false).build());
        setBlockStartOffsets();
        setRandomReadByteOffsets();
        generateFileNamesToRead();
        setupBufferPoolDirectory();
        setupMMapDirectory();

        // Release file data after both directories are set up — no longer needed
        fileData = null;
    }

    private void setupBufferPoolDirectory() throws Exception {
        Provider provider = Security.getProvider(DEFAULT_CRYPTO_PROVIDER);
        MasterKeyProvider keyProvider = BenchmarkKeyProvider.create();

        Settings nodeSettings = Settings
            .builder()
            .put("plugins.crypto.enabled", true)
            .put("node.store.crypto.pool_size_percentage", 0.10)
            .put("node.store.crypto.warmup_percentage", 0.0)
            .put("node.store.crypto.cache_to_pool_ratio", 0.8)
            .build();

        this.poolResources = PoolBuilder.build(nodeSettings);
        Pool<RefCountedByteBuffer> segmentPool = poolResources.getSegmentPool();

        String indexUuid = "bench-idx-" + System.nanoTime();
        String indexName = "bench-index";
        FSDirectory fsDir = FSDirectory.open(bufferPoolPath);
        int shardId = 0;

        KeyResolver keyResolver = new TestKeyResolver(indexUuid, indexName, fsDir, provider, keyProvider, shardId);

        EncryptionMetadataCache encMetaCache = EncryptionMetadataCacheRegistry.getOrCreateCache(indexUuid, shardId, indexName);

        BlockLoader<RefCountedByteBuffer> loader = new CryptoDirectIOBlockLoader(segmentPool, keyResolver, encMetaCache);
        Worker worker = poolResources.getSharedReadaheadWorker();

        @SuppressWarnings("unchecked")
        CaffeineBlockCache<RefCountedByteBuffer, RefCountedByteBuffer> sharedCache =
            (CaffeineBlockCache<RefCountedByteBuffer, RefCountedByteBuffer>) poolResources.getBlockCache();

        BlockCache<RefCountedByteBuffer> directoryCache = new CaffeineBlockCache<>(
            sharedCache.getCache(),
            loader,
            poolResources.getMaxCacheBlocks()
        );

        this.bufferPoolDirectory = new BufferPoolDirectory(
            bufferPoolPath,
            FSLockFactory.getDefault(),
            provider,
            keyResolver,
            segmentPool,
            directoryCache,
            loader,
            worker,
            encMetaCache
        );

        // Write test files through bufferpool write path
        // TODO Allow disabling encryption and pure use of bufferpool directory.
        for (String fileName : fileNames) {
            try (IndexOutput out = bufferPoolDirectory.createOutput(fileName, IOContext.DEFAULT)) {
                out.writeBytes(fileData, fileData.length);
            }
        }

        // Only open inputs if this is the active directory type
        if ("bufferpool".equals(directoryType)) {
            for (int i = 0; i < fileNames.length; i++) {
                indexInputs[i] = bufferPoolDirectory.openInput(fileNames[i], IOContext.DEFAULT);
            }
        }
    }

    private void setupMMapDirectory() throws Exception {
        if (directoryType.equals("mmap")) {
            this.mmapDirectory = new MMapDirectory(mmapPath);
        } else {
            this.mmapDirectory = new MMapDirectory(mmapPath, StaticConfigs.CACHE_BLOCK_SIZE);
        }
        // Write identical plaintext data for MMap (no encryption)
        for (String fileName : fileNames) {
            try (IndexOutput out = mmapDirectory.createOutput(fileName, IOContext.DEFAULT)) {
                out.writeBytes(fileData, fileData.length);
            }
        }

        // Only open inputs if this is the active directory type
        if ("mmap".equals(directoryType) || "multisegment_mmap_impl".equals(directoryType)) {
            for (int i = 0; i < fileNames.length; i++) {
                indexInputs[i] = mmapDirectory.openInput(fileNames[i], IOContext.DEFAULT);
            }
        }
    }

    private void generateFileNamesToRead() {
        for (int i = 0; i < numFilesToRead; i++) {
            fileNames[i] = FILE_NAME_PREFIX + i;
        }
    }

    public void tearDownTrial() throws Exception {
        if (bufferPoolDirectory != null)
            bufferPoolDirectory.close();
        if (mmapDirectory != null)
            mmapDirectory.close();
        if (poolResources != null)
            poolResources.close();
        BenchmarkConfig.deleteRecursively(tempDir);
    }

    protected void closeInputs() throws IOException {
        for (IndexInput indexInput : indexInputs) {
            indexInput.close();
        }
    }

    protected void setBlockStartOffsets() {
        final int blockSize = StaticConfigs.CACHE_BLOCK_SIZE;
        for (int block = 0; block < numBlocks; block++) {
            blockStartOffsets[block] = (long) block * blockSize;
        }
    }

    protected void setRandomReadByteOffsets() {
        final int blockSize = StaticConfigs.CACHE_BLOCK_SIZE;
        this.randomReadByteOffsets = new long[numBlocks];
        for (int i = 0; i < numBlocks; i++) {
            int blockBytes = (i == numBlocks - 1) ? Math.max(1, fileSize - (int) blockStartOffsets[i]) : blockSize;
            // Reserve 128 bytes headroom so multi-byte reads (readLong etc.) stay in-block.
            // For tiny last blocks, just use offset 0 within the block.
            int usable = Math.max(1, blockBytes - 128);
            randomReadByteOffsets[i] = blockStartOffsets[i] + random.nextInt(usable);
        }
    }

    protected long[] getRandomReadByteOffsets() {
        return randomReadByteOffsets;
    }

}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block;

import static org.opensearch.index.store.CryptoDirectoryFactory.DEFAULT_CRYPTO_PROVIDER;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.After;
import org.junit.Before;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.CryptoDirectoryFactory;
import org.opensearch.index.store.DummyKeyProvider;
import org.opensearch.index.store.PanamaNativeAccess;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.bufferpoolfs.BufferPoolDirectory;
import org.opensearch.index.store.bufferpoolfs.TestKeyResolver;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.cipher.EncryptionMetadataCacheRegistry;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.pool.PoolBuilder;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * Reproduces the CorruptIndexException caused by the block size mismatch in
 * {@link CryptoDirectIOBlockLoader#load}. The loader copies 32 KB chunks into
 * pool segments that the cache treats as {@code CACHE_BLOCK_SIZE} (4 KB) blocks.
 * This means each "block" in the cache actually contains data from a much larger
 * region, and consecutive cache blocks skip large swaths of the file, causing
 * Lucene to see scrambled bytes.
 *
 * The test writes a deterministic byte pattern through the encrypted write path,
 * reads it back through the cached read path, and verifies byte-level correctness.
 * With the current bug, the read data will diverge from the written data once the
 * first cache block boundary is crossed.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class BlockLoaderCopyMismatchTests extends OpenSearchTestCase {

    private BufferPoolDirectory bufferPoolDirectory;
    private PoolBuilder.PoolResources poolResources;
    private KeyResolver keyResolver;
    private EncryptionMetadataCache encryptionMetadataCache;
    private boolean originalWriteCacheEnabled;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        // Save and disable write-through cache so reads go through the BlockLoader (Direct I/O)
        originalWriteCacheEnabled = CryptoDirectoryFactory.isWriteCacheEnabled();
        CryptoDirectoryFactory.setNodeSettings(Settings.builder().put("node.store.crypto.write_cache_enabled", false).build());
        Path path = createTempDir();
        Provider provider = Security.getProvider(DEFAULT_CRYPTO_PROVIDER);
        MasterKeyProvider keyProvider = DummyKeyProvider.create();

        Settings nodeSettings = Settings
            .builder()
            .put("plugins.crypto.enabled", true)
            .put("node.store.crypto.pool_size_percentage", 0.05)
            .put("node.store.crypto.warmup_percentage", 0.0)
            .put("node.store.crypto.cache_to_pool_ratio", 0.8)
            .build();

        this.poolResources = PoolBuilder.build(nodeSettings);
        Pool<RefCountedMemorySegment> segmentPool = poolResources.getSegmentPool();

        String indexUuid = randomAlphaOfLength(10);
        String indexName = randomAlphaOfLength(10);
        FSDirectory fsDirectory = FSDirectory.open(path);
        int shardId = 0;

        KeyResolver keyResolver = new TestKeyResolver(indexUuid, indexName, fsDirectory, provider, keyProvider, shardId);
        this.keyResolver = keyResolver;

        EncryptionMetadataCache encryptionMetadataCache = EncryptionMetadataCacheRegistry.getOrCreateCache(indexUuid, shardId, indexName);
        this.encryptionMetadataCache = encryptionMetadataCache;

        BlockLoader<RefCountedMemorySegment> loader = new CryptoDirectIOBlockLoader(segmentPool, keyResolver, encryptionMetadataCache);

        Worker worker = poolResources.getSharedReadaheadWorker();

        @SuppressWarnings("unchecked")
        CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> sharedCache =
            (CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment>) poolResources.getBlockCache();

        BlockCache<RefCountedMemorySegment> directoryCache = new CaffeineBlockCache<>(
            sharedCache.getCache(),
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
    public void tearDown() throws Exception {
        PanamaNativeAccess.clearFileSystemBlockSizeOverride();
        CryptoDirectoryFactory
            .setNodeSettings(Settings.builder().put("node.store.crypto.write_cache_enabled", originalWriteCacheEnabled).build());
        this.bufferPoolDirectory.close();
        this.poolResources.close();
        super.tearDown();
    }

    /**
     * Writes a file whose content is a repeating deterministic pattern, then reads
     * it back via the cached IndexInput and asserts byte-level equality.
     *
     * The file size is chosen to span many cache blocks (several multiples of 32 KB)
     * so the 32 KB-vs-CACHE_BLOCK_SIZE copy mismatch in the loader is exercised.
     *
     * Expected failure with the bug:
     *   - readBytes returns wrong data after the first CACHE_BLOCK_SIZE bytes
     *   - The mismatch manifests as skipped regions (28 KB gaps when CACHE_BLOCK_SIZE=4KB)
     */
    public void testSequentialReadIntegrity() throws IOException {
        // Use a size that spans many 32 KB loader chunks to exercise the mismatch.
        // 256 KB = 8 loader chunks of 32 KB = 64 cache blocks of 4 KB
        int fileSize = 256 * 1024 + 37; // +37 to test partial trailing block

        byte[] expected = buildDeterministicPattern(fileSize);

        String fileName = "test_sequential_integrity";
        writeFile(fileName, expected);

        try (IndexInput input = bufferPoolDirectory.openInput(fileName, IOContext.DEFAULT)) {
            assertEquals("IndexInput length must match written bytes", fileSize, input.length());

            byte[] actual = new byte[fileSize];
            input.readBytes(actual, 0, fileSize);

            // Find first mismatch for a clear error message
            for (int i = 0; i < fileSize; i++) {
                if (expected[i] != actual[i]) {
                    fail(
                        "Data corruption at byte offset "
                            + i
                            + " (cache block "
                            + (i / CACHE_BLOCK_SIZE)
                            + ", offset-in-block "
                            + (i % CACHE_BLOCK_SIZE)
                            + ")"
                            + " expected=0x"
                            + String.format("%02X", expected[i] & 0xFF)
                            + " actual=0x"
                            + String.format("%02X", actual[i] & 0xFF)
                    );
                }
            }
        }
    }

    /**
     * Same idea but exercises the slice + clone path, which is how Lucene reads
     * compound files (.cfs). The stacktrace shows corruption inside a .cfs slice,
     * so this directly mirrors the production failure.
     */
    public void testSliceReadIntegrity() throws IOException {
        int fileSize = 512 * 1024 + 13;
        byte[] expected = buildDeterministicPattern(fileSize);

        String fileName = "test_slice_integrity";
        writeFile(fileName, expected);

        try (IndexInput input = bufferPoolDirectory.openInput(fileName, IOContext.DEFAULT)) {
            // Simulate how Lucene opens a sub-file inside a .cfs:
            // slice at an offset that is NOT cache-block-aligned
            int sliceOffset = CACHE_BLOCK_SIZE + 17; // deliberately misaligned
            int sliceLength = 128 * 1024;

            IndexInput slice = input.slice("test-slice", sliceOffset, sliceLength);

            byte[] actual = new byte[sliceLength];
            slice.readBytes(actual, 0, sliceLength);

            for (int i = 0; i < sliceLength; i++) {
                int filePos = sliceOffset + i;
                if (expected[filePos] != actual[i]) {
                    fail(
                        "Slice data corruption at slice offset "
                            + i
                            + " (file offset "
                            + filePos
                            + ")"
                            + " expected=0x"
                            + String.format("%02X", expected[filePos] & 0xFF)
                            + " actual=0x"
                            + String.format("%02X", actual[i] & 0xFF)
                    );
                }
            }
        }
    }

    /**
     * Exercises random-access positional reads (readByte(pos), readInt(pos), readLong(pos))
     * across cache block boundaries. These are used heavily by Lucene's terms dictionary
     * and doc values readers — exactly the codepath in the stacktrace.
     */
    public void testRandomAccessReadIntegrity() throws IOException {
        int fileSize = 256 * 1024;
        byte[] expected = buildDeterministicPattern(fileSize);

        String fileName = "test_random_access_integrity";
        writeFile(fileName, expected);

        try (IndexInput input = bufferPoolDirectory.openInput(fileName, IOContext.DEFAULT)) {
            // Probe at positions that cross 32 KB boundaries (where the bug manifests)
            int[] probeOffsets = {
                0,                     // start of file
                CACHE_BLOCK_SIZE - 1,  // end of first cache block
                CACHE_BLOCK_SIZE,      // start of second cache block (first mismatch with bug)
                CACHE_BLOCK_SIZE * 2,  // third cache block
                32 * 1024,             // start of second 32KB loader chunk
                32 * 1024 + 1,         // one byte into second loader chunk
                64 * 1024,             // third loader chunk
                fileSize - 8,          // near end of file
            };

            for (int offset : probeOffsets) {
                if (offset >= fileSize)
                    continue;

                input.seek(offset);
                byte actualByte = input.readByte();
                assertEquals("readByte mismatch at offset " + offset, expected[offset], actualByte);
            }

            // Also test readBytes at a position that spans a 32KB boundary
            int spanStart = 32 * 1024 - 16;
            int spanLen = 32;
            input.seek(spanStart);
            byte[] spanBuf = new byte[spanLen];
            input.readBytes(spanBuf, 0, spanLen);

            for (int i = 0; i < spanLen; i++) {
                assertEquals("Span read mismatch at file offset " + (spanStart + i), expected[spanStart + i], spanBuf[i]);
            }
        }
    }

    /**
     * Exercises concurrent clone reads, similar to how multiple search threads
     * read from the same IndexInput via clone(). Each clone should see consistent data.
     */
    public void testConcurrentCloneReadIntegrity() throws Exception {
        int fileSize = 256 * 1024;
        byte[] expected = buildDeterministicPattern(fileSize);

        String fileName = "test_concurrent_clone_integrity";
        writeFile(fileName, expected);

        try (IndexInput input = bufferPoolDirectory.openInput(fileName, IOContext.DEFAULT)) {
            int numThreads = 8;
            java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(numThreads);
            java.util.concurrent.atomic.AtomicReference<AssertionError> failure = new java.util.concurrent.atomic.AtomicReference<>();

            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                new Thread(() -> {
                    try {
                        IndexInput clone = input.clone();
                        // Each thread reads a different region
                        int regionStart = threadId * 32 * 1024;
                        int regionLen = Math.min(32 * 1024, fileSize - regionStart);
                        if (regionLen <= 0)
                            return;

                        clone.seek(regionStart);
                        byte[] buf = new byte[regionLen];
                        clone.readBytes(buf, 0, regionLen);

                        for (int i = 0; i < regionLen; i++) {
                            if (expected[regionStart + i] != buf[i]) {
                                failure
                                    .compareAndSet(
                                        null,
                                        new AssertionError(
                                            "Thread "
                                                + threadId
                                                + ": corruption at file offset "
                                                + (regionStart + i)
                                                + " expected=0x"
                                                + String.format("%02X", expected[regionStart + i] & 0xFF)
                                                + " actual=0x"
                                                + String.format("%02X", buf[i] & 0xFF)
                                        )
                                    );
                                return;
                            }
                        }
                    } catch (IOException e) {
                        failure.compareAndSet(null, new AssertionError("Thread " + threadId + " IOException", e));
                    } finally {
                        latch.countDown();
                    }
                }).start();
            }

            latch.await();
            if (failure.get() != null) {
                throw failure.get();
            }
        }
    }

    /**
     * Directly tests the BlockLoader with blockCount > 1, which is the exact path
     * used by readahead/prefetch. This is where the 32KB-vs-CACHE_BLOCK_SIZE mismatch
     * causes an IndexOutOfBoundsException because the loader tries to copy 32KB
     * (the hardcoded blockSize) into a pool segment that is only CACHE_BLOCK_SIZE bytes.
     *
     * This reproduces the production failure: readahead loads multiple blocks, the copy
     * loop uses the wrong chunk size, and the MemorySegment.copy overflows the pool segment.
     */
    public void testBlockLoaderMultiBlockLoadCorruption() throws Exception {
        // First, write a file through the encrypted write path
        int fileSize = 256 * 1024;
        byte[] expected = buildDeterministicPattern(fileSize);

        String fileName = "test_loader_multiblock";
        writeFile(fileName, expected);

        // Now directly invoke the loader with blockCount > 1, simulating readahead
        Path filePath = bufferPoolDirectory.getDirectory().resolve(fileName);

        // Request 2 cache blocks (2 * CACHE_BLOCK_SIZE = 8KB with power=12)
        // The loader will try to copy min(32768, 8192) = 8192 bytes into a 4096-byte segment
        int blockCount = 2;
        try {
            Pool<RefCountedMemorySegment> pool = poolResources.getSegmentPool();

            BlockLoader<RefCountedMemorySegment> loader = new CryptoDirectIOBlockLoader(pool, keyResolver, encryptionMetadataCache);

            RefCountedMemorySegment[] blocks = loader.load(filePath, 0, blockCount, 5000);

            // If we get here without exception, verify the data is correct
            for (int b = 0; b < blockCount; b++) {
                assertNotNull("Block " + b + " should not be null", blocks[b]);
                MemorySegment seg = blocks[b].segment();

                int blockStart = b * CACHE_BLOCK_SIZE;
                for (int i = 0; i < CACHE_BLOCK_SIZE && (blockStart + i) < fileSize; i++) {
                    byte actual = seg.get(LAYOUT_BYTE, i);
                    assertEquals(
                        "Block " + b + " corruption at offset " + i + " (file offset " + (blockStart + i) + ")",
                        expected[blockStart + i],
                        actual
                    );
                }

                blocks[b].close();
            }
        } catch (IndexOutOfBoundsException e) {
            // This is the expected failure with the bug: the loader tries to copy
            // 32KB (hardcoded blockSize) into a CACHE_BLOCK_SIZE (4KB) pool segment.
            // The error message will show something like:
            // "Out of bound access on segment ... limit: 4096; new length = 8192"
            logger.info("Caught expected IndexOutOfBoundsException from multi-block load: {}", e.getMessage());
            fail(
                "BlockLoader multi-block load failed with IndexOutOfBoundsException. "
                    + "The loader's copy loop uses hardcoded blockSize (32KB) instead of CACHE_BLOCK_SIZE ("
                    + CACHE_BLOCK_SIZE
                    + " bytes). Error: "
                    + e.getMessage()
            );
        }
    }

    // ---- FS block size vs cache block size mismatch tests ----

    /**
     * Simulates a filesystem whose block size (16 KB) is larger than the cache block
     * size (8 KB). A single cache block is loaded and verified for byte-level correctness.
     * This exercises the alignment path in {@code directIOReadAligned} where the I/O
     * alignment exceeds the amount of data the cache actually needs per block.
     */
    public void testSingleBlockLoadWithLargerFsBlockSize() throws Exception {
        PanamaNativeAccess.setFileSystemBlockSizeOverride(16384); // 16 KB > CACHE_BLOCK_SIZE (8 KB)

        int fileSize = CACHE_BLOCK_SIZE * 4; // 32 KB — enough to have multiple blocks on disk
        byte[] expected = buildDeterministicPattern(fileSize);

        String fileName = "test_single_block_large_fs";
        writeFile(fileName, expected);

        Path filePath = bufferPoolDirectory.getDirectory().resolve(fileName);
        Pool<RefCountedMemorySegment> pool = poolResources.getSegmentPool();
        BlockLoader<RefCountedMemorySegment> loader = new CryptoDirectIOBlockLoader(pool, keyResolver, encryptionMetadataCache);

        RefCountedMemorySegment[] blocks = loader.load(filePath, 0, 1, 5000);
        try {
            assertNotNull("Block 0 should not be null", blocks[0]);
            MemorySegment seg = blocks[0].segment();
            for (int i = 0; i < CACHE_BLOCK_SIZE; i++) {
                byte actual = seg.get(LAYOUT_BYTE, i);
                assertEquals("Corruption at offset " + i + " with larger FS block size", expected[i], actual);
            }
        } finally {
            for (RefCountedMemorySegment b : blocks) {
                if (b != null)
                    b.close();
            }
        }
    }

    /**
     * Simulates a filesystem whose block size (32 KB) is larger than the cache block
     * size (8 KB) and loads multiple cache blocks in one call. This is the readahead /
     * prefetch path where the loader copies data into several pool segments.
     */
    public void testMultiBlockLoadWithLargerFsBlockSize() throws Exception {
        PanamaNativeAccess.setFileSystemBlockSizeOverride(32768); // 32 KB > CACHE_BLOCK_SIZE (8 KB)

        int blockCount = 4;
        int fileSize = CACHE_BLOCK_SIZE * blockCount * 2; // plenty of data
        byte[] expected = buildDeterministicPattern(fileSize);

        String fileName = "test_multi_block_large_fs";
        writeFile(fileName, expected);

        Path filePath = bufferPoolDirectory.getDirectory().resolve(fileName);
        Pool<RefCountedMemorySegment> pool = poolResources.getSegmentPool();
        BlockLoader<RefCountedMemorySegment> loader = new CryptoDirectIOBlockLoader(pool, keyResolver, encryptionMetadataCache);

        RefCountedMemorySegment[] blocks = loader.load(filePath, 0, blockCount, 5000);
        try {
            for (int b = 0; b < blockCount; b++) {
                assertNotNull("Block " + b + " should not be null", blocks[b]);
                MemorySegment seg = blocks[b].segment();
                int blockStart = b * CACHE_BLOCK_SIZE;
                for (int i = 0; i < CACHE_BLOCK_SIZE && (blockStart + i) < fileSize; i++) {
                    byte actual = seg.get(LAYOUT_BYTE, i);
                    assertEquals(
                        "Block " + b + " corruption at offset " + i + " (file offset " + (blockStart + i) + ") with 32KB FS block size",
                        expected[blockStart + i],
                        actual
                    );
                }
            }
        } finally {
            for (RefCountedMemorySegment b : blocks) {
                if (b != null)
                    b.close();
            }
        }
    }

    /**
     * Simulates a filesystem whose block size (4 KB) is smaller than the cache block
     * size (8 KB). A single cache block is loaded via the {@link CryptoDirectIOBlockLoader}
     * and verified for byte-level correctness. This exercises the alignment path where
     * the I/O alignment is finer-grained than the cache block granularity.
     *
     * <p>Note: The override must be ≥ the real filesystem block size for Direct I/O to
     * succeed (the OS kernel enforces the real block size). On APFS/ext4 this is 4096.
     */
    public void testSingleBlockLoadWithSmallerFsBlockSize() throws Exception {
        PanamaNativeAccess.setFileSystemBlockSizeOverride(4096); // 4 KB < CACHE_BLOCK_SIZE (8 KB)

        int fileSize = CACHE_BLOCK_SIZE * 4;
        byte[] expected = buildDeterministicPattern(fileSize);

        String fileName = "test_single_block_small_fs";
        writeFile(fileName, expected);

        Path filePath = bufferPoolDirectory.getDirectory().resolve(fileName);
        Pool<RefCountedMemorySegment> pool = poolResources.getSegmentPool();
        BlockLoader<RefCountedMemorySegment> loader = new CryptoDirectIOBlockLoader(pool, keyResolver, encryptionMetadataCache);

        RefCountedMemorySegment[] blocks = loader.load(filePath, 0, 1, 5000);
        try {
            assertNotNull("Block 0 should not be null", blocks[0]);
            MemorySegment seg = blocks[0].segment();
            for (int i = 0; i < CACHE_BLOCK_SIZE; i++) {
                byte actual = seg.get(LAYOUT_BYTE, i);
                assertEquals("Corruption at offset " + i + " with 4KB FS block size", expected[i], actual);
            }
        } finally {
            for (RefCountedMemorySegment b : blocks) {
                if (b != null)
                    b.close();
            }
        }
    }

    /**
     * Simulates a filesystem whose block size (4 KB) is smaller than the cache block
     * size (8 KB) and loads multiple cache blocks via the {@link CryptoDirectIOBlockLoader}.
     * This mirrors common ext4/xfs setups where the FS block size is 4096 but the cache
     * operates at 8192-byte granularity.
     */
    public void testMultiBlockLoadWithSmallerFsBlockSize() throws Exception {
        PanamaNativeAccess.setFileSystemBlockSizeOverride(4096); // 4 KB < CACHE_BLOCK_SIZE (8 KB)

        int blockCount = 4;
        int fileSize = CACHE_BLOCK_SIZE * blockCount * 2;
        byte[] expected = buildDeterministicPattern(fileSize);

        String fileName = "test_multi_block_small_fs";
        writeFile(fileName, expected);

        Path filePath = bufferPoolDirectory.getDirectory().resolve(fileName);
        Pool<RefCountedMemorySegment> pool = poolResources.getSegmentPool();
        BlockLoader<RefCountedMemorySegment> loader = new CryptoDirectIOBlockLoader(pool, keyResolver, encryptionMetadataCache);

        RefCountedMemorySegment[] blocks = loader.load(filePath, 0, blockCount, 5000);
        try {
            for (int b = 0; b < blockCount; b++) {
                assertNotNull("Block " + b + " should not be null", blocks[b]);
                MemorySegment seg = blocks[b].segment();
                int blockStart = b * CACHE_BLOCK_SIZE;
                for (int i = 0; i < CACHE_BLOCK_SIZE && (blockStart + i) < fileSize; i++) {
                    byte actual = seg.get(LAYOUT_BYTE, i);
                    assertEquals(
                        "Block " + b + " corruption at offset " + i + " (file offset " + (blockStart + i) + ") with 4KB FS block size",
                        expected[blockStart + i],
                        actual
                    );
                }
            }
        } finally {
            for (RefCountedMemorySegment b : blocks) {
                if (b != null)
                    b.close();
            }
        }
    }

    // ---- helpers ----

    private static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;

    private void writeFile(String name, byte[] data) throws IOException {
        try (IndexOutput output = bufferPoolDirectory.createOutput(name, IOContext.DEFAULT)) {
            output.writeBytes(data, data.length);
        }
    }

    /**
     * Builds a byte array where each byte is deterministic based on its position.
     * This makes it trivial to verify correctness: expected[i] = pattern(i).
     * The pattern is designed so that adjacent cache blocks have visibly different
     * content, making the 32KB skip immediately detectable.
     */
    private byte[] buildDeterministicPattern(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            // Mix position bits so that bytes at offset N and offset N+32KB are different
            data[i] = (byte) ((i * 31 + (i >>> 8) * 7 + (i >>> 16) * 13) & 0xFF);
        }
        return data;
    }
}

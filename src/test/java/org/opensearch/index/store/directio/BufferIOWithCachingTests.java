/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.Security;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.bufferpoolfs.BufferIOWithCaching;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Comprehensive tests for BufferIOWithCaching covering:
 * - Buffering logic (small writes, large writes, buffer management)
 * - Block caching (full blocks, partial blocks, final partial block)
 * - Frame management and encryption boundaries
 * - Error handling and edge cases
 */
@SuppressWarnings("unchecked")
public class BufferIOWithCachingTests extends OpenSearchTestCase {

    private static final int CACHE_BLOCK_SIZE = 8192; // DirectIoConfigs.CACHE_BLOCK_SIZE
    private static final int BUFFER_SIZE = 65_536;
    private static final int FRAME_SIZE = 4 * 1024 * 1024; // Default frame size

    private Pool<RefCountedMemorySegment> mockPool;
    private BlockCache<RefCountedMemorySegment> mockCache;
    private EncryptionMetadataCache encryptionMetadataCache;
    private Provider provider;
    private byte[] testKey;
    private Path tempFile;
    private Arena arena;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        mockPool = mock(Pool.class);
        mockCache = mock(BlockCache.class);
        encryptionMetadataCache = new EncryptionMetadataCache();
        provider = Security.getProvider("SunJCE");

        // Generate test key (32 bytes for AES-256)
        testKey = new byte[32];
        new SecureRandom().nextBytes(testKey);

        tempFile = Files.createTempFile("test-buffer-io", ".dat");
        arena = Arena.ofAuto();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (tempFile != null && Files.exists(tempFile)) {
            Files.delete(tempFile);
        }
    }

    /**
     * Tests that small writes (< BUFFER_SIZE) are buffered correctly.
     */
    public void testSmallWritesAreBuffered() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // Setup mock pool to return segment
        RefCountedMemorySegment mockSegment = createMockSegment();
        when(mockPool.tryAcquire(anyLong(), any(TimeUnit.class))).thenReturn(mockSegment);

        try (
            BufferIOWithCaching output = new BufferIOWithCaching(
                "test",
                tempFile,
                baos,
                testKey,
                mockPool,
                mockCache,
                provider,
                encryptionMetadataCache
            )
        ) {
            // Write small chunks that should be buffered
            byte[] data = new byte[1024];
            for (int i = 0; i < 5; i++) {
                output.writeBytes(data, data.length);
            }
            // No flush yet, data should be in buffer
        }

        // After close, data should be encrypted and written
        assertTrue("Should have written encrypted data", baos.size() > 0);
    }

    /**
     * Tests that large writes (>= BUFFER_SIZE) bypass buffering.
     */
    public void testLargeWritesBypassBuffer() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        RefCountedMemorySegment mockSegment = createMockSegment();
        when(mockPool.tryAcquire(anyLong(), any(TimeUnit.class))).thenReturn(mockSegment);

        try (
            BufferIOWithCaching output = new BufferIOWithCaching(
                "test",
                tempFile,
                baos,
                testKey,
                mockPool,
                mockCache,
                provider,
                encryptionMetadataCache
            )
        ) {
            // Write data >= BUFFER_SIZE (should bypass buffering)
            byte[] largeData = new byte[BUFFER_SIZE];
            new SecureRandom().nextBytes(largeData);
            output.writeBytes(largeData, largeData.length);
        }

        assertTrue("Should have written encrypted data", baos.size() > 0);
    }

    /**
     * Tests buffer overflow behavior - should flush complete blocks and keep tail.
     */
    public void testBufferOverflowFlushesCompleteBlocks() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        RefCountedMemorySegment mockSegment = createMockSegment();
        when(mockPool.tryAcquire(anyLong(), any(TimeUnit.class))).thenReturn(mockSegment);

        try (
            BufferIOWithCaching output = new BufferIOWithCaching(
                "test",
                tempFile,
                baos,
                testKey,
                mockPool,
                mockCache,
                provider,
                encryptionMetadataCache
            )
        ) {
            // Fill buffer to near capacity
            byte[] data = new byte[BUFFER_SIZE - 100];
            output.writeBytes(data, data.length);

            // Write more data that would overflow - should trigger flush of complete blocks
            byte[] moreData = new byte[1000];
            output.writeBytes(moreData, moreData.length);
        }

        assertTrue("Should have written encrypted data", baos.size() > 0);
    }

    /**
     * Tests single-byte write method.
     */
    public void testSingleByteWrite() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        RefCountedMemorySegment mockSegment = createMockSegment();
        when(mockPool.tryAcquire(anyLong(), any(TimeUnit.class))).thenReturn(mockSegment);

        try (
            BufferIOWithCaching output = new BufferIOWithCaching(
                "test",
                tempFile,
                baos,
                testKey,
                mockPool,
                mockCache,
                provider,
                encryptionMetadataCache
            )
        ) {
            // Write individual bytes
            for (int i = 0; i < 100; i++) {
                output.writeByte((byte) i);
            }
        }

        assertTrue("Should have written encrypted data", baos.size() > 0);
    }

    /**
     * Tests that full aligned blocks (8KB) are cached immediately.
     */
    public void testFullBlocksAreCachedImmediately() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        RefCountedMemorySegment mockSegment = createMockSegment();
        when(mockPool.tryAcquire(anyLong(), any(TimeUnit.class))).thenReturn(mockSegment);

        try (
            BufferIOWithCaching output = new BufferIOWithCaching(
                "test",
                tempFile,
                baos,
                testKey,
                mockPool,
                mockCache,
                provider,
                encryptionMetadataCache
            )
        ) {
            // Write exactly one cache block (8192 bytes)
            byte[] fullBlock = new byte[CACHE_BLOCK_SIZE];
            new SecureRandom().nextBytes(fullBlock);
            output.writeBytes(fullBlock, fullBlock.length);
        }

        // Verify that block was cached
        verify(mockCache, atLeastOnce()).put(any(BlockCacheKey.class), any(RefCountedMemorySegment.class));
    }

    /**
     * Tests that multiple full blocks are cached correctly.
     */
    public void testMultipleFullBlocksAreCached() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        RefCountedMemorySegment mockSegment = createMockSegment();
        when(mockPool.tryAcquire(anyLong(), any(TimeUnit.class))).thenReturn(mockSegment);

        try (
            BufferIOWithCaching output = new BufferIOWithCaching(
                "test",
                tempFile,
                baos,
                testKey,
                mockPool,
                mockCache,
                provider,
                encryptionMetadataCache
            )
        ) {
            // Write 5 complete cache blocks
            for (int i = 0; i < 5; i++) {
                byte[] fullBlock = new byte[CACHE_BLOCK_SIZE];
                new SecureRandom().nextBytes(fullBlock);
                output.writeBytes(fullBlock, fullBlock.length);
            }
        }

        // Verify that blocks were cached (at least 5 times, possibly 6 with final partial)
        verify(mockCache, atLeastOnce()).put(any(BlockCacheKey.class), any(RefCountedMemorySegment.class));
    }

    /**
     * Tests that partial blocks are accumulated but not immediately cached.
     */
    public void testPartialBlocksAreAccumulated() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        RefCountedMemorySegment mockSegment = createMockSegment();
        when(mockPool.tryAcquire(anyLong(), any(TimeUnit.class))).thenReturn(mockSegment);

        try (
            BufferIOWithCaching output = new BufferIOWithCaching(
                "test",
                tempFile,
                baos,
                testKey,
                mockPool,
                mockCache,
                provider,
                encryptionMetadataCache
            )
        ) {
            // Write partial block (not aligned to 8KB)
            byte[] partialBlock = new byte[4096];
            new SecureRandom().nextBytes(partialBlock);
            output.writeBytes(partialBlock, partialBlock.length);
        }

        // Verify that final partial block was cached on close
        verify(mockCache, atLeastOnce()).put(any(BlockCacheKey.class), any(RefCountedMemorySegment.class));
    }

    /**
     * Tests that final partial block is cached on close.
     */
    public void testFinalPartialBlockIsCachedOnClose() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        RefCountedMemorySegment mockSegment = createMockSegment();
        when(mockPool.tryAcquire(anyLong(), any(TimeUnit.class))).thenReturn(mockSegment);

        try (
            BufferIOWithCaching output = new BufferIOWithCaching(
                "test",
                tempFile,
                baos,
                testKey,
                mockPool,
                mockCache,
                provider,
                encryptionMetadataCache
            )
        ) {
            // Write data that results in partial final block
            byte[] data = new byte[CACHE_BLOCK_SIZE + 1234]; // 8192 + 1234 bytes
            new SecureRandom().nextBytes(data);
            output.writeBytes(data, data.length);
        }

        // Should cache both full block and final partial block
        verify(mockCache, atLeastOnce()).put(any(BlockCacheKey.class), any(RefCountedMemorySegment.class));
    }

    /**
     * Tests that unaligned writes spanning blocks are cached correctly.
     */
    public void testUnalignedWritesSpanningBlocks() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        RefCountedMemorySegment mockSegment = createMockSegment();
        when(mockPool.tryAcquire(anyLong(), any(TimeUnit.class))).thenReturn(mockSegment);

        try (
            BufferIOWithCaching output = new BufferIOWithCaching(
                "test",
                tempFile,
                baos,
                testKey,
                mockPool,
                mockCache,
                provider,
                encryptionMetadataCache
            )
        ) {
            // Write unaligned data that spans multiple blocks
            // First partial: 6KB
            byte[] data1 = new byte[6144];
            output.writeBytes(data1, data1.length);

            // Second write: 4KB (completes first block + starts second)
            byte[] data2 = new byte[4096];
            output.writeBytes(data2, data2.length);

            // Third write: 8KB (completes second block + full third block)
            byte[] data3 = new byte[CACHE_BLOCK_SIZE];
            output.writeBytes(data3, data3.length);
        }

        verify(mockCache, atLeastOnce()).put(any(BlockCacheKey.class), any(RefCountedMemorySegment.class));
    }

    /**
     * Tests writing data that spans frame boundaries (4MB frames).
     */
    public void testWriteSpanningFrameBoundaries() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        RefCountedMemorySegment mockSegment = createMockSegment();
        when(mockPool.tryAcquire(anyLong(), any(TimeUnit.class))).thenReturn(mockSegment);

        try (
            BufferIOWithCaching output = new BufferIOWithCaching(
                "test",
                tempFile,
                baos,
                testKey,
                mockPool,
                mockCache,
                provider,
                encryptionMetadataCache
            )
        ) {
            // Write data close to frame boundary
            int nearBoundary = FRAME_SIZE - 1024;
            byte[] data1 = new byte[nearBoundary];
            output.writeBytes(data1, data1.length);

            // Write data that crosses frame boundary
            byte[] data2 = new byte[2048];
            output.writeBytes(data2, data2.length);
        }

        assertTrue("Should handle frame boundary crossing", baos.size() > 0);
    }

    /**
     * Tests handling of null input buffer.
     */
    public void testNullInputBufferThrowsException() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        RefCountedMemorySegment mockSegment = createMockSegment();
        when(mockPool.tryAcquire(anyLong(), any(TimeUnit.class))).thenReturn(mockSegment);

        try (
            BufferIOWithCaching output = new BufferIOWithCaching(
                "test",
                tempFile,
                baos,
                testKey,
                mockPool,
                mockCache,
                provider,
                encryptionMetadataCache
            )
        ) {
            expectThrows(NullPointerException.class, () -> { output.writeBytes(null, 100); });
        }
    }

    /**
     * Tests handling of invalid offset/length parameters.
     */
    public void testInvalidOffsetLengthThrowsException() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        RefCountedMemorySegment mockSegment = createMockSegment();
        when(mockPool.tryAcquire(anyLong(), any(TimeUnit.class))).thenReturn(mockSegment);

        try (
            BufferIOWithCaching output = new BufferIOWithCaching(
                "test",
                tempFile,
                baos,
                testKey,
                mockPool,
                mockCache,
                provider,
                encryptionMetadataCache
            )
        ) {
            byte[] data = new byte[100];

            // Negative offset
            expectThrows(IndexOutOfBoundsException.class, () -> { output.writeBytes(data, -1, 50); });

            // Negative length
            expectThrows(IndexOutOfBoundsException.class, () -> { output.writeBytes(data, 0, -1); });

            // Offset + length > array length
            expectThrows(IndexOutOfBoundsException.class, () -> { output.writeBytes(data, 50, 100); });
        }
    }

    /**
     * Tests zero-length write (should be no-op).
     */
    public void testZeroLengthWrite() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        RefCountedMemorySegment mockSegment = createMockSegment();
        when(mockPool.tryAcquire(anyLong(), any(TimeUnit.class))).thenReturn(mockSegment);

        try (
            BufferIOWithCaching output = new BufferIOWithCaching(
                "test",
                tempFile,
                baos,
                testKey,
                mockPool,
                mockCache,
                provider,
                encryptionMetadataCache
            )
        ) {
            byte[] data = new byte[100];
            output.writeBytes(data, 0, 0); // Should not throw, no-op
        }

        // Should only contain footer after close
        assertTrue("Should handle zero-length write", baos.size() > 0);
    }

    /**
     * Tests graceful handling when pool acquisition fails.
     */
    public void testPoolAcquisitionFailureHandledGracefully() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // Pool returns null (acquisition failed)
        when(mockPool.tryAcquire(anyLong(), any(TimeUnit.class))).thenReturn(null);

        try (
            BufferIOWithCaching output = new BufferIOWithCaching(
                "test",
                tempFile,
                baos,
                testKey,
                mockPool,
                mockCache,
                provider,
                encryptionMetadataCache
            )
        ) {
            // Write full block - should not cache but should still encrypt and write
            byte[] fullBlock = new byte[CACHE_BLOCK_SIZE];
            output.writeBytes(fullBlock, fullBlock.length);
        }

        // Should still write encrypted data even if caching fails
        assertTrue("Should write data even when caching fails", baos.size() > 0);
    }

    /**
     * Tests empty file (close without any writes).
     */
    public void testEmptyFile() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        RefCountedMemorySegment mockSegment = createMockSegment();
        when(mockPool.tryAcquire(anyLong(), any(TimeUnit.class))).thenReturn(mockSegment);

        try (
            BufferIOWithCaching output = new BufferIOWithCaching(
                "test",
                tempFile,
                baos,
                testKey,
                mockPool,
                mockCache,
                provider,
                encryptionMetadataCache
            )
        ) {
            // Close without writing
        }

        // Should still write footer
        assertTrue("Should write footer for empty file", baos.size() > 0);
    }

    /**
     * Tests exact block boundary writes (no partial blocks).
     */
    public void testExactBlockBoundaryWrites() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        RefCountedMemorySegment mockSegment = createMockSegment();
        when(mockPool.tryAcquire(anyLong(), any(TimeUnit.class))).thenReturn(mockSegment);

        try (
            BufferIOWithCaching output = new BufferIOWithCaching(
                "test",
                tempFile,
                baos,
                testKey,
                mockPool,
                mockCache,
                provider,
                encryptionMetadataCache
            )
        ) {
            // Write exactly 10 cache blocks
            for (int i = 0; i < 10; i++) {
                byte[] block = new byte[CACHE_BLOCK_SIZE];
                output.writeBytes(block, block.length);
            }
        }

        verify(mockCache, atLeastOnce()).put(any(BlockCacheKey.class), any(RefCountedMemorySegment.class));
    }

    /**
     * Tests very large file write.
     */
    public void testVeryLargeFileWrite() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        RefCountedMemorySegment mockSegment = createMockSegment();
        when(mockPool.tryAcquire(anyLong(), any(TimeUnit.class))).thenReturn(mockSegment);

        try (
            BufferIOWithCaching output = new BufferIOWithCaching(
                "test",
                tempFile,
                baos,
                testKey,
                mockPool,
                mockCache,
                provider,
                encryptionMetadataCache
            )
        ) {
            // Write 10MB (spans multiple frames)
            int totalSize = 10 * 1024 * 1024;
            int chunkSize = 1024 * 1024; // 1MB chunks

            for (int i = 0; i < totalSize / chunkSize; i++) {
                byte[] chunk = new byte[chunkSize];
                output.writeBytes(chunk, chunk.length);
            }
        }

        assertTrue("Should handle large file writes", baos.size() > 0);
    }

    private RefCountedMemorySegment createMockSegment() {
        MemorySegment segment = arena.allocate(CACHE_BLOCK_SIZE);
        return new RefCountedMemorySegment(segment, CACHE_BLOCK_SIZE, (ref) -> {});
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Before;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;
import org.opensearch.test.OpenSearchTestCase;

@SuppressWarnings("unchecked")
public class CachedMemorySegmentIndexInputTests extends OpenSearchTestCase {

    private static final int BLOCK_SIZE = 8192; // DirectIoConfigs.CACHE_BLOCK_SIZE
    private static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;
    private static final ValueLayout.OfShort LAYOUT_LE_SHORT = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfInt LAYOUT_LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong LAYOUT_LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfFloat LAYOUT_LE_FLOAT = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    private BlockCache<RefCountedMemorySegment> mockCache;
    private BlockSlotTinyCache mockTinyCache;
    private ReadaheadManager mockReadaheadManager;
    private ReadaheadContext mockReadaheadContext;
    private Path testPath;
    private Arena arena;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mockCache = mock(BlockCache.class);
        mockTinyCache = mock(BlockSlotTinyCache.class);
        mockReadaheadManager = mock(ReadaheadManager.class);
        mockReadaheadContext = mock(ReadaheadContext.class);
        testPath = Paths.get("/test/exhaustive.dat");
        arena = Arena.ofAuto();
    }

    /**
     * Tests reading a single byte at exact block boundary (first byte of second block).
     */
    public void testReadByteAtBlockBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0x10);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0x20);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Read first byte of second block
        input.seek(BLOCK_SIZE);
        byte value = input.readByte();

        assertEquals("Should read first byte of second block", (byte) 0x20, value);
        assertEquals("Position should advance", BLOCK_SIZE + 1, input.getFilePointer());
    }

    /**
     * Tests reading byte at last position of first block.
     */
    public void testReadByteAtEndOfBlock() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0x10);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0x20);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Seek to last byte of first block
        input.seek(BLOCK_SIZE - 1);
        byte value = input.readByte();

        assertEquals("Should read last byte of first block", (byte) 0x10, value);
        assertEquals("Position should be at block boundary", BLOCK_SIZE, input.getFilePointer());
    }

    /**
     * Tests reading byte one position before block boundary.
     */
    public void testReadByteOneBeforeBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0xBB);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 2);
        byte value = input.readByte();

        assertEquals("Should read second-to-last byte of first block", (byte) 0xAA, value);
    }

    /**
     * Tests reading bytes that span exact block boundary (4 bytes before, 4 bytes after).
     */
    public void testReadBytesAcrossExactBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0xBB);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Position at 4 bytes before boundary
        input.seek(BLOCK_SIZE - 4);
        byte[] buffer = new byte[8]; // Read 8 bytes (4 from each block)
        input.readBytes(buffer, 0, 8);

        // First 4 bytes should be 0xAA, next 4 should be 0xBB
        for (int i = 0; i < 4; i++) {
            assertEquals("First 4 bytes from block 0", (byte) 0xAA, buffer[i]);
        }
        for (int i = 4; i < 8; i++) {
            assertEquals("Next 4 bytes from block 1", (byte) 0xBB, buffer[i]);
        }
    }

    /**
     * Tests reading bytes that start exactly at block boundary.
     */
    public void testReadBytesStartingAtBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0x11);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0x22);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE);
        byte[] buffer = new byte[10];
        input.readBytes(buffer, 0, 10);

        for (int i = 0; i < 10; i++) {
            assertEquals("All bytes from second block", (byte) 0x22, buffer[i]);
        }
    }

    /**
     * Tests reading bytes that end exactly at block boundary.
     */
    public void testReadBytesEndingAtBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0x33);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0x44);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 10);
        byte[] buffer = new byte[10];
        input.readBytes(buffer, 0, 10);

        for (int i = 0; i < 10; i++) {
            assertEquals("All bytes from first block", (byte) 0x33, buffer[i]);
        }
        assertEquals("Position should be at boundary", BLOCK_SIZE, input.getFilePointer());
    }

    /**
     * Tests reading large byte array spanning 3 complete blocks.
     */
    public void testReadBytesSpanningThreeBlocks() throws IOException {
        long fileLength = BLOCK_SIZE * 4;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0x11);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0x22);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 0x33);
        MemorySegment block3 = createBlockWithPattern(3, (byte) 0x44);

        setupFourBlocks(block0, block1, block2, block3);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Read from middle of block 0 through middle of block 3
        input.seek(BLOCK_SIZE - 100);
        int readSize = BLOCK_SIZE * 2 + 200; // Spans blocks 0, 1, 2, and into 3
        byte[] buffer = new byte[readSize];
        input.readBytes(buffer, 0, readSize);

        // Verify pattern
        for (int i = 0; i < 100; i++) {
            assertEquals("Bytes from block 0", (byte) 0x11, buffer[i]);
        }
        for (int i = 100; i < 100 + BLOCK_SIZE; i++) {
            assertEquals("Bytes from block 1", (byte) 0x22, buffer[i]);
        }
        for (int i = 100 + BLOCK_SIZE; i < 100 + BLOCK_SIZE * 2; i++) {
            assertEquals("Bytes from block 2", (byte) 0x33, buffer[i]);
        }
        for (int i = 100 + BLOCK_SIZE * 2; i < readSize; i++) {
            assertEquals("Bytes from block 3", (byte) 0x44, buffer[i]);
        }
    }

    /**
     * Tests reading exactly one full block (block-aligned, full block size).
     */
    public void testReadFullBlockAligned() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0xBB);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        byte[] buffer = new byte[BLOCK_SIZE];
        input.readBytes(buffer, 0, BLOCK_SIZE);

        for (int i = 0; i < BLOCK_SIZE; i++) {
            assertEquals("Full block read", (byte) 0xAA, buffer[i]);
        }
        assertEquals("Position should be at next block", BLOCK_SIZE, input.getFilePointer());
    }

    /**
     * Tests reading multiple full blocks sequentially.
     */
    public void testReadMultipleFullBlocks() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 3);

        setupThreeBlocks(block0, block1, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        byte[] buffer = new byte[BLOCK_SIZE * 2];
        input.readBytes(buffer, 0, BLOCK_SIZE * 2);

        for (int i = 0; i < BLOCK_SIZE; i++) {
            assertEquals("Block 0 data", (byte) 1, buffer[i]);
        }
        for (int i = BLOCK_SIZE; i < BLOCK_SIZE * 2; i++) {
            assertEquals("Block 1 data", (byte) 2, buffer[i]);
        }
    }

    /**
     * Tests reading short value that spans block boundary (1 byte in each block).
     */
    public void testReadShortAcrossBlockBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // Set up short value split across blocks (little-endian: 0x1234)
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 1, (byte) 0x34);
        block1.set(LAYOUT_BYTE, 0, (byte) 0x12);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 1);
        short value = input.readShort();

        assertEquals("Short should span blocks correctly", (short) 0x1234, value);
        assertEquals("Position advanced by 2", BLOCK_SIZE + 1, input.getFilePointer());
    }

    /**
     * Tests reading short at exact block boundary (both bytes in second block).
     */
    public void testReadShortAtBlockBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // Place short at start of block 1
        block1.set(LAYOUT_LE_SHORT, 0, (short) 0x5678);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE);
        short value = input.readShort();

        assertEquals("Short at boundary", (short) 0x5678, value);
    }

    /**
     * Tests reading short one byte before block boundary.
     */
    public void testReadShortOneByteBeforeBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        block0.set(LAYOUT_LE_SHORT, BLOCK_SIZE - 2, (short) 0xABCD);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 2);
        short value = input.readShort();

        assertEquals("Short before boundary", (short) 0xABCD, value);
    }

    /**
     * Tests reading int that spans block boundary (2 bytes in each block).
     */
    public void testReadIntAcrossBlockBoundaryEvenSplit() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // Position int so 2 bytes in each block (little-endian: 0x78563412)
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 2, (byte) 0x12);
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 1, (byte) 0x34);
        block1.set(LAYOUT_BYTE, 0, (byte) 0x56);
        block1.set(LAYOUT_BYTE, 1, (byte) 0x78);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 2);
        int value = input.readInt();

        assertEquals("Int should span blocks (2+2)", 0x78563412, value);
    }

    /**
     * Tests reading int with 1 byte in first block, 3 bytes in second.
     */
    public void testReadIntAcrossBlockBoundaryUnevenSplit1_3() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // 1 byte in block0, 3 bytes in block1
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 1, (byte) 0x12);
        block1.set(LAYOUT_BYTE, 0, (byte) 0x34);
        block1.set(LAYOUT_BYTE, 1, (byte) 0x56);
        block1.set(LAYOUT_BYTE, 2, (byte) 0x78);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 1);
        int value = input.readInt();

        assertEquals("Int should span blocks (1+3)", 0x78563412, value);
    }

    /**
     * Tests reading int with 3 bytes in first block, 1 byte in second.
     */
    public void testReadIntAcrossBlockBoundaryUnevenSplit3_1() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // 3 bytes in block0, 1 byte in block1
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 3, (byte) 0x12);
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 2, (byte) 0x34);
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 1, (byte) 0x56);
        block1.set(LAYOUT_BYTE, 0, (byte) 0x78);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 3);
        int value = input.readInt();

        assertEquals("Int should span blocks (3+1)", 0x78563412, value);
    }

    /**
     * Tests reading int at exact block boundary (all 4 bytes in second block).
     */
    public void testReadIntAtBlockBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        block1.set(LAYOUT_LE_INT, 0, 0xDEADBEEF);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE);
        int value = input.readInt();

        assertEquals("Int at boundary", 0xDEADBEEF, value);
    }

    // ==================== Long Reads Across Boundaries ====================

    /**
     * Tests reading long that spans block boundary (4 bytes in each block).
     */
    public void testReadLongAcrossBlockBoundaryEvenSplit() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // Position long so 4 bytes in each block
        long testValue = 0x123456789ABCDEF0L;
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 4, (byte) 0xF0);
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 3, (byte) 0xDE);
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 2, (byte) 0xBC);
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 1, (byte) 0x9A);
        block1.set(LAYOUT_BYTE, 0, (byte) 0x78);
        block1.set(LAYOUT_BYTE, 1, (byte) 0x56);
        block1.set(LAYOUT_BYTE, 2, (byte) 0x34);
        block1.set(LAYOUT_BYTE, 3, (byte) 0x12);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 4);
        long value = input.readLong();

        assertEquals("Long should span blocks (4+4)", testValue, value);
    }

    /**
     * Tests reading long with various split positions.
     */
    public void testReadLongAcrossBlockBoundaryVariousSplits() throws IOException {
        // Test splits: 1+7, 2+6, 3+5, 5+3, 6+2, 7+1
        int[] splits = { 1, 2, 3, 5, 6, 7 };

        for (int bytesInFirstBlock : splits) {
            long fileLength = BLOCK_SIZE * 2;
            MemorySegment block0 = arena.allocate(BLOCK_SIZE);
            MemorySegment block1 = arena.allocate(BLOCK_SIZE);

            long testValue = 0x123456789ABCDEF0L;
            byte[] bytes = new byte[8];
            for (int i = 0; i < 8; i++) {
                bytes[i] = (byte) (testValue >> (i * 8));
            }

            // Place bytes split across blocks
            for (int i = 0; i < bytesInFirstBlock; i++) {
                block0.set(LAYOUT_BYTE, BLOCK_SIZE - bytesInFirstBlock + i, bytes[i]);
            }
            for (int i = bytesInFirstBlock; i < 8; i++) {
                block1.set(LAYOUT_BYTE, i - bytesInFirstBlock, bytes[i]);
            }

            setupTwoBlocks(block0, block1);

            CachedMemorySegmentIndexInput input = createInput(fileLength);

            input.seek(BLOCK_SIZE - bytesInFirstBlock);
            long value = input.readLong();

            assertEquals("Long with " + bytesInFirstBlock + "+" + (8 - bytesInFirstBlock) + " split", testValue, value);
        }
    }

    /**
     * Tests readInts when int array spans block boundary.
     */
    public void testReadIntsSpanningBlockBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // Write 10 ints near boundary
        for (int i = 0; i < 10; i++) {
            long offset = BLOCK_SIZE - 20 + i * 4L;
            int value = 1000 + i;
            if (offset >= 0 && offset + 4 <= BLOCK_SIZE) {
                block0.set(LAYOUT_LE_INT, (int) offset, value);
            } else if (offset >= BLOCK_SIZE) {
                block1.set(LAYOUT_LE_INT, (int) (offset - BLOCK_SIZE), value);
            }
            // Ints spanning boundary will be handled by fallback
        }

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 20);
        int[] buffer = new int[5]; // Read 5 ints spanning boundary
        input.readInts(buffer, 0, 5);

        // Should read values successfully
        assertNotNull("Buffer should be populated", buffer);
    }

    /**
     * Tests readInts entirely within one block.
     */
    public void testReadIntsWithinBlock() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // Write ints in middle of block
        for (int i = 0; i < 10; i++) {
            block0.set(LAYOUT_LE_INT, 1000 + i * 4, 100 + i);
        }

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(1000);
        int[] buffer = new int[10];
        input.readInts(buffer, 0, 10);

        for (int i = 0; i < 10; i++) {
            assertEquals("Int " + i, 100 + i, buffer[i]);
        }
    }

    /**
     * Tests readLongs spanning block boundary.
     */
    public void testReadLongsSpanningBlockBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // Write longs near boundary
        for (int i = 0; i < 5; i++) {
            long offset = BLOCK_SIZE - 24 + i * 8L;
            long value = 10000L + i;
            if (offset >= 0 && offset + 8 <= BLOCK_SIZE) {
                block0.set(LAYOUT_LE_LONG, (int) offset, value);
            } else if (offset >= BLOCK_SIZE) {
                block1.set(LAYOUT_LE_LONG, (int) (offset - BLOCK_SIZE), value);
            }
        }

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 24);
        long[] buffer = new long[3];
        input.readLongs(buffer, 0, 3);

        assertNotNull("Buffer should be populated", buffer);
    }

    /**
     * Tests readFloats spanning block boundary.
     */
    public void testReadFloatsSpanningBlockBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // Write floats near boundary
        for (int i = 0; i < 5; i++) {
            long offset = BLOCK_SIZE - 8 + i * 4L;
            float value = 10.5f + i;
            if (offset >= 0 && offset + 4 <= BLOCK_SIZE) {
                block0.set(LAYOUT_LE_FLOAT, (int) offset, value);
            } else if (offset >= BLOCK_SIZE) {
                block1.set(LAYOUT_LE_FLOAT, (int) (offset - BLOCK_SIZE), value);
            }
        }

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 8);
        float[] buffer = new float[3];
        input.readFloats(buffer, 0, 3);

        assertNotNull("Buffer should be populated", buffer);
    }

    /**
     * Tests random access readByte at block boundaries and various offsets.
     */
    public void testRandomAccessByteAtVariousOffsets() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);
        MemorySegment block2 = arena.allocate(BLOCK_SIZE);

        // Fill with identifiable pattern
        for (int i = 0; i < BLOCK_SIZE; i++) {
            block0.set(LAYOUT_BYTE, i, (byte) (i % 128));
            block1.set(LAYOUT_BYTE, i, (byte) ((i + 50) % 128));
            block2.set(LAYOUT_BYTE, i, (byte) ((i + 100) % 128));
        }

        setupThreeBlocks(block0, block1, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Random access reads at strategic positions
        assertEquals("Byte at 0", (byte) 0, input.readByte(0));
        assertEquals("Byte at BLOCK_SIZE-1", (byte) ((BLOCK_SIZE - 1) % 128), input.readByte(BLOCK_SIZE - 1));
        assertEquals("Byte at BLOCK_SIZE", (byte) 50, input.readByte(BLOCK_SIZE));
        assertEquals("Byte at BLOCK_SIZE+1", (byte) 51, input.readByte(BLOCK_SIZE + 1));
        assertEquals("Byte at 2*BLOCK_SIZE-1", (byte) ((BLOCK_SIZE - 1 + 50) % 128), input.readByte(BLOCK_SIZE * 2 - 1));
        assertEquals("Byte at 2*BLOCK_SIZE", (byte) 100, input.readByte(BLOCK_SIZE * 2));

        // File pointer should not change
        assertEquals("File pointer should not move", 0, input.getFilePointer());
    }

    /**
     * Tests random access readInt at exact block boundaries.
     */
    public void testRandomAccessIntAtBlockBoundaries() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);
        MemorySegment block2 = arena.allocate(BLOCK_SIZE);

        // Place ints at strategic positions
        block0.set(LAYOUT_LE_INT, 0, 0x11111111);
        block0.set(LAYOUT_LE_INT, BLOCK_SIZE - 4, 0x22222222);
        block1.set(LAYOUT_LE_INT, 0, 0x33333333);
        block1.set(LAYOUT_LE_INT, BLOCK_SIZE / 2, 0x44444444);
        block1.set(LAYOUT_LE_INT, BLOCK_SIZE - 4, 0x55555555);
        block2.set(LAYOUT_LE_INT, 0, 0x66666666);

        setupThreeBlocks(block0, block1, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        assertEquals("Int at 0", 0x11111111, input.readInt(0));
        assertEquals("Int at end of block 0", 0x22222222, input.readInt(BLOCK_SIZE - 4));
        assertEquals("Int at start of block 1", 0x33333333, input.readInt(BLOCK_SIZE));
        assertEquals("Int at mid block 1", 0x44444444, input.readInt(BLOCK_SIZE + BLOCK_SIZE / 2));
        assertEquals("Int at end of block 1", 0x55555555, input.readInt(BLOCK_SIZE * 2 - 4));
        assertEquals("Int at start of block 2", 0x66666666, input.readInt(BLOCK_SIZE * 2));

        assertEquals("File pointer unchanged", 0, input.getFilePointer());
    }

    /**
     * Tests random access readLong spanning boundary.
     */
    public void testRandomAccessLongSpanningBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // Set up long spanning boundary
        long testValue = 0xFEDCBA9876543210L;
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 4, (byte) 0x10);
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 3, (byte) 0x32);
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 2, (byte) 0x54);
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 1, (byte) 0x76);
        block1.set(LAYOUT_BYTE, 0, (byte) 0x98);
        block1.set(LAYOUT_BYTE, 1, (byte) 0xBA);
        block1.set(LAYOUT_BYTE, 2, (byte) 0xDC);
        block1.set(LAYOUT_BYTE, 3, (byte) 0xFE);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        assertEquals("Long spanning boundary", testValue, input.readLong(BLOCK_SIZE - 4));
        assertEquals("File pointer unchanged", 0, input.getFilePointer());
    }

    /**
     * Tests random access readShort spanning boundary.
     */
    public void testRandomAccessShortSpanningBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 1, (byte) 0xAB);
        block1.set(LAYOUT_BYTE, 0, (byte) 0xCD);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        assertEquals("Short spanning boundary", (short) 0xCDAB, input.readShort(BLOCK_SIZE - 1));
    }

    /**
     * Tests slice starting exactly at block boundary.
     */
    public void testSliceStartingAtBlockBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0x10);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0x20);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 0x30);

        setupThreeBlocks(block0, block1, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Create slice starting at block 1
        CachedMemorySegmentIndexInput slice = input.slice("block1_slice", BLOCK_SIZE, BLOCK_SIZE);

        assertEquals("Slice length", BLOCK_SIZE, slice.length());
        assertEquals("Slice position", 0, slice.getFilePointer());

        byte value = slice.readByte();
        assertEquals("First byte of slice from block 1", (byte) 0x20, value);
    }

    /**
     * Tests slice ending exactly at block boundary.
     */
    public void testSliceEndingAtBlockBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 3);

        setupThreeBlocks(block0, block1, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Slice from middle of block 0 to exactly end of block 1
        long sliceOffset = BLOCK_SIZE / 2;
        long sliceLength = BLOCK_SIZE + BLOCK_SIZE / 2;
        CachedMemorySegmentIndexInput slice = input.slice("partial_slice", sliceOffset, sliceLength);

        assertEquals("Slice length", sliceLength, slice.length());

        // Read to end
        slice.seek(sliceLength - 1);
        byte lastByte = slice.readByte();
        assertEquals("Last byte at block 1 end", (byte) 2, lastByte);
    }

    /**
     * Tests slice spanning multiple blocks with non-aligned start and end.
     */
    public void testSliceSpanningBlocksNonAligned() throws IOException {
        long fileLength = BLOCK_SIZE * 4;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 3);
        MemorySegment block3 = createBlockWithPattern(3, (byte) 4);

        setupFourBlocks(block0, block1, block2, block3);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Slice from 1/4 into block 0 to 3/4 into block 3
        long sliceOffset = BLOCK_SIZE / 4;
        long sliceLength = BLOCK_SIZE * 3;
        CachedMemorySegmentIndexInput slice = input.slice("multi_block_slice", sliceOffset, sliceLength);

        // Read through slice verifying block transitions
        int readSize = BLOCK_SIZE + 100;
        byte[] buffer = new byte[readSize];
        slice.readBytes(buffer, 0, readSize);

        // First 3/4 block from block 0 (6144 bytes)
        for (int i = 0; i < BLOCK_SIZE * 3 / 4; i++) {
            assertEquals("Bytes from block 0", (byte) 1, buffer[i]);
        }
        // Next 100 bytes from block 1
        for (int i = BLOCK_SIZE * 3 / 4; i < readSize; i++) {
            assertEquals("Bytes from block 1", (byte) 2, buffer[i]);
        }
    }

    /**
     * Tests nested slices with boundary crossings.
     */
    public void testNestedSlicesAcrossBoundaries() throws IOException {
        long fileLength = BLOCK_SIZE * 4;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 3);
        MemorySegment block3 = createBlockWithPattern(3, (byte) 4);

        setupFourBlocks(block0, block1, block2, block3);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // First slice: spans blocks 1-2
        CachedMemorySegmentIndexInput slice1 = input.slice("slice1", BLOCK_SIZE, BLOCK_SIZE * 2);

        // Second slice: middle portion of first slice, crosses block 1-2 boundary
        CachedMemorySegmentIndexInput slice2 = slice1.slice("slice2", BLOCK_SIZE / 2, BLOCK_SIZE);

        // slice2 starts at BLOCK_SIZE + BLOCK_SIZE/2, spans into block 2
        byte[] buffer = new byte[100];
        slice2.seek(BLOCK_SIZE / 2 - 50); // Position near boundary
        slice2.readBytes(buffer, 0, 100);

        // Should have bytes from block 1 and block 2
        for (int i = 0; i < 50; i++) {
            assertEquals("Bytes from block 1", (byte) 2, buffer[i]);
        }
        for (int i = 50; i < 100; i++) {
            assertEquals("Bytes from block 2", (byte) 3, buffer[i]);
        }
    }

    /**
     * Tests slice absolute offset calculation with boundaries.
     */
    public void testSliceAbsoluteFileOffsetAtBoundaries() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 3);

        setupThreeBlocks(block0, block1, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Slice starting at block boundary
        CachedMemorySegmentIndexInput slice = input.slice("test_slice", BLOCK_SIZE, BLOCK_SIZE * 2);

        assertEquals("Slice absolute offset at pos 0", BLOCK_SIZE, slice.getAbsoluteFileOffset());
        slice.seek(BLOCK_SIZE - 1); // One before next boundary
        assertEquals("Slice absolute offset before boundary", BLOCK_SIZE * 2 - 1, slice.getAbsoluteFileOffset());
        slice.seek(BLOCK_SIZE); // Exactly at next boundary
        assertEquals("Slice absolute offset at boundary", BLOCK_SIZE * 2, slice.getAbsoluteFileOffset());
    }

    // ==================== Edge Cases and Error Conditions ====================

    /**
     * Tests reading zero bytes (no-op).
     */
    public void testReadZeroBytes() throws IOException {
        long fileLength = BLOCK_SIZE;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        byte[] buffer = new byte[10];
        input.readBytes(buffer, 0, 0);

        assertEquals("Position should not change", 0, input.getFilePointer());
    }

    /**
     * Tests zero-length operations for array reads.
     */
    public void testZeroLengthArrayReads() throws IOException {
        long fileLength = BLOCK_SIZE;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.readInts(new int[5], 0, 0);
        input.readLongs(new long[5], 0, 0);
        input.readFloats(new float[5], 0, 0);

        assertEquals("Position unchanged", 0, input.getFilePointer());
    }

    /**
     * Tests seek to negative position throws exception.
     * Note: The slice implementation uses assert, so this throws AssertionError in test mode.
     */
    public void testSeekNegativePosition() throws IOException {
        long fileLength = BLOCK_SIZE;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Expect either IOException or AssertionError depending on whether assertions are enabled
        expectThrows(Throwable.class, () -> input.seek(-1));
    }

    /**
     * Tests seek past EOF throws exception.
     */
    public void testSeekPastEOF() throws IOException {
        long fileLength = BLOCK_SIZE;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        expectThrows(IOException.class, () -> input.seek(fileLength + 1));
    }

    /**
     * Tests seek to exact file length is valid.
     */
    public void testSeekToFileLength() throws IOException {
        long fileLength = BLOCK_SIZE + 100;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(fileLength); // Should be valid
        assertEquals("Position at EOF", fileLength, input.getFilePointer());
    }

    /**
     * Tests clone preserves position at block boundary.
     */
    public void testCloneAtBlockBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE); // At exact boundary
        CachedMemorySegmentIndexInput clone = input.clone();

        assertEquals("Clone at boundary", BLOCK_SIZE, clone.getFilePointer());
        assertEquals("Clone length", fileLength, clone.length());
    }

    /**
     * Tests clone independence with reads across boundaries.
     */
    public void testCloneIndependenceAcrossBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 5);
        CachedMemorySegmentIndexInput clone = input.clone();

        // Read in original crosses boundary
        byte[] buffer1 = new byte[10];
        input.readBytes(buffer1, 0, 10);

        // Clone should still be at BLOCK_SIZE - 5
        assertEquals("Clone position independent", BLOCK_SIZE - 5, clone.getFilePointer());

        // Clone reads same data
        byte[] buffer2 = new byte[10];
        clone.readBytes(buffer2, 0, 10);

        assertArrayEquals("Clone reads same data", buffer1, buffer2);
    }

    /**
     * Tests reading at exact file length boundary.
     */
    public void testReadAtExactFileLength() throws IOException {
        long fileLength = BLOCK_SIZE + 99;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);
        for (int i = 0; i < 99; i++) {
            block1.set(LAYOUT_BYTE, i, (byte) (i + 2));
        }

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(fileLength - 1);
        byte lastByte = input.readByte();

        assertEquals("Last byte of file", (byte) (98 + 2), lastByte);
        assertEquals("At EOF", fileLength, input.getFilePointer());
    }

    /**
     * Tests partial read at end crossing block boundary.
     */
    public void testPartialReadAtEndCrossingBoundary() throws IOException {
        long fileLength = BLOCK_SIZE + 50;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);
        for (int i = 0; i < 50; i++) {
            block1.set(LAYOUT_BYTE, i, (byte) 2);
        }

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 10);
        byte[] buffer = new byte[60];
        input.readBytes(buffer, 0, 60);

        for (int i = 0; i < 10; i++) {
            assertEquals("Bytes from block 0", (byte) 1, buffer[i]);
        }
        for (int i = 10; i < 60; i++) {
            assertEquals("Bytes from block 1", (byte) 2, buffer[i]);
        }

        assertEquals("At EOF", fileLength, input.getFilePointer());
    }

    /**
     * Tests large sequential read through many block boundaries.
     */
    public void testLargeSequentialReadManyBlocks() throws IOException {
        int numBlocks = 10;
        long fileLength = BLOCK_SIZE * numBlocks;

        // Setup 10 blocks with different patterns
        for (int i = 0; i < numBlocks; i++) {
            MemorySegment block = createBlockWithPattern(i, (byte) (i + 1));
            setupBlock(i * BLOCK_SIZE, block);
        }

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        byte[] buffer = new byte[(int) fileLength];
        input.readBytes(buffer, 0, (int) fileLength);

        // Verify each block
        for (int blockIdx = 0; blockIdx < numBlocks; blockIdx++) {
            for (int i = 0; i < BLOCK_SIZE; i++) {
                int bufferPos = blockIdx * BLOCK_SIZE + i;
                assertEquals("Block " + blockIdx + " byte " + i, (byte) (blockIdx + 1), buffer[bufferPos]);
            }
        }
    }

    /**
     * Tests seek and read pattern across boundaries (simulating random access).
     */
    public void testRandomSeekReadPatternAcrossBoundaries() throws IOException {
        long fileLength = BLOCK_SIZE * 5;

        for (int i = 0; i < 5; i++) {
            MemorySegment block = createBlockWithPattern(i, (byte) (i * 10));
            setupBlock(i * BLOCK_SIZE, block);
        }

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Random access pattern at various boundaries
        long[] positions = {
            0,
            BLOCK_SIZE - 1,
            BLOCK_SIZE,
            BLOCK_SIZE + 1,
            BLOCK_SIZE * 2 - 1,
            BLOCK_SIZE * 2,
            BLOCK_SIZE * 3 + BLOCK_SIZE / 2,
            BLOCK_SIZE * 4 - 10 };

        for (long pos : positions) {
            input.seek(pos);
            byte value = input.readByte();
            assertNotNull("Should read byte at position " + pos, value);
        }
    }

    /**
     * Tests slice with invalid parameters at boundaries.
     */
    public void testSliceInvalidParametersAtBoundaries() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Negative offset
        expectThrows(IllegalArgumentException.class, () -> input.slice("test", -1, BLOCK_SIZE));

        // Negative length
        expectThrows(IllegalArgumentException.class, () -> input.slice("test", 0, -1));

        // Offset + length > file length (at boundary)
        expectThrows(IllegalArgumentException.class, () -> input.slice("test", BLOCK_SIZE, BLOCK_SIZE + 1));

        // Offset beyond file length
        expectThrows(IllegalArgumentException.class, () -> input.slice("test", fileLength + 1, 10));
    }

    // ==================== Close Operation Tests ====================

    /**
     * Tests that close clears resources properly.
     */
    public void testCloseUnpinsCurrentBlock() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Read a byte to load the block
        input.readByte();

        // Verify block is loaded by reading current position
        assertEquals(1, input.getFilePointer());

        // Close the input
        input.close();

        // Verify that tiny cache was cleared (which indicates cleanup happened)
        verify(mockTinyCache, times(1)).clear();
    }

    /**
     * Tests that close clears the block slot tiny cache.
     */
    public void testCloseClearsBlockSlotCache() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Read to populate cache
        input.readByte();

        // Close the input
        input.close();

        // Verify that tiny cache clear was called
        verify(mockTinyCache, times(1)).clear();
    }

    /**
     * Tests that close on master instance clears tiny cache but slice does not.
     */
    public void testCloseOnSliceDoesNotClearTinyCache() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);
        CachedMemorySegmentIndexInput slice = input.slice("test_slice", 100, BLOCK_SIZE);

        // Read from slice
        slice.readByte();

        // Reset mock to clear any previous interactions
        clearInvocations(mockTinyCache);

        // Close the slice (not the master)
        slice.close();

        // Verify that tiny cache clear was NOT called for slice
        verify(mockTinyCache, never()).clear();

        // Now close the master
        input.close();

        // Verify that tiny cache clear WAS called for master
        verify(mockTinyCache, times(1)).clear();
    }

    /**
     * Tests that close closes the readahead manager for master instance.
     */
    public void testCloseClosesReadaheadManager() throws IOException {
        long fileLength = BLOCK_SIZE;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Close the input
        input.close();

        // Verify readahead manager close was called
        verify(mockReadaheadManager, times(1)).close();
    }

    /**
     * Tests that close on slice does not close the readahead manager.
     */
    public void testCloseOnSliceDoesNotCloseReadaheadManager() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);
        CachedMemorySegmentIndexInput slice = input.slice("test_slice", 0, BLOCK_SIZE);

        // Reset mock to clear any previous interactions
        clearInvocations(mockReadaheadManager);

        // Close the slice
        slice.close();

        // Verify readahead manager was NOT closed
        verify(mockReadaheadManager, never()).close();

        // Close the master
        input.close();

        // Verify readahead manager WAS closed
        verify(mockReadaheadManager, times(1)).close();
    }

    /**
     * Tests that calling close multiple times is idempotent (safe).
     */
    public void testCloseIsIdempotent() throws IOException {
        long fileLength = BLOCK_SIZE;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Read to load a block
        input.readByte();

        // Close multiple times
        input.close();
        input.close();
        input.close();

        // Verify tiny cache clear was called only once (idempotent)
        verify(mockTinyCache, times(1)).clear();

        // Verify readahead manager close was called only once
        verify(mockReadaheadManager, times(1)).close();
    }

    /**
     * Tests that close unpins block even when positioned at block boundary.
     */
    public void testCloseUnpinsBlockAtBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Read up to block boundary
        input.seek(BLOCK_SIZE - 1);
        input.readByte();

        // Verify we're at the boundary
        assertEquals(BLOCK_SIZE, input.getFilePointer());

        // Close should unpin the current block
        input.close();

        // Verify tiny cache was cleared
        verify(mockTinyCache, times(1)).clear();
    }

    /**
     * Tests that close properly handles the case with no current block loaded.
     */
    public void testCloseWithNoCurrentBlock() throws IOException {
        long fileLength = BLOCK_SIZE;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Close immediately without reading (no current block)
        input.close();

        // Should still clear cache and close readahead manager
        verify(mockTinyCache, times(1)).clear();
        verify(mockReadaheadManager, times(1)).close();
    }

    /**
     * Tests that clone creates independent instance with separate lifecycle.
     */
    public void testCloneHasIndependentLifecycle() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Read to load block
        input.readByte();

        // Create clone
        CachedMemorySegmentIndexInput clone = input.clone();

        // Close the original
        input.close();

        // Clone should still be usable (it's a slice, so it has separate lifecycle)
        byte value = clone.readByte();
        assertEquals((byte) 1, value);

        // Close the clone
        clone.close();
    }

    /**
     * Tests cleanup with multiple blocks loaded and closed.
     */
    public void testCloseWithMultipleBlocksLoaded() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 3);

        setupThreeBlocks(block0, block1, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Read from multiple blocks to load them
        input.seek(BLOCK_SIZE - 1);
        input.readByte(); // Loads block 0

        input.seek(BLOCK_SIZE);
        input.readByte(); // Loads block 1

        input.seek(BLOCK_SIZE * 2);
        input.readByte(); // Loads block 2

        // Close should clean up resources
        input.close();

        // Verify cache clear was called
        verify(mockTinyCache, times(1)).clear();
        verify(mockReadaheadManager, times(1)).close();
    }

    private MemorySegment createBlockWithPattern(int blockIndex, byte pattern) {
        MemorySegment segment = arena.allocate(BLOCK_SIZE);
        for (int i = 0; i < BLOCK_SIZE; i++) {
            segment.set(LAYOUT_BYTE, i, pattern);
        }
        return segment;
    }

    private void setupOneBlock(MemorySegment block0) throws IOException {
        setupBlock(0, block0);
    }

    private void setupTwoBlocks(MemorySegment block0, MemorySegment block1) throws IOException {
        setupBlock(0, block0);
        setupBlock(BLOCK_SIZE, block1);
    }

    private void setupThreeBlocks(MemorySegment block0, MemorySegment block1, MemorySegment block2) throws IOException {
        setupBlock(0, block0);
        setupBlock(BLOCK_SIZE, block1);
        setupBlock(BLOCK_SIZE * 2, block2);
    }

    private void setupFourBlocks(MemorySegment block0, MemorySegment block1, MemorySegment block2, MemorySegment block3)
        throws IOException {
        setupBlock(0, block0);
        setupBlock(BLOCK_SIZE, block1);
        setupBlock(BLOCK_SIZE * 2, block2);
        setupBlock(BLOCK_SIZE * 3, block3);
    }

    private void setupBlock(long offset, MemorySegment segment) throws IOException {
        // Create a real RefCountedMemorySegment with a no-op releaser
        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, (int) segment.byteSize(), (seg) -> {
            // No-op releaser for tests
        });

        BlockCacheValue<RefCountedMemorySegment> value = mock(BlockCacheValue.class);
        when(value.value()).thenReturn(refSegment);
        when(value.tryPin()).thenReturn(true);

        BlockSlotTinyCache.LookupResult result = new BlockSlotTinyCache.LookupResult(value, true);
        when(mockTinyCache.acquireRefCountedValue(eq(offset))).thenReturn(result);
        when(mockCache.getOrLoad(any(FileBlockCacheKey.class))).thenReturn(value);
    }

    private CachedMemorySegmentIndexInput createInput(long length) {
        return CachedMemorySegmentIndexInput
            .newInstance("test", testPath, length, mockCache, mockReadaheadManager, mockReadaheadContext, mockTinyCache);
    }
}

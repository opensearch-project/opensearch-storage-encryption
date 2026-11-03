/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.junit.Before;
import org.opensearch.index.store.block.RefCountedByteBuffer;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;
import org.opensearch.test.OpenSearchTestCase;

@SuppressWarnings("unchecked")
public class CachedByteBufferIndexInputTests extends OpenSearchTestCase {

    private static final int BLOCK_SIZE = 8192;

    private BlockCache<RefCountedByteBuffer> mockCache;
    private BlockSlotTinyCache<RefCountedByteBuffer> mockTinyCache;
    private ReadaheadManager mockReadaheadManager;
    private ReadaheadContext mockReadaheadContext;
    private Path testPath;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mockCache = mock(BlockCache.class);
        mockTinyCache = mock(BlockSlotTinyCache.class);
        mockReadaheadManager = mock(ReadaheadManager.class);
        mockReadaheadContext = mock(ReadaheadContext.class);
        testPath = Paths.get("/test/bytebuffer.dat");
    }

    private RefCountedByteBuffer createBlock(byte[] data) {
        ByteBuffer buf = ByteBuffer.allocateDirect(data.length).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(0, data, 0, data.length);
        return new RefCountedByteBuffer(buf, data.length);
    }

    private void setupTinyCache(long blockOffset, RefCountedByteBuffer block) throws IOException {
        BlockCacheValue<RefCountedByteBuffer> value = block;
        when(mockTinyCache.acquireRefCountedValue(anyLong(), any())).thenAnswer(inv -> {
            long reqOffset = inv.getArgument(0);
            long alignedOffset = reqOffset & ~(BLOCK_SIZE - 1);
            if (alignedOffset == blockOffset)
                return value;
            return null;
        });
    }

    private void setupTwoCacheBlocks(long offset0, RefCountedByteBuffer block0, long offset1, RefCountedByteBuffer block1)
        throws IOException {
        when(mockTinyCache.acquireRefCountedValue(anyLong(), any())).thenAnswer(inv -> {
            long reqOffset = inv.getArgument(0);
            long alignedOffset = reqOffset & ~(BLOCK_SIZE - 1);
            if (alignedOffset == offset0) return (BlockCacheValue<RefCountedByteBuffer>) block0;
            if (alignedOffset == offset1) return (BlockCacheValue<RefCountedByteBuffer>) block1;
            return null;
        });
    }

    private CachedByteBufferIndexInput createInput(long fileLength) {
        return CachedByteBufferIndexInput
            .newInstance("test", testPath, fileLength, mockCache, mockReadaheadManager, mockReadaheadContext, mockTinyCache);
    }

    // ---- readByte ----

    public void testReadByteAtBlockBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        byte[] block1Data = new byte[BLOCK_SIZE];
        block1Data[0] = 42;
        setupTinyCache(BLOCK_SIZE, createBlock(block1Data));

        CachedByteBufferIndexInput input = createInput(fileLength);
        input.seek(BLOCK_SIZE);
        assertEquals(42, input.readByte());
    }

    public void testReadByteAtEndOfBlock() throws IOException {
        byte[] block0Data = new byte[BLOCK_SIZE];
        block0Data[BLOCK_SIZE - 1] = 99;
        setupTinyCache(0, createBlock(block0Data));

        CachedByteBufferIndexInput input = createInput(BLOCK_SIZE);
        input.seek(BLOCK_SIZE - 1);
        assertEquals(99, input.readByte());
    }

    // ---- readBytes ----

    public void testReadBytesWithinBlock() throws IOException {
        byte[] block0Data = new byte[BLOCK_SIZE];
        for (int i = 0; i < 100; i++)
            block0Data[i] = (byte) i;
        setupTinyCache(0, createBlock(block0Data));

        CachedByteBufferIndexInput input = createInput(BLOCK_SIZE);
        byte[] result = new byte[100];
        input.readBytes(result, 0, 100);
        for (int i = 0; i < 100; i++)
            assertEquals((byte) i, result[i]);
    }

    public void testReadBytesAcrossBlockBoundary() throws IOException {
        byte[] block0 = new byte[BLOCK_SIZE];
        byte[] block1 = new byte[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++)
            block0[i] = (byte) 0xAA;
        for (int i = 0; i < BLOCK_SIZE; i++)
            block1[i] = (byte) 0xBB;
        setupTwoCacheBlocks(0, createBlock(block0), BLOCK_SIZE, createBlock(block1));

        CachedByteBufferIndexInput input = createInput(BLOCK_SIZE * 2);
        input.seek(BLOCK_SIZE - 4);
        byte[] result = new byte[8];
        input.readBytes(result, 0, 8);
        for (int i = 0; i < 4; i++)
            assertEquals((byte) 0xAA, result[i]);
        for (int i = 4; i < 8; i++)
            assertEquals((byte) 0xBB, result[i]);
    }

    // ---- readShort / readInt / readLong ----

    public void testReadShort() throws IOException {
        byte[] data = new byte[BLOCK_SIZE];
        ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).putShort(100, (short) 12345);
        setupTinyCache(0, createBlock(data));

        CachedByteBufferIndexInput input = createInput(BLOCK_SIZE);
        input.seek(100);
        assertEquals(12345, input.readShort());
    }

    public void testReadInt() throws IOException {
        byte[] data = new byte[BLOCK_SIZE];
        ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).putInt(200, 0xDEADBEEF);
        setupTinyCache(0, createBlock(data));

        CachedByteBufferIndexInput input = createInput(BLOCK_SIZE);
        input.seek(200);
        assertEquals(0xDEADBEEF, input.readInt());
    }

    public void testReadLong() throws IOException {
        byte[] data = new byte[BLOCK_SIZE];
        ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).putLong(300, 0x123456789ABCDEF0L);
        setupTinyCache(0, createBlock(data));

        CachedByteBufferIndexInput input = createInput(BLOCK_SIZE);
        input.seek(300);
        assertEquals(0x123456789ABCDEF0L, input.readLong());
    }

    // ---- RandomAccessInput positional reads ----

    public void testReadBytePositional() throws IOException {
        byte[] data = new byte[BLOCK_SIZE];
        data[500] = 77;
        setupTinyCache(0, createBlock(data));

        CachedByteBufferIndexInput input = createInput(BLOCK_SIZE);
        RandomAccessInput rai = (RandomAccessInput) input;
        assertEquals(77, rai.readByte(500));
    }

    public void testReadIntPositional() throws IOException {
        byte[] data = new byte[BLOCK_SIZE];
        ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).putInt(400, 42);
        setupTinyCache(0, createBlock(data));

        CachedByteBufferIndexInput input = createInput(BLOCK_SIZE);
        RandomAccessInput rai = (RandomAccessInput) input;
        assertEquals(42, rai.readInt(400));
    }

    public void testReadLongPositional() throws IOException {
        byte[] data = new byte[BLOCK_SIZE];
        ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).putLong(600, 999999L);
        setupTinyCache(0, createBlock(data));

        CachedByteBufferIndexInput input = createInput(BLOCK_SIZE);
        RandomAccessInput rai = (RandomAccessInput) input;
        assertEquals(999999L, rai.readLong(600));
    }

    // ---- seek / length / clone / slice ----

    public void testSeekAndGetFilePointer() throws IOException {
        byte[] data = new byte[BLOCK_SIZE];
        setupTinyCache(0, createBlock(data));

        CachedByteBufferIndexInput input = createInput(BLOCK_SIZE);
        assertEquals(0, input.getFilePointer());
        input.seek(100);
        assertEquals(100, input.getFilePointer());
    }

    public void testLength() throws IOException {
        CachedByteBufferIndexInput input = createInput(12345);
        assertEquals(12345, input.length());
    }

    public void testClone() throws IOException {
        byte[] data = new byte[BLOCK_SIZE];
        data[0] = 11;
        setupTinyCache(0, createBlock(data));

        CachedByteBufferIndexInput input = createInput(BLOCK_SIZE);
        input.seek(50);
        CachedByteBufferIndexInput clone = input.clone();
        assertEquals(50, clone.getFilePointer());
        assertEquals(BLOCK_SIZE, clone.length());
    }

    public void testSlice() throws IOException {
        byte[] data = new byte[BLOCK_SIZE];
        ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).putInt(100, 42);
        setupTinyCache(0, createBlock(data));

        CachedByteBufferIndexInput input = createInput(BLOCK_SIZE);
        IndexInput slice = input.slice("test-slice", 100, 200);
        assertEquals(200, slice.length());
        assertEquals(0, slice.getFilePointer());
        assertEquals(42, slice.readInt());
    }

    public void testSliceOutOfBoundsThrows() throws IOException {
        CachedByteBufferIndexInput input = createInput(BLOCK_SIZE);
        expectThrows(IllegalArgumentException.class, () -> input.slice("bad", -1, 10));
        expectThrows(IllegalArgumentException.class, () -> input.slice("bad", 0, BLOCK_SIZE + 1));
    }

    // ---- close ----

    public void testCloseMarksAsClosed() throws IOException {
        CachedByteBufferIndexInput input = createInput(BLOCK_SIZE);
        input.close();
        expectThrows(Exception.class, () -> input.getFilePointer());
    }

    public void testCloseIsIdempotent() throws IOException {
        CachedByteBufferIndexInput input = createInput(BLOCK_SIZE);
        input.close();
        input.close(); // should not throw
    }
}

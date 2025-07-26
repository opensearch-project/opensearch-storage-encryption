/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIOReader.directIOReadAligned;
import static org.opensearch.index.store.directio.DirectIOReader.getDirectOpenOption;

import java.io.EOFException;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.BlockLoader;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;

@SuppressWarnings("preview")
public class CryptoDirectIOMemoryIndexInput extends IndexInput implements RandomAccessInput {
    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectIOIndexOutput.class);

    static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;
    static final ValueLayout.OfShort LAYOUT_LE_SHORT = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfInt LAYOUT_LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfLong LAYOUT_LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfFloat LAYOUT_LE_FLOAT = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    final long length;
    final long chunkSizeMask;
    final int chunkSizePower;
    final FileChannel channel;
    final Arena arena;
    final MemorySegment[] segments;
    final RefCountedMemorySegment[] inAccessMemorySegments;
    final Path path;
    final BlockCache<RefCountedMemorySegment> blockCache;
    final BlockLoader<RefCountedMemorySegment> blockLoader;
    final boolean ownsSegments;
    final String resourceDescription;
    final byte[] key;
    final byte[] iv;
    private volatile boolean closed = false;

    int curSegmentIndex = -1;
    MemorySegment curSegment; // redundant for speed: segments[curSegmentIndex], also marker if closed!
    long curPosition; // relative to curSegment, not globally

    public static CryptoDirectIOMemoryIndexInput newInstance(
        String resourceDescription,
        FileChannel channel,
        Path path,
        Arena arena,
        BlockCache<RefCountedMemorySegment> blockCache,
        BlockLoader<RefCountedMemorySegment> blockLoader,
        MemorySegment[] segments,
        RefCountedMemorySegment[] inAccessMemorySegments,
        long length,
        int chunkSizePower,
        byte[] key,
        byte[] iv
    ) {
        return new MultiSegmentImpl(
            resourceDescription,
            channel,
            path,
            arena,
            blockCache,
            blockLoader,
            segments,
            inAccessMemorySegments,
            0,
            length,
            chunkSizePower,
            true,
            key,
            iv
        );
    }

    private CryptoDirectIOMemoryIndexInput(
        String resourceDescription,
        FileChannel channel,
        Path path,
        Arena arena,
        BlockCache<RefCountedMemorySegment> blockCache,
        BlockLoader<RefCountedMemorySegment> blockLoader,
        MemorySegment[] segments,
        RefCountedMemorySegment[] inAccessMemorySegments,
        long length,
        int chunkSizePower,
        boolean ownsSegments,
        byte[] key,
        byte[] iv
    ) {
        super(resourceDescription);
        this.channel = channel;
        this.arena = arena;
        this.resourceDescription = resourceDescription;
        this.blockCache = blockCache;
        this.blockLoader = blockLoader;
        this.path = path;
        this.segments = segments;
        this.inAccessMemorySegments = inAccessMemorySegments;
        this.length = length;
        this.chunkSizePower = chunkSizePower;
        this.chunkSizeMask = (1L << chunkSizePower) - 1L;
        this.ownsSegments = ownsSegments;
        this.curSegment = (segments[0] != null) ? segments[0] : null;
        this.key = key;
        this.iv = iv;
    }

    void ensureOpen() {
        if (closed) {
            throw alreadyClosed(null);
        }
    }

    // the unused parameter is just to silence javac about unused variables
    RuntimeException handlePositionalIOOBE(RuntimeException unused, String action, long pos) throws IOException {
        if (pos < 0L) {
            return new IllegalArgumentException(action + " negative position (pos=" + pos + "): " + this);
        } else {
            throw new EOFException(action + " past EOF (pos=" + pos + "): " + this);
        }
    }

    // the unused parameter is just to silence javac about unused variables
    AlreadyClosedException alreadyClosed(RuntimeException unused) {
        return new AlreadyClosedException("Already closed: " + this);
    }

    private MemorySegment ensureSegmentLoaded(int segmentIndex) throws IOException {
        int chunkSize = (1 << chunkSizePower);
        MemorySegment segment = segments[segmentIndex];

        if (segment != null) {
            return segment;
        }

        long offset = segmentIndex * chunkSize;
        BlockCacheKey cacheKey = new DirectIOBlockCacheKey(path, offset);

        Optional<BlockCacheValue<RefCountedMemorySegment>> valueOpt = blockCache.getOrLoad(cacheKey, chunkSize, blockLoader);

        if (valueOpt.isPresent()) {
            RefCountedMemorySegment refSeg = valueOpt.get().block();
            segments[segmentIndex] = refSeg.segment();
            inAccessMemorySegments[segmentIndex] = refSeg;
            return refSeg.segment();
        }

        // todo figure out how we can use the already initilized file channel.
        try {
            segment = directIOReadAligned(channel, offset, chunkSize, arena);
            try {
                DirectIOReader.decryptSegment(arena, segment, offset, key, iv);
            } catch (Throwable t) {
                throw new RuntimeException("Decryption failed at offset: " + offset, t);
            }
            segments[segmentIndex] = segment;
            return segment;

        } catch (java.nio.channels.ClosedChannelException e) {
            throw new AlreadyClosedException("FileChannel already closed for: " + path, e);
        }
    }

    @Override
    public final byte readByte() throws IOException {
        try {
            if (curSegment == null) {
                curSegment = ensureSegmentLoaded(curSegmentIndex);
            }

            final byte v = curSegment.get(LAYOUT_BYTE, curPosition);
            curPosition++;
            return v;
        } catch (@SuppressWarnings("unused") IndexOutOfBoundsException e) {
            do {
                curSegmentIndex++;
                if (curSegmentIndex >= segments.length) {
                    throw new EOFException("read past EOF: " + this);
                }
                curSegment = ensureSegmentLoaded(curSegmentIndex);
                curPosition = 0L;
            } while (curSegment.byteSize() == 0L);
            final byte v = curSegment.get(LAYOUT_BYTE, curPosition);
            curPosition++;
            return v;
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final void readBytes(byte[] b, int offset, int len) throws IOException {
        try {
            // Ensure current segment is loaded
            if (curSegment == null) {
                curSegment = ensureSegmentLoaded(curSegmentIndex);
            }

            MemorySegment.copy(curSegment, LAYOUT_BYTE, curPosition, b, offset, len);
            curPosition += len;
        } catch (@SuppressWarnings("unused") IndexOutOfBoundsException e) {
            readBytesBoundary(b, offset, len);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    private void readBytesBoundary(byte[] b, int offset, int len) throws IOException {
        try {
            if (curSegment == null) {
                curSegment = ensureSegmentLoaded(curSegmentIndex);
            }

            long curAvail = curSegment.byteSize() - curPosition;
            while (len > curAvail) {
                MemorySegment.copy(curSegment, LAYOUT_BYTE, curPosition, b, offset, (int) curAvail);
                len -= curAvail;
                offset += curAvail;
                curSegmentIndex++;
                if (curSegmentIndex >= segments.length) {
                    throw new EOFException("read past EOF: " + this);
                }

                curSegment = ensureSegmentLoaded(curSegmentIndex);
                curPosition = 0L;
                curAvail = curSegment.byteSize();
            }
            MemorySegment.copy(curSegment, LAYOUT_BYTE, curPosition, b, offset, len);
            curPosition += len;
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    private void ensureSegmentsForBoundaryCrossing(int elementCount, int elementSize) throws IOException {
        // Ensure current segment is loaded
        if (curSegment == null) {
            curSegment = ensureSegmentLoaded(curSegmentIndex);
        }

        // Calculate how many segments this read might span
        long totalBytes = elementSize * (long) elementCount;
        long currentRemaining = curSegment.byteSize() - curPosition;

        // If read spans beyond current segment, ensure next segments are loaded
        if (totalBytes > currentRemaining) {
            int segmentsToLoad = (int) ((totalBytes - currentRemaining + (1L << chunkSizePower) - 1) >>> chunkSizePower);
            for (int i = 1; i <= segmentsToLoad && (curSegmentIndex + i) < segments.length; i++) {
                ensureSegmentLoaded(curSegmentIndex + i);
            }
        }
    }

    @Override
    public void readInts(int[] dst, int offset, int length) throws IOException {
        try {
            // Ensure current segment is loaded
            if (curSegment == null) {
                curSegment = ensureSegmentLoaded(curSegmentIndex);
            }

            MemorySegment.copy(curSegment, LAYOUT_LE_INT, curPosition, dst, offset, length);
            curPosition += Integer.BYTES * (long) length;
        } catch (@SuppressWarnings("unused") IndexOutOfBoundsException iobe) {
            ensureSegmentsForBoundaryCrossing(length, Integer.BYTES);
            super.readInts(dst, offset, length);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public void readLongs(long[] dst, int offset, int length) throws IOException {
        try {
            if (curSegment == null) {
                curSegment = ensureSegmentLoaded(curSegmentIndex);
            }

            MemorySegment.copy(curSegment, LAYOUT_LE_LONG, curPosition, dst, offset, length);
            curPosition += Long.BYTES * (long) length;
        } catch (@SuppressWarnings("unused") IndexOutOfBoundsException iobe) {
            ensureSegmentsForBoundaryCrossing(length, Long.BYTES);
            super.readLongs(dst, offset, length);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public void readFloats(float[] dst, int offset, int length) throws IOException {
        try {
            if (curSegment == null) {
                curSegment = ensureSegmentLoaded(curSegmentIndex);
            }

            MemorySegment.copy(curSegment, LAYOUT_LE_FLOAT, curPosition, dst, offset, length);
            curPosition += Float.BYTES * (long) length;
        } catch (@SuppressWarnings("unused") IndexOutOfBoundsException iobe) {
            ensureSegmentsForBoundaryCrossing(length, Float.BYTES);
            super.readFloats(dst, offset, length);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final short readShort() throws IOException {
        try {
            if (curSegment == null) {
                curSegment = ensureSegmentLoaded(curSegmentIndex);
            }

            final short v = curSegment.get(LAYOUT_LE_SHORT, curPosition);
            curPosition += Short.BYTES;
            return v;
        } catch (@SuppressWarnings("unused") IndexOutOfBoundsException e) {
            // Any single primitive read (short/int/long) can span at most 2 segments
            // when crossing a boundary, so we only need to ensure next segment is loaded
            if (curSegmentIndex + 1 < segments.length) {
                ensureSegmentLoaded(curSegmentIndex + 1);
            }
            return super.readShort();
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final int readInt() throws IOException {
        try {
            // Ensure current segment is loaded
            if (curSegment == null) {
                curSegment = ensureSegmentLoaded(curSegmentIndex);
            }

            final int v = curSegment.get(LAYOUT_LE_INT, curPosition);
            curPosition += Integer.BYTES;
            return v;
        } catch (@SuppressWarnings("unused") IndexOutOfBoundsException e) {
            if (curSegmentIndex + 1 < segments.length) {
                ensureSegmentLoaded(curSegmentIndex + 1);
            }
            return super.readInt();
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final long readLong() throws IOException {
        try {
            // Ensure current segment is loaded
            if (curSegment == null) {
                curSegment = ensureSegmentLoaded(curSegmentIndex);
            }

            final long v = curSegment.get(LAYOUT_LE_LONG, curPosition);
            curPosition += Long.BYTES;
            return v;
        } catch (@SuppressWarnings("unused") IndexOutOfBoundsException e) {
            if (curSegmentIndex + 1 < segments.length) {
                ensureSegmentLoaded(curSegmentIndex + 1);
            }
            return super.readLong();
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public long getFilePointer() {
        ensureOpen();
        return (((long) curSegmentIndex) << chunkSizePower) + curPosition;
    }

    @Override
    public void seek(long pos) throws IOException {
        ensureOpen();
        // we use >> here to preserve negative, so we will catch AIOOBE,
        // in case pos + offset overflows.

        final int si = (int) (pos >> chunkSizePower);
        try {
            if (si != curSegmentIndex) {
                this.curSegmentIndex = si;
                this.curSegment = null; // Mark as not loaded
            }
            this.curPosition = pos & chunkSizeMask;
        } catch (IndexOutOfBoundsException e) {
            throw handlePositionalIOOBE(e, "seek", pos);
        }
    }

    // used only by random access methods to handle reads across boundaries
    private void setPos(long pos, int si) throws IOException {
        try {
            final MemorySegment loadedSeg = (segments[si] != null) ? segments[si] : ensureSegmentLoaded(si);

            this.curPosition = pos & chunkSizeMask;
            this.curSegmentIndex = si;
            this.curSegment = loadedSeg;
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public byte readByte(long pos) throws IOException {
        try {
            final int si = (int) (pos >> chunkSizePower);

            MemorySegment segment = (segments[si] != null) ? segments[si] : ensureSegmentLoaded(si);

            return segment.get(LAYOUT_BYTE, pos & chunkSizeMask);
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public short readShort(long pos) throws IOException {
        final int si = (int) (pos >> chunkSizePower);
        try {
            MemorySegment segment = (segments[si] != null) ? segments[si] : ensureSegmentLoaded(si);

            return segment.get(LAYOUT_LE_SHORT, pos & chunkSizeMask);
        } catch (@SuppressWarnings("unused") IndexOutOfBoundsException ioobe) {
            // either it's a boundary, or read past EOF, fall back:
            setPos(pos, si);  // This will load the segment
            return readShort(); // Sequential readShort() already handles lazy loading
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public int readInt(long pos) throws IOException {
        final int si = (int) (pos >> chunkSizePower);
        try {

            MemorySegment segment = (segments[si] != null) ? segments[si] : ensureSegmentLoaded(si);

            return segment.get(LAYOUT_LE_INT, pos & chunkSizeMask);
        } catch (@SuppressWarnings("unused") IndexOutOfBoundsException ioobe) {
            // either it's a boundary, or read past EOF, fall back:
            setPos(pos, si);
            return readInt();
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public long readLong(long pos) throws IOException {
        final int si = (int) (pos >> chunkSizePower);
        try {

            MemorySegment segment = (segments[si] != null) ? segments[si] : ensureSegmentLoaded(si);

            return segment.get(LAYOUT_LE_LONG, pos & chunkSizeMask);
        } catch (@SuppressWarnings("unused") IndexOutOfBoundsException ioobe) {
            // either it's a boundary, or read past EOF, fall back:
            setPos(pos, si);
            return readLong();
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final long length() {
        return length;
    }

    @Override
    public final CryptoDirectIOMemoryIndexInput clone() {
        final CryptoDirectIOMemoryIndexInput clone = buildSlice((String) null, 0L, this.length);
        try {
            clone.seek(getFilePointer());
        } catch (IOException ioe) {
            throw new AssertionError(ioe);
        }

        return clone;
    }

    /**
     * Creates a slice of this index input, with the given description, offset, and length. The slice
     * is seeked to the beginning.
     */
    @Override
    public final CryptoDirectIOMemoryIndexInput slice(String sliceDescription, long offset, long length) {
        if (offset < 0 || length < 0 || offset + length > this.length) {
            throw new IllegalArgumentException(
                "slice() "
                    + sliceDescription
                    + " out of bounds: offset="
                    + offset
                    + ",length="
                    + length
                    + ",fileLength="
                    + this.length
                    + ": "
                    + this
            );
        }

        return buildSlice(sliceDescription, offset, length);
    }

    CryptoDirectIOMemoryIndexInput buildSlice(String sliceDescription, long offset, long length) {
        ensureOpen();

        final long sliceEnd = offset + length;
        final int startIndex = (int) (offset >>> chunkSizePower);
        final int endIndex = (int) (sliceEnd >>> chunkSizePower);

        // we always allocate one more slice, the last one may be a 0 byte one after truncating with
        // asSlice():
        final MemorySegment slices[] = ArrayUtil.copyOfSubArray(segments, startIndex, endIndex + 1);

        for (int i = 0; i < slices.length; i++) {
            if (slices[i] == null) {
                int originalIndex = startIndex + i;
                try {
                    slices[i] = borrowSegment(originalIndex);
                } catch (IOException e) {
                    throw new RuntimeException(
                        String.format("Failed to load segment %d via DirectIO (range: %d-%d)", i, startIndex, endIndex),
                        e
                    );
                }
            }
        }

        // Now we can safely slice the last segment since it's guaranteed to be loaded
        slices[slices.length - 1] = slices[slices.length - 1].asSlice(0L, sliceEnd & chunkSizeMask);

        offset = offset & chunkSizeMask;

        final String newResourceDescription = getFullSliceDescription(sliceDescription);
        long sliceOffset = offset & chunkSizeMask;

        return new MultiSegmentImpl(
            newResourceDescription,
            channel,  // still needed for lazy load
            path,
            arena,
            blockCache,
            blockLoader,
            slices,
            inAccessMemorySegments,
            sliceOffset,
            length,
            chunkSizePower,
            false,  // not owner of arena or segments
            key,
            iv
        );
    }

    private MemorySegment borrowSegment(int segmentIndex) throws IOException {
        int chunkSize = (1 << chunkSizePower);
        MemorySegment segment = segments[segmentIndex];

        if (segment != null) {
            return segment;
        }

        long offset = segmentIndex * chunkSize;
        BlockCacheKey cacheKey = new DirectIOBlockCacheKey(path, offset);

        BlockCacheValue<RefCountedMemorySegment> maybeValue = blockCache.get(cacheKey);
        if (maybeValue != null) {
            RefCountedMemorySegment refSeg = maybeValue.borrowBlock();
            segments[segmentIndex] = refSeg.segment();
            return refSeg.segment();
        }

        // todo figure out how we can use the already initilized file channel.
        try (FileChannel verySubOptimalFileChannel = FileChannel.open(path, StandardOpenOption.READ, getDirectOpenOption())) {
            segment = directIOReadAligned(verySubOptimalFileChannel, offset, chunkSize, arena);
            try {
                DirectIOReader.decryptSegment(arena, segment, offset, key, iv);
            } catch (Throwable t) {
                throw new RuntimeException("Decryption failed at offset: " + offset, t);
            }
            segments[segmentIndex] = segment;
            return segment;

        } catch (java.nio.channels.ClosedChannelException e) {
            throw new AlreadyClosedException("FileChannel already closed for: " + path, e);
        }
    }

    @Override
    public final void close() throws IOException {
        if (closed) {
            return;
        }

        closed = true;  // Mark as closed first

        // Close FileChannel first
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                LOGGER.warn("Failed to close FileChannel for {}", path, e);
            }
        }

        // sliced segments are not owned.
        if (ownsSegments) {
            for (RefCountedMemorySegment refSeg : inAccessMemorySegments) {
                if (refSeg != null) {
                    refSeg.decRef();
                }
            }

            // the master IndexInput has an Arena and is able
            // to release all resources (unmap segments) - a
            // side effect is that other threads still using clones
            // will throw IllegalStateException
            if (arena != null) {
                while (arena.scope().isAlive()) {
                    try {
                        arena.close();
                        break;
                    } catch (IllegalStateException e) {
                        Thread.onSpinWait();
                    }
                }
            }
        }

        curSegment = null;  // Still set to null for NPE on access
        Arrays.fill(segments, null);
    }

    /** This class adds offset support to MemorySegmentIndexInput, which is needed for slices. */
    static final class MultiSegmentImpl extends CryptoDirectIOMemoryIndexInput {
        private final long offset;

        MultiSegmentImpl(
            String resourceDescription,
            FileChannel channel,
            Path path,
            Arena arena,
            BlockCache<RefCountedMemorySegment> blockCache,
            BlockLoader<RefCountedMemorySegment> blockLoader,
            MemorySegment[] segments,
            RefCountedMemorySegment[] inAccessMemorySegments,
            long offset,
            long length,
            int chunkSizePower,
            boolean ownsSegments,
            byte[] key,
            byte[] iv
        ) {
            super(
                resourceDescription,
                channel,
                path,
                arena,
                blockCache,
                blockLoader,
                segments,
                inAccessMemorySegments,
                length,
                chunkSizePower,
                ownsSegments,
                key,
                iv
            );
            this.offset = offset;
            try {
                seek(0L);
            } catch (IOException ioe) {
                throw new AssertionError(ioe);
            }
            assert curSegmentIndex >= 0;
        }

        @Override
        RuntimeException handlePositionalIOOBE(RuntimeException unused, String action, long pos) throws IOException {
            return super.handlePositionalIOOBE(unused, action, pos - offset);
        }

        @Override
        public void seek(long pos) throws IOException {
            assert pos >= 0L : "negative position";
            super.seek(pos + offset);
        }

        @Override
        public long getFilePointer() {
            return super.getFilePointer() - offset;
        }

        @Override
        public byte readByte(long pos) throws IOException {
            return super.readByte(pos + offset);
        }

        @Override
        public short readShort(long pos) throws IOException {
            return super.readShort(pos + offset);
        }

        @Override
        public int readInt(long pos) throws IOException {
            return super.readInt(pos + offset);
        }

        @Override
        public long readLong(long pos) throws IOException {
            return super.readLong(pos + offset);
        }

        @Override
        CryptoDirectIOMemoryIndexInput buildSlice(String sliceDescription, long ofs, long length) {
            return super.buildSlice(sliceDescription, this.offset + ofs, length);
        }
    }
}

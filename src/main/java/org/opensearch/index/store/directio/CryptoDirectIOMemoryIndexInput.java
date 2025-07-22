/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import java.io.EOFException;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;

@SuppressWarnings("preview")
public class CryptoDirectIOMemoryIndexInput extends IndexInput implements RandomAccessInput {
    static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;
    static final ValueLayout.OfShort LAYOUT_LE_SHORT = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfInt LAYOUT_LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfLong LAYOUT_LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfFloat LAYOUT_LE_FLOAT = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    final long length;
    final long chunkSizeMask;
    final int chunkSizePower;
    final Arena arena;
    final RefCountedMemorySegment[] segments;
    final Path path;
    final BlockCache<RefCountedMemorySegment> blockCache;
    private volatile boolean closed = false;

    int curSegmentIndex = -1;
    MemorySegment curSegment; // redundant for speed: segments[curSegmentIndex], also marker if closed!
    long curPosition; // relative to curSegment, not globally

    public static CryptoDirectIOMemoryIndexInput newInstance(
        String resourceDescription,
        Path path,
        Arena arena,
        BlockCache<RefCountedMemorySegment> blockCache,
        RefCountedMemorySegment[] segments,
        long length,
        int chunkSizePower
    ) {
        return new MultiSegmentImpl(resourceDescription, path, arena, blockCache, segments, 0, length, chunkSizePower);
    }

    private CryptoDirectIOMemoryIndexInput(
        String resourceDescription,
        Path path,
        Arena arena,
        BlockCache<RefCountedMemorySegment> blockCache,
        RefCountedMemorySegment[] segments,
        long length,
        int chunkSizePower
    ) {
        super(resourceDescription);
        this.arena = arena;
        this.blockCache = blockCache;
        this.path = path;
        this.segments = segments;
        this.length = length;
        this.chunkSizePower = chunkSizePower;
        this.chunkSizeMask = (1L << chunkSizePower) - 1L;
        this.curSegment = (segments[0] != null) ? segments[0].segment() : null;
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
        if (segments[segmentIndex] == null) {
            long offset = segmentIndex * chunkSize;
            BlockCacheKey cacheKey = new DirectIOBlockCacheKey(path, offset);

            Optional<BlockCacheValue<RefCountedMemorySegment>> valueOpt = blockCache.getOrLoad(cacheKey, chunkSize);

            if (valueOpt.isPresent()) {
                BlockCacheValue<RefCountedMemorySegment> blockValue = valueOpt.get();

                RefCountedMemorySegment refSeg = blockValue.block();
                segments[segmentIndex] = refSeg;
            } else {
                throw new IOException(
                    "Failed to load segment " + segmentIndex + " - potentially the pool exhausted or DirectIO failed for: " + path
                );
            }
        }

        return segments[segmentIndex].segment();
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
            final MemorySegment loadedSeg = (segments[si] != null) ? segments[si].segment() : ensureSegmentLoaded(si);

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

            MemorySegment segment = (segments[si] != null) ? segments[si].segment() : ensureSegmentLoaded(si);

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
            MemorySegment segment = (segments[si] != null) ? segments[si].segment() : ensureSegmentLoaded(si);

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

            MemorySegment segment = (segments[si] != null) ? segments[si].segment() : ensureSegmentLoaded(si);

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

            MemorySegment segment = (segments[si] != null) ? segments[si].segment() : ensureSegmentLoaded(si);

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

    /** Builds the actual sliced IndexInput (may apply extra offset in subclasses). * */

    CryptoDirectIOMemoryIndexInput buildSlice(String sliceDescription, long offset, long length) {
        ensureOpen();

        final long sliceEnd = offset + length;
        final int startIndex = (int) (offset >>> chunkSizePower);
        final int endIndex = (int) (sliceEnd >>> chunkSizePower);

        // LAZY LOADING: Ensure all segments in slice range are loaded
        for (int i = startIndex; i <= endIndex; i++) {
            if (segments[i] == null) {
                try {
                    ensureSegmentLoaded(i);
                } catch (IOException e) {
                    throw new RuntimeException(
                        String.format("Failed to load segment %d via DirectIO (range: %d-%d)", i, startIndex, endIndex),
                        e
                    );
                }
            }
        }

        // Copy RefCountedMemorySegment references
        final RefCountedMemorySegment[] slices = ArrayUtil.copyOfSubArray(segments, startIndex, endIndex + 1);

        // Handle the last segment slicing
        if (slices[slices.length - 1] != null) {
            RefCountedMemorySegment originalRefSeg = slices[slices.length - 1];
            MemorySegment slicedSegment = originalRefSeg.segment().asSlice(0L, sliceEnd & chunkSizeMask);

            // Create new RefCountedMemorySegment for sliced view (borrowing, not owning)
            slices[slices.length - 1] = new RefCountedMemorySegment(
                slicedSegment,
                (int) (sliceEnd & chunkSizeMask),
                seg -> { /* No-op releaser - slice doesn't own the memory */ }
            );
        }

        offset = offset & chunkSizeMask;
        final String newResourceDescription = getFullSliceDescription(sliceDescription);

        return new MultiSegmentImpl(
            newResourceDescription,
            path,
            null, // no arena ownership
            blockCache,
            slices, // RefCountedMemorySegment[] with proper boundaries
            offset,
            length,
            chunkSizePower
        );
    }

    @Override
    public final void close() throws IOException {
        if (closed) {
            return;
        }

        closed = true;  // Mark as closed first

        for (RefCountedMemorySegment refSeg : segments) {
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

        curSegment = null;  // Still set to null for NPE on access
        Arrays.fill(segments, null);
    }

    /** This class adds offset support to MemorySegmentIndexInput, which is needed for slices. */
    static final class MultiSegmentImpl extends CryptoDirectIOMemoryIndexInput {
        private final long offset;

        MultiSegmentImpl(
            String resourceDescription,
            Path path,
            Arena arena,
            BlockCache<RefCountedMemorySegment> blockCache,
            RefCountedMemorySegment[] segments,
            long offset,
            long length,
            int chunkSizePower
        ) {
            super(resourceDescription, path, arena, blockCache, segments, length, chunkSizePower);
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

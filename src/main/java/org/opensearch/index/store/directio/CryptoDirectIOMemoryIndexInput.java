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
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;

@SuppressForbidden(reason = "temporary bypass")
@SuppressWarnings("preview")
public class CryptoDirectIOMemoryIndexInput extends IndexInput implements RandomAccessInput {

    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectIOMemoryIndexInput.class);

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
    final RefCountedMemorySegment[] refSegments;
    final long absoluteBaseOffset;
    final int baseSegmentIndex;
    final Path path;
    final BlockCache<RefCountedMemorySegment> blockCache;
    final ReadaheadManager readaheadManager;
    final ReadaheadContext readaheadContext;
    final String resourceDescription;
    final byte[] key;
    final byte[] iv;

    int curSegmentIndex = -1;
    MemorySegment curSegment; // redundant for speed: segments[curSegmentIndex], also marker if closed!
    long curPosition; // relative to curSegment, not globally

    volatile boolean isOpen = true;

    public static CryptoDirectIOMemoryIndexInput newInstance(
        String resourceDescription,
        FileChannel channel,
        Path path,
        Arena arena,
        BlockCache<RefCountedMemorySegment> blockCache,
        ReadaheadManager readAheadManager,
        ReadaheadContext readAheadContext,
        MemorySegment[] segments,
        RefCountedMemorySegment[] refSegments,
        long length,
        int chunkSizePower,
        byte[] key,
        byte[] iv
    ) {

        assert Arrays.stream(segments).map(MemorySegment::scope).allMatch(arena.scope()::equals);

        return new MultiSegmentImpl(
            resourceDescription,
            channel,
            path,
            arena,
            blockCache,
            readAheadManager,
            readAheadContext,
            segments,
            refSegments,
            0L,
            0L,
            length,
            chunkSizePower,
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
        ReadaheadManager readaheadManager,
        ReadaheadContext readaheadContext,
        MemorySegment[] segments,
        RefCountedMemorySegment[] refSegments,
        long absoluteBaseOffset,
        long length,
        int chunkSizePower,
        byte[] key,
        byte[] iv
    ) {
        super(resourceDescription);
        this.channel = channel;
        this.arena = arena;
        this.resourceDescription = resourceDescription;
        this.blockCache = blockCache;
        this.readaheadManager = readaheadManager;
        this.readaheadContext = readaheadContext;
        this.path = path;
        this.segments = segments;
        this.refSegments = refSegments;
        this.absoluteBaseOffset = absoluteBaseOffset;
        this.baseSegmentIndex = (int) (absoluteBaseOffset >> chunkSizePower);
        this.length = length;
        this.chunkSizePower = chunkSizePower;
        this.chunkSizeMask = (1L << chunkSizePower) - 1L;
        this.curSegment = segments[0];
        this.key = key;
        this.iv = iv;
    }

    void ensureOpen() throws AlreadyClosedException {
        if (!isOpen) {
            throw alreadyClosed(null);
        }
    }

    protected long getAbsoluteBaseOffset() {
        // Use the existing getFilePointer() which gives us position relative to this input
        return getFilePointer() + absoluteBaseOffset;
    }

    protected long getAbsoluteBaseOffset(long pos) {
        // pos is relative to this input, add base offset for absolute position
        return pos + absoluteBaseOffset;
    }

    protected int getBaseSegmentIndex() {
        return baseSegmentIndex;
    }

    protected int getBaseSegmentIndex(long pos) {
        return (int) (getAbsoluteBaseOffset(pos) >> chunkSizePower);
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

    // for positioning/seeking (no data needed)
    protected MemorySegment loadSegment(int segmentIndex) throws IOException {
        ensureOpen();
        // Direct mmap for positioning - no cache interaction
        return segments[segmentIndex];
    }

    private MemorySegment loadSegment(int segmentIndex, long fileOffset, int lengthNeeded) throws IOException {
        ensureOpen();

        return segments[segmentIndex];

        // // Already cached
        // if (refSegments != null && refSegments[segmentIndex] != null) {
        // return segments[segmentIndex];
        // }

        // // Align file offset to cache block boundary first
        // final long alignedBlockStart = fileOffset & ~CACHE_BLOCK_MASK;

        // try {
        // final DirectIOBlockCacheKey cacheKey = new DirectIOBlockCacheKey(path, alignedBlockStart);
        // final BlockCacheValue<RefCountedMemorySegment> value = blockCache.getOrLoad(cacheKey);
        // if (value.tryPin()) {
        // LOGGER
        // .debug(
        // "CACHE_HIT thread={} path={} reqOff={} alignedOff={} blockSize={} len={}",
        // Thread.currentThread().getName(),
        // path,
        // fileOffset,
        // alignedBlockStart,
        // CACHE_BLOCK_SIZE
        // );
        // // Hold onto the pinned block until IndexInput is closed.
        // // This guarantees the underlying memory segment stays valid.
        // // We call ref.decRef() in IndexInput.close().
        // RefCountedMemorySegment pinned = value.value();

        // // only owners can pin.
        // if (arena != null && refSegments != null) {
        // refSegments[segmentIndex] = pinned;
        // }

        // segments[segmentIndex] = pinned.segment();

        // return segments[segmentIndex];
        // } else {
        // throw new IOException("Cannot acquire block");
        // }

        // } catch (IOException e) {
        // // Fall back to MMAP.
        // // Note we cannot copy this memory segment into the index input
        // // arena pool because it may end up leaking decryoted bytes in swap space.
        // // todo: Implement an expanding segment pool to be able to acquire more segments
        // // on memory pressure from this addition. If a segment acquire fails from the pool,
        // // we will need to directly decrypt bytes for the read.
        // LOGGER.error("Hit an error will fallback to MMAP {} {}", segmentIndex, fileOffset, e);
        // throw new IOException(e);
        // }
    }

    @Override
    public final byte readByte() throws IOException {
        try {
            long fileOffset = getAbsoluteBaseOffset();
            curSegment = loadSegment(curSegmentIndex, fileOffset, 1);
            final byte v = curSegment.get(LAYOUT_BYTE, curPosition);
            curPosition++;
            return v;
        } catch (@SuppressWarnings("unused") IndexOutOfBoundsException e) {
            do {
                curSegmentIndex++;
                if (curSegmentIndex >= segments.length) {
                    throw new EOFException("read past EOF: " + this);
                }
                long fileOffset = getAbsoluteBaseOffset();
                curSegment = loadSegment(curSegmentIndex, fileOffset, 1);
                curPosition = 0L;
            } while (curSegment.byteSize() == 0L);
            long fileOffset = getAbsoluteBaseOffset();
            curSegment = loadSegment(curSegmentIndex, fileOffset, 1);
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
            long fileOffset = getAbsoluteBaseOffset();
            curSegment = loadSegment(curSegmentIndex, fileOffset, len);
            MemorySegment.copy(curSegment, LAYOUT_BYTE, curPosition, b, offset, len);
            curPosition += len;
        } catch (IndexOutOfBoundsException e) {
            readBytesBoundary(b, offset, len);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        } catch (IOException e) {
            throw new IOException("Decryption or read failed", e);
        }
    }

    private void readBytesBoundary(byte[] b, int offset, int len) throws IOException {
        try {
            long fileEnd = absoluteBaseOffset + length;

            while (len > 0) {
                // Clamp to both segment size and file length
                long curAvail = Math.min(curSegment.byteSize() - curPosition, fileEnd - getAbsoluteBaseOffset());

                if (curAvail <= 0) {
                    // advance to next segment
                    curSegmentIndex++;
                    if (curSegmentIndex >= segments.length) {
                        throw new EOFException("read past EOF: " + this);
                    }
                    curPosition = 0L;
                    long nextFileOffset = getAbsoluteBaseOffset();
                    curSegment = loadSegment(curSegmentIndex, nextFileOffset, len);
                    continue;
                }

                int toCopy = (int) Math.min(len, curAvail);
                MemorySegment.copy(curSegment, LAYOUT_BYTE, curPosition, b, offset, toCopy);

                curPosition += toCopy;
                offset += toCopy;
                len -= toCopy;
            }
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        } catch (IOException e) {
            throw new IOException("Decryption failed", e);
        }
    }

    private void ensureSegmentsForBoundaryCrossing(int elementCount, int elementSize) throws IOException {
        long totalBytes = elementSize * (long) elementCount;
        long fileOffset = getAbsoluteBaseOffset();
        curSegment = loadSegment(curSegmentIndex, fileOffset, (int) totalBytes);

        long currentRemaining = curSegment.byteSize() - curPosition;
        // If read spans beyond current segment, ensure next segments are loaded
        if (totalBytes > currentRemaining) {
            long remainingBytes = totalBytes - currentRemaining;
            int segmentsToLoad = (int) ((remainingBytes + (1L << chunkSizePower) - 1) >>> chunkSizePower);

            for (int i = 1; i <= segmentsToLoad && (curSegmentIndex + i) < segments.length; i++) {
                long bytesForThisSegment = Math.min(remainingBytes, 1L << chunkSizePower);
                // Calculate the absolute file offset for this segment
                long nextSegmentAbsolutePos = ((long) (curSegmentIndex + i) << chunkSizePower);
                long nextSegmentFileOffset = getAbsoluteBaseOffset(nextSegmentAbsolutePos);
                loadSegment(curSegmentIndex + i, nextSegmentFileOffset, (int) bytesForThisSegment);
                remainingBytes -= bytesForThisSegment;
            }
        }
    }

    @Override
    public void readInts(int[] dst, int offset, int length) throws IOException {
        try {
            long totalBytes = Integer.BYTES * (long) length;
            long fileOffset = getAbsoluteBaseOffset();
            curSegment = loadSegment(curSegmentIndex, fileOffset, (int) totalBytes);
            MemorySegment.copy(curSegment, LAYOUT_LE_INT, curPosition, dst, offset, length);
            curPosition += totalBytes;
        } catch (@SuppressWarnings("unused") IndexOutOfBoundsException iobe) {
            // Crossing segment boundaries - decrypt current segment remainder and next segment
            ensureSegmentsForBoundaryCrossing(length, Integer.BYTES);
            super.readInts(dst, offset, length);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public void readLongs(long[] dst, int offset, int length) throws IOException {
        try {
            int totalBytes = Long.BYTES * length;
            long fileOffset = getAbsoluteBaseOffset();
            curSegment = loadSegment(curSegmentIndex, fileOffset, totalBytes);

            MemorySegment.copy(curSegment, LAYOUT_LE_LONG, curPosition, dst, offset, length);
            curPosition += totalBytes;
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
            int totalBytes = Float.BYTES * length;
            long fileOffset = getAbsoluteBaseOffset();
            curSegment = loadSegment(curSegmentIndex, fileOffset, totalBytes);

            MemorySegment.copy(curSegment, LAYOUT_LE_FLOAT, curPosition, dst, offset, length);
            curPosition += totalBytes;
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
            long fileOffset = getAbsoluteBaseOffset();
            curSegment = loadSegment(curSegmentIndex, fileOffset, Short.BYTES);
            final short v = curSegment.get(LAYOUT_LE_SHORT, curPosition);
            curPosition += Short.BYTES;
            return v;
        } catch (@SuppressWarnings("unused") IndexOutOfBoundsException e) {
            ensureSegmentsForBoundaryCrossing(1, Short.BYTES);
            return super.readShort();
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final int readInt() throws IOException {
        try {
            long fileOffset = getAbsoluteBaseOffset();
            curSegment = loadSegment(curSegmentIndex, fileOffset, Integer.BYTES);
            final int v = curSegment.get(LAYOUT_LE_INT, curPosition);
            curPosition += Integer.BYTES;
            return v;
        } catch (@SuppressWarnings("unused") IndexOutOfBoundsException e) {
            ensureSegmentsForBoundaryCrossing(1, Integer.BYTES);
            return super.readInt();
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final long readLong() throws IOException {
        try {
            long fileOffset = getAbsoluteBaseOffset();
            curSegment = loadSegment(curSegmentIndex, fileOffset, Long.BYTES);
            final long v = curSegment.get(LAYOUT_LE_LONG, curPosition);
            curPosition += Long.BYTES;
            return v;
        } catch (@SuppressWarnings("unused") IndexOutOfBoundsException e) {
            ensureSegmentsForBoundaryCrossing(1, Long.BYTES);
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
        final int si = (int) (pos >> chunkSizePower);
        try {
            if (si != curSegmentIndex) {
                final MemorySegment seg = loadSegment(si);
                this.curSegmentIndex = si;
                this.curSegment = seg;
            }

            final long targetPosition = pos & chunkSizeMask;
            curSegment = loadSegment(curSegmentIndex);
            this.curPosition = Objects.checkIndex(targetPosition, curSegment.byteSize() + 1);
        } catch (IndexOutOfBoundsException e) {
            throw handlePositionalIOOBE(e, "seek", pos);
        }
    }

    // used only by random access methods to handle reads across boundaries
    private void setPos(long pos, int si) throws IOException {
        try {
            MemorySegment segment = loadSegment(si);
            this.curPosition = pos & chunkSizeMask;
            this.curSegmentIndex = si;
            this.curSegment = segment;
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
            final long segmentOffset = pos & chunkSizeMask;
            long fileOffset = getAbsoluteBaseOffset(pos);
            MemorySegment segment = loadSegment(si, fileOffset, 1);
            return segment.get(LAYOUT_BYTE, segmentOffset);
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public short readShort(long pos) throws IOException {
        final int si = (int) (pos >> chunkSizePower);
        final long segmentOffset = pos & chunkSizeMask;
        try {
            long fileOffset = getAbsoluteBaseOffset(pos);
            MemorySegment segment = loadSegment(si, fileOffset, Short.BYTES);
            return segment.get(LAYOUT_LE_SHORT, segmentOffset);
        } catch (@SuppressWarnings("unused") IndexOutOfBoundsException ioobe) {
            // either it's a boundary, or read past EOF, fall back:
            setPos(pos, si);
            return readShort();
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }

    }

    @Override
    public int readInt(long pos) throws IOException {
        final int si = (int) (pos >> chunkSizePower);
        final long segmentOffset = pos & chunkSizeMask;

        try {
            // Add decryption before reading
            long fileOffset = getAbsoluteBaseOffset(pos);
            MemorySegment segment = loadSegment(si, fileOffset, Integer.BYTES);
            return segment.get(LAYOUT_LE_INT, segmentOffset);
        } catch (@SuppressWarnings("unused") IndexOutOfBoundsException ioobe) {
            // either it's a boundary, or read past EOF, fall back:
            setPos(pos, si);
            return readInt();
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        } catch (IOException e) {
            throw new IOException("Decryption failed", e);
        }
    }

    @Override
    public long readLong(long pos) throws IOException {
        final int si = (int) (pos >> chunkSizePower);
        final long segmentOffset = pos & chunkSizeMask;

        try {
            // Add decryption before reading
            long fileOffset = getAbsoluteBaseOffset(pos);
            MemorySegment segment = loadSegment(si, fileOffset, Long.BYTES);
            return segment.get(LAYOUT_LE_LONG, segmentOffset);
        } catch (@SuppressWarnings("unused") IndexOutOfBoundsException ioobe) {
            // either it's a boundary, or read past EOF, fall back:
            setPos(pos, si);
            return readLong();
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        } catch (IOException e) {
            throw new IOException("Decryption failed", e);
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
     * Creates a slice of this index input, with the given description, offset,
     * and length. The slice is seeked to the beginning.
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

    /**
     * Builds the actual sliced IndexInput (may apply extra offset in
     * subclasses). *
     */
    CryptoDirectIOMemoryIndexInput buildSlice(String sliceDescription, long offset, long length) {
        ensureOpen();

        // Calculate the absolute file position where this slice starts
        // This is crucial for decryption - we need to know where in the original file we are
        final long sliceAbsoluteOffset = this.absoluteBaseOffset + offset;

        final long sliceEnd = offset + length;
        final int startIndex = (int) (offset >>> chunkSizePower);
        final int endIndex = (int) (sliceEnd >>> chunkSizePower);

        // Ensure we don't go beyond array bounds
        final int actualEndIndex = Math.min(endIndex, segments.length - 1);
        // Copy the segments to prepare our view.
        final MemorySegment slices[] = ArrayUtil.copyOfSubArray(segments, startIndex, actualEndIndex + 1);

        // Set the last segment's limit for the sliced view

        if (slices.length > 0) {
            long lastSegmentLimit = sliceEnd & chunkSizeMask;
            if (lastSegmentLimit > 0) {
                slices[slices.length - 1] = slices[slices.length - 1].asSlice(0L, lastSegmentLimit);
            }
        }

        // Convert offset to position within the first segment
        final long segmentOffset = offset & chunkSizeMask;

        LOGGER
            .debug(
                "Building slice: description={}, parentOffset={}, length={}, " + "absoluteFileOffset={}, segmentOffset={}, startIndex={}",
                sliceDescription,
                offset,
                length,
                sliceAbsoluteOffset,
                segmentOffset,
                startIndex
            );

        final String newResourceDescription = getFullSliceDescription(sliceDescription);

        LOGGER
            .debug(
                "SLICE_CREATED: desc={} sliceLength={} segmentOffset={} sliceAbsOffset={} startIdx={} endIdx={} slicesCount={}",
                sliceDescription,
                length,
                segmentOffset,
                sliceAbsoluteOffset,
                startIndex,
                actualEndIndex,
                slices.length
            );

        return new MultiSegmentImpl(
            newResourceDescription,
            null,  // channel
            path,
            null,  // arena
            blockCache,
            readaheadManager,
            readaheadContext,
            slices,
            null, // reference segment tracking null for slices.
            segmentOffset,
            sliceAbsoluteOffset,
            length,
            chunkSizePower,
            key,
            iv
        );

    }

    @Override
    public final void close() throws IOException {
        if (!isOpen) {
            return; // Already closed
        }

        // Close file channel if this is the master input
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                LOGGER.warn("Failed to close FileChannel for {}", path, e);
            }
        }

        // Close readahead context if this is the master input
        if (readaheadManager != null && readaheadContext != null) {
            try {
                readaheadManager.cancel(readaheadContext);
            } catch (Exception e) {
                LOGGER.warn("Failed to cancel readahead context for {}", path, e);
            }
        }

        // Only the main input owns the Arena and references
        if (arena != null) {
            // Now try to close the Arena
            while (arena.scope().isAlive()) {
                try {
                    arena.close();
                    break;
                } catch (@SuppressWarnings("unused") IllegalStateException e) {
                    Thread.onSpinWait();
                }
            }
        }

        if (refSegments != null) {
            for (RefCountedMemorySegment ref : refSegments) {
                if (ref != null)
                    try {
                        ref.decRef();
                    } catch (Exception e) {
                        LOGGER.warn("Failed to decRef {}", e.toString());
                        throw e;
                    }
            }
        }

        // Null everything out for safety
        curSegment = null;
        Arrays.fill(segments, null);

        isOpen = false;
    }

    /**
     * This class adds offset support to MemorySegmentIndexInput, which is
     * needed for slices.
     */
    static final class MultiSegmentImpl extends CryptoDirectIOMemoryIndexInput {
        private final long offset;
        private final int sliceBaseSegmentIndex;

        MultiSegmentImpl(
            String resourceDescription,
            FileChannel channel,
            Path path,
            Arena arena,
            BlockCache<RefCountedMemorySegment> blockCache,
            ReadaheadManager readaheadManager,
            ReadaheadContext readaheadContext,
            MemorySegment[] segments,
            RefCountedMemorySegment[] refSegments,
            long offset,
            long absoluteBaseOffset,
            long length,
            int chunkSizePower,
            byte[] key,
            byte[] iv
        ) {
            super(
                resourceDescription,
                channel,
                path,
                arena,
                blockCache,
                readaheadManager,
                readaheadContext,
                segments,
                refSegments,
                absoluteBaseOffset,
                length,
                chunkSizePower,
                key,
                iv
            );
            this.offset = offset;
            this.sliceBaseSegmentIndex = (int) (absoluteBaseOffset >> chunkSizePower);
            try {
                seek(0L);
            } catch (IOException ioe) {
                throw new AssertionError(ioe);
            }
            assert curSegment != null && curSegmentIndex >= 0;
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

        @Override
        protected long getAbsoluteBaseOffset() {
            // getFilePointer() already returns position relative to this slice
            return absoluteBaseOffset + offset + super.getFilePointer();
        }

        @Override
        protected long getAbsoluteBaseOffset(long pos) {
            // pos is relative to the slice, we need to add offset
            return absoluteBaseOffset + offset + pos;
        }

        @Override
        protected int getBaseSegmentIndex() {
            return sliceBaseSegmentIndex;
        }

        @Override
        protected int getBaseSegmentIndex(long pos) {
            return (int) (getAbsoluteBaseOffset(pos) >> chunkSizePower);
        }
    }
}

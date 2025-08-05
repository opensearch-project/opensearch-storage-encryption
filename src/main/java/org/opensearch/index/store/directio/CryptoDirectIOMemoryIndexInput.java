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
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.BlockLoader;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_MASK;
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE;

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
    final Path path;
    final BlockCache<RefCountedMemorySegment> blockCache;
    final BlockLoader<RefCountedMemorySegment> blockLoader;
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
        BlockLoader<RefCountedMemorySegment> blockLoader,
        MemorySegment[] segments,
        RefCountedMemorySegment[] refSegments,
        long length,
        int chunkSizePower,
        byte[] key,
        byte[] iv
    ) {

        assert Arrays.stream(segments).map(MemorySegment::scope).allMatch(arena.scope()::equals);

        if (segments.length == 1) {
            return new SingleSegmentImpl(
                resourceDescription,
                channel,
                path,
                arena,
                blockCache,
                blockLoader,
                segments[0],
                refSegments,
                0L,
                length,
                chunkSizePower,
                key,
                iv
            );
        } else {
            return new MultiSegmentImpl(
                resourceDescription,
                channel,
                path,
                arena,
                blockCache,
                blockLoader,
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

    }

    private CryptoDirectIOMemoryIndexInput(
        String resourceDescription,
        FileChannel channel,
        Path path,
        Arena arena,
        BlockCache<RefCountedMemorySegment> blockCache,
        BlockLoader<RefCountedMemorySegment> blockLoader,
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
        this.blockLoader = blockLoader;
        this.path = path;
        this.segments = segments;
        this.refSegments = refSegments;
        this.absoluteBaseOffset = absoluteBaseOffset;
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

    private void fillBlockCacheForRange(long fileOffset, long lengthNeeded) throws IOException {
        if (lengthNeeded == 0)
            return;

        long alignedBlockStart = fileOffset & ~CACHE_BLOCK_MASK;
        long requestEnd = fileOffset + lengthNeeded;
        long alignedBlockEnd = ((requestEnd + CACHE_BLOCK_SIZE - 1) & ~CACHE_BLOCK_MASK);

        int blocksProcessed = 0;
        long startTime = System.nanoTime();

        for (long blockOffset = alignedBlockStart; blockOffset < alignedBlockEnd; blockOffset += CACHE_BLOCK_SIZE) {
            DirectIOBlockCacheKey cacheKey = new DirectIOBlockCacheKey(path, blockOffset);

            try {
                blockCache.getOrLoad(cacheKey, CACHE_BLOCK_SIZE, blockLoader);
                blocksProcessed++;
            } catch (IOException e) {
                LOGGER.error("Failed to access cache for block at offset {}: {}", blockOffset, e);
            }
        }

        long durationMicros = (System.nanoTime() - startTime) / 1_000;
        LOGGER
            .info(
                "Cache fill for {} (offset={}, length={}): blocks={}, took={} us",
                path.getFileName(),
                fileOffset,
                lengthNeeded,
                blocksProcessed,
                durationMicros
            );
    }

    // for positioning/seeking (no data needed)
    protected MemorySegment loadSegment(int segmentIndex) throws IOException {
        ensureOpen();
        // Direct mmap for positioning - no cache interaction
        return segments[segmentIndex];
    }

    protected MemorySegment loadSegment(int segmentIndex, long fileOffset, int lengthNeeded) throws IOException {
        ensureOpen();

        if (lengthNeeded < CACHE_BLOCK_SIZE) {
            // Small read - always use mmap
            return segments[segmentIndex];
        } else {
            // Check if this segment was already upgraded to cached version
            if (refSegments[segmentIndex] != null) {
                // Already cached - no lookup needed!
                return segments[segmentIndex];
            }

            // Large read - cache-first strategy
            if (blockCache != null) {
                long alignedBlockStart = fileOffset & ~CACHE_BLOCK_MASK;
                DirectIOBlockCacheKey cacheKey = new DirectIOBlockCacheKey(path, alignedBlockStart);

                Optional<BlockCacheValue<RefCountedMemorySegment>> cached = blockCache.get(cacheKey);

                if (cached.isPresent()) {
                    RefCountedMemorySegment refSeg = cached.get().block();
                    segments[segmentIndex] = refSeg.segment();
                    refSegments[segmentIndex] = refSeg;  // Mark as upgraded

                    return segments[segmentIndex];
                }
            }

            return segments[segmentIndex];  // fallback to mmap
        }
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
        long startFileOffset = getAbsoluteBaseOffset();
        int originalLen = len;
        try {
            long curAvail = curSegment.byteSize() - curPosition;
            while (len > curAvail) {
                long fileOffset = startFileOffset + (originalLen - len);
                curSegment = loadSegment(curSegmentIndex, fileOffset, len);
                MemorySegment.copy(curSegment, LAYOUT_BYTE, curPosition, b, offset, (int) curAvail);
                len -= curAvail;
                offset += curAvail;
                curSegmentIndex++;
                if (curSegmentIndex >= segments.length) {
                    throw new EOFException("read past EOF: " + this);
                }
                curPosition = 0L;
                curAvail = curSegment.byteSize();
            }

            long fileOffset = startFileOffset + (originalLen - len);
            curSegment = loadSegment(curSegmentIndex, fileOffset, len);
            MemorySegment.copy(curSegment, LAYOUT_BYTE, curPosition, b, offset, len);
            curPosition += len;
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        } catch (IOException e) {
            throw new IOException("Decryption failed", e);
        }
    }

    private void ensureSegmentsForBoundaryCrossing(int elementCount, int elementSize) throws IOException {
        long totalBytes = elementSize * (long) elementCount;
        long fileOffset = absoluteBaseOffset + ((long) curSegmentIndex << chunkSizePower) + curPosition;
        curSegment = loadSegment(curSegmentIndex, fileOffset, (int) totalBytes);

        long currentRemaining = curSegment.byteSize() - curPosition;
        // If read spans beyond current segment, ensure next segments are loaded
        if (totalBytes > currentRemaining) {
            long remainingBytes = totalBytes - currentRemaining;
            int segmentsToLoad = (int) ((remainingBytes + (1L << chunkSizePower) - 1) >>> chunkSizePower);

            for (int i = 1; i <= segmentsToLoad && (curSegmentIndex + i) < segments.length; i++) {
                long bytesForThisSegment = Math.min(remainingBytes, 1L << chunkSizePower);
                long nextSegmentFileOffset = absoluteBaseOffset + ((long) (curSegmentIndex + i) << chunkSizePower);
                loadSegment(curSegmentIndex + i, nextSegmentFileOffset, (int) bytesForThisSegment);
                remainingBytes -= bytesForThisSegment;
            }
        }
    }

    @Override
    public void readInts(int[] dst, int offset, int length) throws IOException {
        try {
            int totalBytes = Integer.BYTES * length;
            long fileOffset = getAbsoluteBaseOffset();
            curSegment = loadSegment(curSegmentIndex, fileOffset, totalBytes);
            MemorySegment.copy(curSegment, LAYOUT_LE_INT, curPosition, dst, offset, length);
            curPosition += totalBytes;
        } catch (@SuppressWarnings("unused") IndexOutOfBoundsException iobe) {
            // Crossing segment boundaries - decrypt current segment remainder and next segment
            // Decrypt remainder of current segment
            ensureSegmentsForBoundaryCrossing(length, Integer.BYTES);
            super.readInts(dst, offset, length);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public void readLongs(long[] dst, int offset, int length) throws IOException {
        try {
            // Decrypt the range we're about to read
            int totalBytes = Long.BYTES * length;
            long fileOffset = getAbsoluteBaseOffset();
            curSegment = loadSegment(curSegmentIndex, fileOffset, totalBytes);

            MemorySegment.copy(curSegment, LAYOUT_LE_LONG, curPosition, dst, offset, length);
            curPosition += totalBytes;
        } catch (@SuppressWarnings("unused") IndexOutOfBoundsException iobe) {
            // Crossing segment boundaries - decrypt segments then delegate to super
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
            // Crossing segment boundaries - decrypt segments then delegate to super
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
            // Crossing segment boundaries - ensure segments are loaded then delegate to super
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
            ensureSegmentsForBoundaryCrossing(1, Short.BYTES);

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

        if (slices.length == 1) {

            return new SingleSegmentImpl(
                newResourceDescription,
                null,  // channel
                path,
                null,  // arena
                blockCache,
                blockLoader,
                slices[0].asSlice(segmentOffset, length),
                null, // reference segment tracking null for slices.
                sliceAbsoluteOffset,
                length,
                chunkSizePower,
                key,
                iv
            );

        } else {
            return new MultiSegmentImpl(
                newResourceDescription,
                null,  // channel
                path,
                null,  // arena
                blockCache,
                blockLoader,
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
     * Optimization of MemorySegmentIndexInput for when there is only one
     * segment.
     */
    static final class SingleSegmentImpl extends CryptoDirectIOMemoryIndexInput {
        SingleSegmentImpl(
            String resourceDescription,
            FileChannel channel,
            Path path,
            Arena arena,
            BlockCache<RefCountedMemorySegment> blockCache,
            BlockLoader<RefCountedMemorySegment> blockLoader,
            MemorySegment segment,
            RefCountedMemorySegment[] refSegments,
            long absoluteBaseOffset,    // absolute file offset of first segment
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
                blockLoader,
                new MemorySegment[] { segment },
                refSegments,
                absoluteBaseOffset,
                length,
                chunkSizePower,
                key,
                iv
            );
            this.curSegmentIndex = 0;
        }

        @Override
        public void seek(long pos) throws IOException {
            ensureOpen();
            try {
                curPosition = Objects.checkIndex(pos, length + 1);
            } catch (IndexOutOfBoundsException e) {
                throw handlePositionalIOOBE(e, "seek", pos);
            }
        }

        @Override
        public long getFilePointer() {
            ensureOpen();
            return curPosition;
        }

        @Override
        public byte readByte(long pos) throws IOException {
            try {
                // For single segment, pos is the absolute file position
                curSegment = super.loadSegment(0, getAbsoluteBaseOffset(pos), 1);
                return curSegment.get(LAYOUT_BYTE, pos);
            } catch (IndexOutOfBoundsException e) {
                throw handlePositionalIOOBE(e, "read", pos);
            } catch (NullPointerException | IllegalStateException e) {
                throw alreadyClosed(e);
            }
        }

        @Override
        public short readShort(long pos) throws IOException {
            try {
                curSegment = super.loadSegment(0, getAbsoluteBaseOffset(pos), 2);
                return curSegment.get(LAYOUT_LE_SHORT, pos);
            } catch (IndexOutOfBoundsException e) {
                throw handlePositionalIOOBE(e, "read", pos);
            } catch (NullPointerException | IllegalStateException e) {
                throw alreadyClosed(e);
            }
        }

        @Override
        public int readInt(long pos) throws IOException {
            try {
                // Decrypt 4 bytes for int
                long addr = curSegment.address() + pos;
                curSegment = super.loadSegment(0, getAbsoluteBaseOffset(pos), 4);
                return curSegment.get(LAYOUT_LE_INT, pos);
            } catch (IndexOutOfBoundsException e) {
                throw handlePositionalIOOBE(e, "read", pos);
            } catch (NullPointerException | IllegalStateException e) {
                throw alreadyClosed(e);
            } catch (IOException e) {
                throw new IOException("Decryption failed", e);
            }
        }

        @Override
        public long readLong(long pos) throws IOException {
            try {
                curSegment = super.loadSegment(0, getAbsoluteBaseOffset(pos), 8);
                return curSegment.get(LAYOUT_LE_LONG, pos);
            } catch (IndexOutOfBoundsException e) {
                throw handlePositionalIOOBE(e, "read", pos);
            } catch (NullPointerException | IllegalStateException e) {
                throw alreadyClosed(e);
            } catch (IOException e) {
                throw new IOException("Decryption failed", e);
            }
        }
    }

    /**
     * This class adds offset support to MemorySegmentIndexInput, which is
     * needed for slices.
     */
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
                blockLoader,
                segments,
                refSegments,
                absoluteBaseOffset,
                length,
                chunkSizePower,
                key,
                iv
            );
            this.offset = offset;
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
    }
}

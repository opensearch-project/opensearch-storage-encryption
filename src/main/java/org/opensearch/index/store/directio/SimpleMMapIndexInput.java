/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_MASK;
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.EOFException;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;

@SuppressWarnings("preview")
public class SimpleMMapIndexInput extends IndexInput implements RandomAccessInput {
    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectIODirectory.class);

    static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;
    static final ValueLayout.OfShort LAYOUT_LE_SHORT = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfInt LAYOUT_LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfLong LAYOUT_LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfFloat LAYOUT_LE_FLOAT = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    final long length;
    final long chunkSizeMask;
    final int chunkSizePower;

    final Path path;
    final Arena arena;
    final BlockCache<RefCountedMemorySegment> blockCache;

    final PinRegistry registry;

    final long absoluteBaseOffset; // absolute position in original file where this input starts
    final boolean isSlice; // true for slices, false for main instances

    long curPosition = 0L; // absolute position within this input (0-based)
    volatile boolean isOpen = true;

    public static SimpleMMapIndexInput newInstance(
        String resourceDescription,
        Path path,
        Arena arena,
        long length,
        BlockCache<RefCountedMemorySegment> blockCache,
        PinRegistry registry
    ) {

        return new MultiSegmentImpl(resourceDescription, path, arena, 0, 0, length, blockCache, registry, false);
    }

    private SimpleMMapIndexInput(
        String resourceDescription,
        Path path,
        Arena arena,
        long absoluteBaseOffset,
        long length,
        BlockCache<RefCountedMemorySegment> blockCache,
        PinRegistry registry,
        boolean isSlice
    ) {
        super(resourceDescription);
        this.path = path;
        this.arena = arena;
        this.absoluteBaseOffset = absoluteBaseOffset;
        this.length = length;
        this.chunkSizePower = CACHE_BLOCK_SIZE_POWER;
        this.chunkSizeMask = CACHE_BLOCK_MASK;
        this.blockCache = blockCache;
        this.registry = registry;
        this.isSlice = isSlice;
    }

    void ensureOpen() {
        if (!isOpen) {
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

    /**
    * Helper method to get cache block for a given position.
    * Handles block alignment, cache loading, and tracks pinned blocks for cleanup.
    *
    * @param pos position relative to this input
    * @return MemorySegment for the cache block containing the position
    * @throws IOException if cache loading fails
    */
    private MemorySegment getCacheBlock(long pos) throws IOException {
        ensureOpen();
        long fileOff = getAbsoluteFileOffset(pos);
        long blockOff = fileOff & ~CACHE_BLOCK_MASK;
        return registry.acquire(blockOff); // always pinned or throws
    }

    @Override
    public final byte readByte() throws IOException {
        ensureOpen();

        final long currentPos = getFilePointer();
        try {
            final MemorySegment seg = getCacheBlock(currentPos);
            final int offInBlock = getOffsetInBlock(currentPos);
            final byte v = seg.get(LAYOUT_BYTE, offInBlock);
            curPosition++;
            return v;
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", currentPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final void readBytes(byte[] b, int offset, int len) throws IOException {
        ensureOpen();

        if (len == 0)
            return;

        final long startPos = getFilePointer();
        int remaining = len;
        int bufferOffset = offset;
        long currentPos = startPos;

        try {
            while (remaining > 0) {
                final MemorySegment seg = getCacheBlock(currentPos);
                final int offInBlock = getOffsetInBlock(currentPos);
                final int availableInBlock = (int) (seg.byteSize() - offInBlock);
                final int toRead = Math.min(remaining, availableInBlock);

                // Copy from cache block to output buffer
                MemorySegment.copy(seg, LAYOUT_BYTE, offInBlock, b, bufferOffset, toRead);

                remaining -= toRead;
                bufferOffset += toRead;
                currentPos += toRead;
            }

            // Update position after successful read
            curPosition += len;

        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", startPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public void readInts(int[] dst, int offset, int length) throws IOException {
        ensureOpen();

        if (length == 0)
            return;

        final long startPos = getFilePointer();
        final long totalBytes = Integer.BYTES * (long) length;

        try {
            final MemorySegment seg = getCacheBlock(startPos);
            final int offInBlock = getOffsetInBlock(startPos);

            // Check if entire read fits in current cache block
            if (offInBlock + totalBytes <= seg.byteSize()) {
                // Fast path: entire read fits in one cache block
                MemorySegment.copy(seg, LAYOUT_LE_INT, offInBlock, dst, offset, length);
                curPosition += totalBytes;
            } else {
                // Slow path: spans cache blocks, fall back to super implementation
                super.readInts(dst, offset, length);
            }
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", startPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public void readLongs(long[] dst, int offset, int length) throws IOException {
        ensureOpen();

        if (length == 0)
            return;

        final long startPos = getFilePointer();
        final long totalBytes = Long.BYTES * (long) length;

        try {
            final MemorySegment seg = getCacheBlock(startPos);
            final int offInBlock = getOffsetInBlock(startPos);

            // Check if entire read fits in current cache block
            if (offInBlock + totalBytes <= seg.byteSize()) {
                // Fast path: entire read fits in one cache block
                MemorySegment.copy(seg, LAYOUT_LE_LONG, offInBlock, dst, offset, length);
                curPosition += totalBytes;
            } else {
                // Slow path: spans cache blocks, fall back to super implementation
                super.readLongs(dst, offset, length);
            }
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", startPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public void readFloats(float[] dst, int offset, int length) throws IOException {
        ensureOpen();

        if (length == 0)
            return;

        final long startPos = getFilePointer();
        final long totalBytes = Float.BYTES * (long) length;

        try {
            final MemorySegment seg = getCacheBlock(startPos);
            final int offInBlock = getOffsetInBlock(startPos);

            // Check if entire read fits in current cache block
            if (offInBlock + totalBytes <= seg.byteSize()) {
                // Fast path: entire read fits in one cache block
                MemorySegment.copy(seg, LAYOUT_LE_FLOAT, offInBlock, dst, offset, length);
                curPosition += totalBytes;
            } else {
                // Slow path: spans cache blocks, fall back to super implementation
                super.readFloats(dst, offset, length);
            }
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", startPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final short readShort() throws IOException {
        ensureOpen();

        final long currentPos = getFilePointer();
        try {
            final MemorySegment seg = getCacheBlock(currentPos);
            final int offInBlock = getOffsetInBlock(currentPos);

            // Check if the short spans beyond the current cache block
            if (offInBlock + Short.BYTES > seg.byteSize()) {
                // Read spans cache block boundary, fall back to super implementation
                return super.readShort();
            }

            final short v = seg.get(LAYOUT_LE_SHORT, offInBlock);
            curPosition += Short.BYTES;
            return v;
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", currentPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final int readInt() throws IOException {
        ensureOpen();

        final long currentPos = getFilePointer();
        try {
            final MemorySegment seg = getCacheBlock(currentPos);
            final int offInBlock = getOffsetInBlock(currentPos);

            // Check if the int spans beyond the current cache block
            if (offInBlock + Integer.BYTES > seg.byteSize()) {
                // Read spans cache block boundary, fall back to super implementation
                return super.readInt();
            }

            final int v = seg.get(LAYOUT_LE_INT, offInBlock);
            curPosition += Integer.BYTES;
            return v;
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", currentPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final long readLong() throws IOException {
        ensureOpen();

        final long currentPos = getFilePointer();
        try {
            final MemorySegment seg = getCacheBlock(currentPos);
            final int offInBlock = getOffsetInBlock(currentPos);

            // Check if the long spans beyond the current cache block
            if (offInBlock + Long.BYTES > seg.byteSize()) {
                // Read spans cache block boundary, fall back to super implementation
                return super.readLong();
            }

            final long v = seg.get(LAYOUT_LE_LONG, offInBlock);
            curPosition += Long.BYTES;
            return v;
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", currentPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public long getFilePointer() {
        ensureOpen();
        return curPosition;
    }

    /**
     * Returns the absolute file offset for the current position.
     * This is useful for cache keys, encryption, and other operations that need
     * the actual position in the original file.
     */
    public long getAbsoluteFileOffset() {
        return absoluteBaseOffset + getFilePointer();
    }

    /**
     * Returns the absolute file offset for a given position within this input.
     * This is useful for cache keys, encryption, and other operations that need
     * the actual position in the original file for random access operations.
     * 
     * @param pos position relative to this input (0-based)
     * @return absolute position in the original file
     */
    public long getAbsoluteFileOffset(long pos) {
        return absoluteBaseOffset + pos;
    }

    /**
     * Helper method to get offset within cache block for a given position.
     * 
     * @param pos position relative to this input
     * @return offset within the cache block
     */
    private int getOffsetInBlock(long pos) {
        final long abs = getAbsoluteFileOffset(pos);
        final long blockOff = abs & ~CACHE_BLOCK_MASK;
        return (int) (abs - blockOff);
    }

    @Override
    public void seek(long pos) throws IOException {
        ensureOpen();
        if (pos < 0 || pos > length) {
            throw handlePositionalIOOBE(null, "seek", pos);
        }
        this.curPosition = pos;
    }

    @Override
    public byte readByte(long pos) throws IOException {
        ensureOpen();

        if (pos < 0 || pos >= length) {
            return 0;
        }

        try {
            final MemorySegment seg = getCacheBlock(pos);
            final int offInBlock = getOffsetInBlock(pos);
            return seg.get(LAYOUT_BYTE, offInBlock);
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public short readShort(long pos) throws IOException {
        ensureOpen();

        final MemorySegment seg = getCacheBlock(pos);
        final int offInBlock = getOffsetInBlock(pos);

        try {
            // Check if the short spans beyond the current cache block
            if (offInBlock + Short.BYTES > seg.byteSize()) {
                // Read spans cache block boundary, fall back to byte-by-byte
                byte b1 = readByte(pos);
                byte b2 = readByte(pos + 1);
                return (short) ((b2 << 8) | (b1 & 0xFF)); // Little endian
            }
            return seg.get(LAYOUT_LE_SHORT, offInBlock);
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public int readInt(long pos) throws IOException {
        ensureOpen();

        final MemorySegment seg = getCacheBlock(pos);
        final int offInBlock = getOffsetInBlock(pos);

        try {
            // Check if the int spans beyond the current cache block
            if (offInBlock + Integer.BYTES > seg.byteSize()) {
                // Read spans cache block boundary, fall back to byte-by-byte
                byte b1 = readByte(pos);
                byte b2 = readByte(pos + 1);
                byte b3 = readByte(pos + 2);
                byte b4 = readByte(pos + 3);
                return (b4 << 24) | ((b3 & 0xFF) << 16) | ((b2 & 0xFF) << 8) | (b1 & 0xFF); // Little endian
            }
            return seg.get(LAYOUT_LE_INT, offInBlock);
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public long readLong(long pos) throws IOException {
        ensureOpen();

        final MemorySegment seg = getCacheBlock(pos);
        final int offInBlock = getOffsetInBlock(pos);

        try {
            // Check if the long spans beyond the current cache block
            if (offInBlock + Long.BYTES > seg.byteSize()) {
                // Read spans cache block boundary, fall back to two ints
                int lowInt = readInt(pos);      // Low 32 bits
                int highInt = readInt(pos + 4); // High 32 bits
                return ((long) highInt << 32) | ((long) lowInt & 0xFFFFFFFFL); // Little endian
            }
            return seg.get(LAYOUT_LE_LONG, offInBlock);
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final long length() {
        return length;
    }

    @Override
    public final SimpleMMapIndexInput clone() {
        final SimpleMMapIndexInput clone = buildSlice((String) null, 0L, this.length);
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
    public final SimpleMMapIndexInput slice(String sliceDescription, long offset, long length) throws IOException {
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

        var slice = buildSlice(sliceDescription, offset, length);

        slice.seek(0L);

        return slice;
    }

    /** Builds the actual sliced IndexInput (may apply extra offset in subclasses). * */
    SimpleMMapIndexInput buildSlice(String sliceDescription, long sliceOffset, long length) {
        ensureOpen();
        // Calculate absolute base offset for the slice
        final long sliceAbsoluteBaseOffset = this.absoluteBaseOffset + sliceOffset;

        LOGGER
            .debug(
                "BUILD_SLICE: desc={} sliceOffset={} length={} parentAbsBase={} sliceAbsBase={}",
                sliceDescription,
                sliceOffset,
                length,
                this.absoluteBaseOffset,
                sliceAbsoluteBaseOffset
            );

        final String newResourceDescription = getFullSliceDescription(sliceDescription);

        return new MultiSegmentImpl(
            newResourceDescription,
            path,
            null, // clones don't have an Arena, as they can't close)
            0, // slice offset is always 0 (slice starts at its beginning)
            sliceAbsoluteBaseOffset,
            length,
            blockCache,
            registry.retainOwner(), // slices retain ownership
            true
        );
    }

    @Override
    public final void close() throws IOException {
        if (!isOpen) {
            return;
        }

        // Mark as closed to ensure all future accesses throw AlreadyClosedException
        isOpen = false;

        // the master IndexInput has an Arena and is able
        // to release all resources (unmap segments) - a
        // side effect is that other threads still using clones
        // will throw IllegalStateException
        if (arena != null) {
            // Assertions for master instance
            assert !isSlice : "Master instance should not be marked as slice";

            while (arena.scope().isAlive()) {
                try {
                    arena.close();
                    break;
                } catch (@SuppressWarnings("unused") IllegalStateException e) {
                    Thread.onSpinWait();
                }
            }

            // master cleanup: drop all pinned blocks
            registry.releaseOwners();
        } else {
            // Assertions for slice instance
            assert isSlice : "Slice instance should be marked as slice";

            // slice/clone cleanup: just drop one reference
            registry.releaseOwner();
        }
    }

    /** This class adds offset support to MemorySegmentIndexInput, which is needed for slices. */
    static final class MultiSegmentImpl extends SimpleMMapIndexInput {
        private final long offset;

        MultiSegmentImpl(
            String resourceDescription,
            Path path,
            Arena arena,
            long offset,
            long absoluteBaseOffset,
            long length,
            BlockCache<RefCountedMemorySegment> blockCache,
            PinRegistry pinRegistry,
            boolean isSlice
        ) {
            super(resourceDescription, path, arena, absoluteBaseOffset, length, blockCache, pinRegistry, isSlice);
            this.offset = offset;
            try {
                seek(0L);
            } catch (IOException ioe) {
                throw new AssertionError(ioe);
            }
            assert isOpen;
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
        SimpleMMapIndexInput buildSlice(String sliceDescription, long ofs, long length) {
            return super.buildSlice(sliceDescription, this.offset + ofs, length);
        }

        @Override
        public long getAbsoluteFileOffset() {
            return absoluteBaseOffset + getFilePointer();
        }

        @Override
        public long getAbsoluteFileOffset(long pos) {
            // pos is relative to the slice, we need to add offset
            return absoluteBaseOffset + offset + pos;
        }
    }
}

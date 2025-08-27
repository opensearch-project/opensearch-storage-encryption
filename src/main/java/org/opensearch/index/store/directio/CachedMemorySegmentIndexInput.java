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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_MASK;
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE;

@SuppressWarnings("preview")
public class CachedMemorySegmentIndexInput extends IndexInput implements RandomAccessInput {
    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectIODirectory.class);

    static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;
    static final ValueLayout.OfShort LAYOUT_LE_SHORT = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfInt LAYOUT_LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfLong LAYOUT_LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfFloat LAYOUT_LE_FLOAT = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    final long length;

    final Path path;
    final Arena arena;
    final BlockCache<RefCountedMemorySegment> blockCache;

    final PinRegistry registry;

    final long absoluteBaseOffset; // absolute position in original file where this input starts
    final boolean isSlice; // true for slices, false for main instances

    long curPosition = 0L; // absolute position within this input (0-based)
    volatile boolean isOpen = true;

    // 2-entry cache for last accessed blocks to smooth boundary crossings
    private volatile long cacheBlockOffset1 = -1;
    private volatile MemorySegment cacheBlock1 = null;
    private volatile long cacheBlockOffset2 = -1;
    private volatile MemorySegment cacheBlock2 = null;

    /**
     * Helper class to encapsulate position calculations and avoid redundant arithmetic.
     * Combines file offset, block offset, and offset-in-block calculations.
     */
    private static final class BlockPosition {
        final long fileOffset;
        final long blockOffset;
        final int offsetInBlock;

        BlockPosition(long pos, long absoluteBaseOffset) {
            this.fileOffset = absoluteBaseOffset + pos;
            this.blockOffset = fileOffset & ~CACHE_BLOCK_MASK;
            this.offsetInBlock = (int) (fileOffset - blockOffset);
        }
    }

    public static CachedMemorySegmentIndexInput newInstance(
        String resourceDescription,
        Path path,
        Arena arena,
        long length,
        BlockCache<RefCountedMemorySegment> blockCache,
        PinRegistry registry
    ) {

        return new MultiSegmentImpl(resourceDescription, path, arena, 0, 0, length, blockCache, registry, false);
    }

    private CachedMemorySegmentIndexInput(
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
     * Helper class to return both cache block and offset in one operation.
     */
    private static final class BlockAccess {
        final MemorySegment segment;
        final int offsetInBlock;

        BlockAccess(MemorySegment segment, int offsetInBlock) {
            this.segment = segment;
            this.offsetInBlock = offsetInBlock;
        }
    }


    /**
     * Optimized method to get both cache block and offset in one operation.
     * Eliminates redundant position calculations between getCacheBlock() and getOffsetInBlock().
     *
     * @param pos position relative to this input
     * @return BlockAccess containing both segment and offset
     * @throws IOException if cache loading fails
     */
    private BlockAccess getCacheBlockWithOffset(long pos) throws IOException {
        ensureOpen();
        BlockPosition blockPos = new BlockPosition(pos, absoluteBaseOffset);

        // Check cache entries
        if (blockPos.blockOffset == cacheBlockOffset1 && cacheBlock1 != null) {
            return new BlockAccess(cacheBlock1, blockPos.offsetInBlock);
        }
        if (blockPos.blockOffset == cacheBlockOffset2 && cacheBlock2 != null) {
            return new BlockAccess(cacheBlock2, blockPos.offsetInBlock);
        }

        // Acquire new block and cache it using LRU replacement
        MemorySegment block = registry.acquire(blockPos.blockOffset, length); // always pinned or throws

        // Shift cache: entry 1 becomes entry 2, new block becomes entry 1
        cacheBlockOffset2 = cacheBlockOffset1;
        cacheBlock2 = cacheBlock1;
        cacheBlockOffset1 = blockPos.blockOffset;
        cacheBlock1 = block;

        return new BlockAccess(block, blockPos.offsetInBlock);
    }

    @Override
    public final byte readByte() throws IOException {
        final long currentPos = getFilePointer();
        try {
            final BlockAccess access = getCacheBlockWithOffset(currentPos);
            final byte v = access.segment.get(LAYOUT_BYTE, access.offsetInBlock);
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
        if (len == 0)
            return;

        final long startPos = curPosition; // avoid virtual call
        int remaining = len;
        int bufferOffset = offset;
        long currentPos = startPos;

        try {
            while (remaining > 0) {
                final BlockAccess a = getCacheBlockWithOffset(currentPos);
                final MemorySegment seg = a.segment;
                final int offInBlock = a.offsetInBlock;
                final int avail = (int) (seg.byteSize() - offInBlock);

                // Fast path: block-aligned and large
                if (offInBlock == 0 && remaining >= CACHE_BLOCK_SIZE && seg.byteSize() >= CACHE_BLOCK_SIZE) {
                    // Copy current full block
                    MemorySegment.copy(seg, LAYOUT_BYTE, 0L, b, bufferOffset, CACHE_BLOCK_SIZE);
                    remaining -= CACHE_BLOCK_SIZE;
                    bufferOffset += CACHE_BLOCK_SIZE;
                    currentPos += CACHE_BLOCK_SIZE;
                    continue; // Loop to next iteration
                }

                // Partial block path (start or end of range, or short final block)
                final int toRead = Math.min(remaining, avail);
                MemorySegment.copy(seg, LAYOUT_BYTE, (long) offInBlock, b, bufferOffset, toRead);
                remaining -= toRead;
                bufferOffset += toRead;
                currentPos += toRead;
            }

            curPosition = startPos + len; // single write

        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", startPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public void readInts(int[] dst, int offset, int length) throws IOException {
        if (length == 0)
            return;

        final long startPos = getFilePointer();
        final long totalBytes = Integer.BYTES * (long) length;

        try {
            final BlockAccess access = getCacheBlockWithOffset(startPos);

            // Check if entire read fits in current cache block
            if (access.offsetInBlock + totalBytes <= access.segment.byteSize()) {
                // Fast path: entire read fits in one cache block
                MemorySegment.copy(access.segment, LAYOUT_LE_INT, access.offsetInBlock, dst, offset, length);
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
        if (length == 0)
            return;

        final long startPos = getFilePointer();
        final long totalBytes = Long.BYTES * (long) length;

        try {
            final BlockAccess access = getCacheBlockWithOffset(startPos);

            // Check if entire read fits in current cache block
            if (access.offsetInBlock + totalBytes <= access.segment.byteSize()) {
                // Fast path: entire read fits in one cache block
                MemorySegment.copy(access.segment, LAYOUT_LE_LONG, access.offsetInBlock, dst, offset, length);
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
        if (length == 0)
            return;

        final long startPos = getFilePointer();
        final long totalBytes = Float.BYTES * (long) length;

        try {
            final BlockAccess access = getCacheBlockWithOffset(startPos);

            // Check if entire read fits in current cache block
            if (access.offsetInBlock + totalBytes <= access.segment.byteSize()) {
                // Fast path: entire read fits in one cache block
                MemorySegment.copy(access.segment, LAYOUT_LE_FLOAT, access.offsetInBlock, dst, offset, length);
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
        final long currentPos = getFilePointer();
        try {
            final BlockAccess access = getCacheBlockWithOffset(currentPos);

            // Check if the short spans beyond the current cache block
            if (access.offsetInBlock + Short.BYTES > access.segment.byteSize()) {
                // Read spans cache block boundary, fall back to super implementation
                return super.readShort();
            }

            final short v = access.segment.get(LAYOUT_LE_SHORT, access.offsetInBlock);
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
        final long currentPos = getFilePointer();
        try {
            final BlockAccess access = getCacheBlockWithOffset(currentPos);

            // Check if the int spans beyond the current cache block
            if (access.offsetInBlock + Integer.BYTES > access.segment.byteSize()) {
                // Read spans cache block boundary, fall back to super implementation
                return super.readInt();
            }

            final int v = access.segment.get(LAYOUT_LE_INT, access.offsetInBlock);
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
        final long currentPos = getFilePointer();
        try {
            final BlockAccess access = getCacheBlockWithOffset(currentPos);

            // Check if the long spans beyond the current cache block
            if (access.offsetInBlock + Long.BYTES > access.segment.byteSize()) {
                // Read spans cache block boundary, fall back to super implementation
                return super.readLong();
            }

            final long v = access.segment.get(LAYOUT_LE_LONG, access.offsetInBlock);
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
        if (pos < 0 || pos >= length) {
            return 0;
        }

        try {
            final BlockAccess access = getCacheBlockWithOffset(pos);
            return access.segment.get(LAYOUT_BYTE, access.offsetInBlock);
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public short readShort(long pos) throws IOException {
        try {
            final BlockAccess access = getCacheBlockWithOffset(pos);

            // Check if the short spans beyond the current cache block
            if (access.offsetInBlock + Short.BYTES > access.segment.byteSize()) {
                // Read spans cache block boundary, fall back to byte-by-byte
                byte b1 = readByte(pos);
                byte b2 = readByte(pos + 1);
                return (short) ((b2 << 8) | (b1 & 0xFF)); // Little endian
            }
            return access.segment.get(LAYOUT_LE_SHORT, access.offsetInBlock);
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public int readInt(long pos) throws IOException {
        try {
            final BlockAccess access = getCacheBlockWithOffset(pos);

            // Check if the int spans beyond the current cache block
            if (access.offsetInBlock + Integer.BYTES > access.segment.byteSize()) {
                // Read spans cache block boundary, fall back to byte-by-byte
                byte b1 = readByte(pos);
                byte b2 = readByte(pos + 1);
                byte b3 = readByte(pos + 2);
                byte b4 = readByte(pos + 3);
                return (b4 << 24) | ((b3 & 0xFF) << 16) | ((b2 & 0xFF) << 8) | (b1 & 0xFF); // Little endian
            }
            return access.segment.get(LAYOUT_LE_INT, access.offsetInBlock);
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public long readLong(long pos) throws IOException {
        try {
            final BlockAccess access = getCacheBlockWithOffset(pos);

            // Check if the long spans beyond the current cache block
            if (access.offsetInBlock + Long.BYTES > access.segment.byteSize()) {
                // Read spans cache block boundary, fall back to two ints
                int lowInt = readInt(pos);      // Low 32 bits
                int highInt = readInt(pos + 4); // High 32 bits
                return ((long) highInt << 32) | ((long) lowInt & 0xFFFFFFFFL); // Little endian
            }
            return access.segment.get(LAYOUT_LE_LONG, access.offsetInBlock);
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
    public final CachedMemorySegmentIndexInput clone() {
        final CachedMemorySegmentIndexInput clone = buildSlice((String) null, 0L, this.length);
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
    public final CachedMemorySegmentIndexInput slice(String sliceDescription, long offset, long length) throws IOException {
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
    CachedMemorySegmentIndexInput buildSlice(String sliceDescription, long sliceOffset, long length) {
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

        // Clear cached block references
        cacheBlock1 = null;
        cacheBlockOffset1 = -1;
        cacheBlock2 = null;
        cacheBlockOffset2 = -1;

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
    static final class MultiSegmentImpl extends CachedMemorySegmentIndexInput {
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
        CachedMemorySegmentIndexInput buildSlice(String sliceDescription, long ofs, long length) {
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

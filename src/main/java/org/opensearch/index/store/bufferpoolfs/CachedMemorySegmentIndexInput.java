/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_MASK;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;

import java.io.EOFException;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.file.Path;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.GroupVIntUtil;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;

/**
 * A high-performance IndexInput implementation that uses memory-mapped segments with block-level caching.
 * 
 * <p>This implementation provides :
 * <ul>
 * <li>Block-aligned cached memory segments for efficient random access</li>
 * <li>Reference counting to manage memory lifecycle</li>
 * <li>Read-ahead support for sequential access patterns</li>
 * <li>Optimized bulk operations for primitive arrays</li>
 * <li>Slice support with offset management</li>
 * </ul>
 * 
 * <p>The class uses a {@link BlockSlotTinyCache} for L1 caching and falls back to
 * the main {@link BlockCache} for cache misses. Memory segments are pinned during
 * access to prevent eviction races and unpinned when no longer needed.
 * 
 * @opensearch.internal
 */
@SuppressWarnings("preview")
public class CachedMemorySegmentIndexInput extends IndexInput implements RandomAccessInput {
    static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;
    static final ValueLayout.OfShort LAYOUT_LE_SHORT = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfInt LAYOUT_LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfLong LAYOUT_LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfFloat LAYOUT_LE_FLOAT = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    final long length;

    final Path path;
    final BlockCache<RefCountedMemorySegment> blockCache;
    final ReadaheadManager readaheadManager;
    final ReadaheadContext readaheadContext;

    final long absoluteBaseOffset; // absolute position in original file where this input starts
    final boolean isSlice; // true for slices, false for main instances

    long curPosition = 0L; // absolute position within this input (0-based)
    volatile boolean isOpen = true;

    // Single block cache for current access
    private long currentBlockOffset = -1;
    private BlockCacheValue<RefCountedMemorySegment> currentBlock = null;

    // Cached offset from last getCacheBlockWithOffset call (avoid BlockAccess allocation)
    private int lastOffsetInBlock;

    private final BlockSlotTinyCache blockSlotTinyCache;

    // Safe because IndexInput instances are not thread-safe per Lucene contract -
    // each thread must use its own clone().
    private final BlockSlotTinyCache.CacheHitHolder cacheHitHolder = new BlockSlotTinyCache.CacheHitHolder();

    /**
     * Creates a new CachedMemorySegmentIndexInput instance.
     * 
     * @param resourceDescription description of the resource for debugging
     * @param path the file path being accessed
     * @param length the length of the file in bytes
     * @param blockCache the main block cache for storing memory segments
     * @param readaheadManager manager for read-ahead operations
     * @param readaheadContext context for read-ahead policy decisions
     * @param blockSlotTinyCache L1 cache for recently accessed blocks
     * @return a new CachedMemorySegmentIndexInput instance
     */
    public static CachedMemorySegmentIndexInput newInstance(
        String resourceDescription,
        Path path,
        long length,
        BlockCache<RefCountedMemorySegment> blockCache,
        ReadaheadManager readaheadManager,
        ReadaheadContext readaheadContext,
        BlockSlotTinyCache blockSlotTinyCache
    ) {
        CachedMemorySegmentIndexInput input = new CachedMemorySegmentIndexInput(
            resourceDescription,
            path,
            0,
            length,
            blockCache,
            readaheadManager,
            readaheadContext,
            false,
            blockSlotTinyCache
        );
        try {
            input.seek(0L);
        } catch (IOException ioe) {
            throw new AssertionError(ioe);
        }
        return input;
    }

    private CachedMemorySegmentIndexInput(
        String resourceDescription,
        Path path,
        long absoluteBaseOffset,
        long length,
        BlockCache<RefCountedMemorySegment> blockCache,
        ReadaheadManager readaheadManager,
        ReadaheadContext readaheadContext,
        boolean isSlice,
        BlockSlotTinyCache blockSlotTinyCache
    ) {
        super(resourceDescription);
        this.path = path;
        this.absoluteBaseOffset = absoluteBaseOffset;
        this.length = length;
        this.blockCache = blockCache;
        this.readaheadManager = readaheadManager;
        this.readaheadContext = readaheadContext;
        this.isSlice = isSlice;
        this.blockSlotTinyCache = blockSlotTinyCache;
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
    * Optimized method to get both cache block and offset in one operation.
    * Returns a pinned block that must be managed via currentBlock.
    *
    * @param pos position relative to this input
    * @return MemorySegment for the cache block (offset available in lastOffsetInBlock)
    * @throws IOException if the block cannot be acquired
    */
    private MemorySegment getCacheBlockWithOffset(long pos) throws IOException {
        final long fileOffset = absoluteBaseOffset + pos;
        final long blockOffset = fileOffset & ~CACHE_BLOCK_MASK;
        final int offsetInBlock = (int) (fileOffset - blockOffset);

        // Fast path: reuse current block if still valid.
        // this access is safe without generation check because currentBlock
        // is pinned (refCount > 1) so it cannot be returned to pool or reused
        // for different data while we hold it.
        if (blockOffset == currentBlockOffset && currentBlock != null) {
            lastOffsetInBlock = offsetInBlock;
            return currentBlock.value().segment();
        }

        cacheHitHolder.reset();

        // BlockSlotTinyCache returns already-pinned values
        final BlockCacheValue<RefCountedMemorySegment> cacheValue = blockSlotTinyCache.acquireRefCountedValue(blockOffset, cacheHitHolder);

        if (cacheValue == null) {
            throw new IOException("Failed to acquire cache value for block at offset " + blockOffset);
        }

        RefCountedMemorySegment pinnedBlock = cacheValue.value();

        // Unpin old block before swapping
        if (currentBlock != null) {
            currentBlock.unpin();
        }

        currentBlockOffset = blockOffset;
        currentBlock = cacheValue;

        // Notify readahead manager of access pattern
        if (readaheadContext != null) {
            readaheadContext.onAccess(blockOffset, cacheHitHolder.wasCacheHit());
        }

        lastOffsetInBlock = offsetInBlock;
        return pinnedBlock.segment();
    }

    /**
    * For slice IndexInputs we do NOT want to hold a long-lived pinned block across calls,
    * because slice fan-out can explode (tens of thousands) and pins add up quickly.
    *
    * Call this in a finally{} in every read*() method that calls getCacheBlockWithOffset().
    *
    * Master (isSlice == false): no-op (keeps the one-block pin across calls for speed).
    * Slice  (isSlice == true) : always unpins to prevent memory exhaustion.
    *
    * CRITICAL: With 10,000+ slices common in Lucene, even 1 leaked pin per slice =
    * 10,000 pinned blocks = memory exhaustion. We cannot rely on close() being called
    * promptly (GC finalization is unpredictable), so we MUST unpin after every operation.
    *
    * The tradeoff is increased atomic refcount churn, but correctness > performance here.
    */
    private void releasePinnedBlockIfSlice() {
        if (!isSlice)
            return;

        final BlockCacheValue<RefCountedMemorySegment> b = currentBlock;
        if (b != null) {
            currentBlock = null;
            currentBlockOffset = -1L;
            b.unpin();
        } else {
            currentBlockOffset = -1L;
        }
    }

    @Override
    public final byte readByte() throws IOException {
        // Use direct field access instead of virtual call
        final long currentPos = curPosition;
        try {
            final MemorySegment segment = getCacheBlockWithOffset(currentPos);
            final byte v = segment.get(LAYOUT_BYTE, lastOffsetInBlock);
            curPosition = currentPos + 1;
            return v;
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", currentPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        } finally {
            // slices must not retain pins across calls.
            releasePinnedBlockIfSlice();
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
                final MemorySegment seg = getCacheBlockWithOffset(currentPos);
                final int offInBlock = lastOffsetInBlock;
                final int avail = (int) (seg.byteSize() - offInBlock);

                // Fast path: full block copy
                if (offInBlock == 0 && remaining >= CACHE_BLOCK_SIZE && seg.byteSize() >= CACHE_BLOCK_SIZE) {

                    MemorySegment.copy(seg, LAYOUT_BYTE, 0L, b, bufferOffset, CACHE_BLOCK_SIZE);

                    remaining -= CACHE_BLOCK_SIZE;
                    bufferOffset += CACHE_BLOCK_SIZE;
                    currentPos += CACHE_BLOCK_SIZE;
                    continue;
                }

                // Partial block
                final int toRead = Math.min(remaining, avail);
                MemorySegment.copy(seg, LAYOUT_BYTE, offInBlock, b, bufferOffset, toRead);

                remaining -= toRead;
                bufferOffset += toRead;
                currentPos += toRead;
            }

            curPosition = startPos + len;

        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", startPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        } finally {
            // Unpin once after entire operation completes (not per loop iteration)
            releasePinnedBlockIfSlice();
        }
    }

    @Override
    public void readInts(int[] dst, int offset, int length) throws IOException {
        if (length == 0)
            return;

        final long startPos = getFilePointer();
        final long totalBytes = Integer.BYTES * (long) length;

        try {
            final MemorySegment segment;
            final int offsetInBlock;

            try {
                segment = getCacheBlockWithOffset(startPos);
                offsetInBlock = lastOffsetInBlock;

                // Check if entire read fits in current cache block
                if (offsetInBlock + totalBytes <= segment.byteSize()) {
                    // Fast path: entire read fits in one cache block
                    MemorySegment.copy(segment, LAYOUT_LE_INT, offsetInBlock, dst, offset, length);
                    curPosition += totalBytes;
                } else {
                    // Slow path: spans cache blocks, fall back to super implementation
                    super.readInts(dst, offset, length);
                }
            } finally {
                releasePinnedBlockIfSlice();
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
            final MemorySegment segment;
            final int offsetInBlock;

            try {
                segment = getCacheBlockWithOffset(startPos);
                offsetInBlock = lastOffsetInBlock;

                // Check if entire read fits in current cache block
                if (offsetInBlock + totalBytes <= segment.byteSize()) {
                    // Fast path: entire read fits in one cache block
                    MemorySegment.copy(segment, LAYOUT_LE_LONG, offsetInBlock, dst, offset, length);
                    curPosition += totalBytes;
                } else {
                    // Slow path: spans cache blocks, fall back to super implementation
                    super.readLongs(dst, offset, length);
                }
            } finally {
                releasePinnedBlockIfSlice();
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
            final MemorySegment segment;
            final int offsetInBlock;

            try {
                segment = getCacheBlockWithOffset(startPos);
                offsetInBlock = lastOffsetInBlock;

                // Check if entire read fits in current cache block
                if (offsetInBlock + totalBytes <= segment.byteSize()) {
                    // Fast path: entire read fits in one cache block
                    MemorySegment.copy(segment, LAYOUT_LE_FLOAT, offsetInBlock, dst, offset, length);
                    curPosition += totalBytes;
                } else {
                    // Slow path: spans cache blocks, fall back to super implementation
                    super.readFloats(dst, offset, length);
                }
            } finally {
                releasePinnedBlockIfSlice();
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
            final MemorySegment segment;
            final int offsetInBlock;

            try {
                segment = getCacheBlockWithOffset(currentPos);
                offsetInBlock = lastOffsetInBlock;

                // Check if the short spans beyond the current cache block
                if (offsetInBlock + Short.BYTES > segment.byteSize()) {
                    // Read spans cache block boundary, fall back to super implementation
                    return super.readShort();
                }

                final short v = segment.get(LAYOUT_LE_SHORT, offsetInBlock);
                curPosition += Short.BYTES;
                return v;
            } finally {
                releasePinnedBlockIfSlice();
            }
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", currentPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final int readInt() throws IOException {
        final long currentPos = curPosition;
        try {
            final MemorySegment segment;
            final int offsetInBlock;

            try {
                segment = getCacheBlockWithOffset(currentPos);
                offsetInBlock = lastOffsetInBlock;

                // Fast path: check if we have enough bytes in current block
                if (offsetInBlock <= segment.byteSize() - Integer.BYTES) {
                    final int v = segment.get(LAYOUT_LE_INT, offsetInBlock);
                    curPosition = currentPos + Integer.BYTES; // Direct assignment, no +=
                    return v;
                }

                // Slow path: spans cache block boundary
                return super.readInt();
            } finally {
                releasePinnedBlockIfSlice();
            }
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", currentPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final long readLong() throws IOException {
        final long currentPos = curPosition;
        try {
            final MemorySegment segment;
            final int offsetInBlock;

            try {
                segment = getCacheBlockWithOffset(currentPos);
                offsetInBlock = lastOffsetInBlock;

                if (offsetInBlock <= segment.byteSize() - Long.BYTES) {
                    final long v = segment.get(LAYOUT_LE_LONG, offsetInBlock);
                    curPosition = currentPos + Long.BYTES;
                    return v;
                }

                // Slow path: spans cache block boundary
                return super.readLong();
            } finally {
                releasePinnedBlockIfSlice();
            }
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", currentPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public void readGroupVInt(int[] dst, int offset) throws IOException {
        try {
            final MemorySegment segment;
            final int offsetInBlock;

            try {
                segment = getCacheBlockWithOffset(curPosition);
                offsetInBlock = lastOffsetInBlock;

                final int len = GroupVIntUtil
                    .readGroupVInt(
                        this,
                        segment.byteSize() - offsetInBlock,
                        p -> segment.get(LAYOUT_LE_INT, p),
                        offsetInBlock,
                        dst,
                        offset
                    );
                curPosition += len;
            } finally {
                releasePinnedBlockIfSlice();
            }
        } catch (IllegalStateException | NullPointerException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final int readVInt() throws IOException {
        // this can make JVM less confused (see LUCENE-10366)
        return super.readVInt();
    }

    @Override
    public final long readVLong() throws IOException {
        // this can make JVM less confused (see LUCENE-10366)
        return super.readVLong();
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
     * 
     * @return the absolute byte offset in the original file
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
            final MemorySegment segment;
            try {
                segment = getCacheBlockWithOffset(pos);
                return segment.get(LAYOUT_BYTE, lastOffsetInBlock);
            } finally {
                releasePinnedBlockIfSlice();
            }
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public short readShort(long pos) throws IOException {
        try {
            final MemorySegment segment;
            final int offsetInBlock;

            try {
                segment = getCacheBlockWithOffset(pos);
                offsetInBlock = lastOffsetInBlock;

                // Check if the short spans beyond the current cache block
                if (offsetInBlock + Short.BYTES > segment.byteSize()) {
                    // Read spans cache block boundary, delegate to sequential readShort()
                    long savedPos = getFilePointer();
                    try {
                        seek(pos);
                        return readShort();
                    } finally {
                        seek(savedPos);
                    }
                }
                return segment.get(LAYOUT_LE_SHORT, offsetInBlock);
            } finally {
                releasePinnedBlockIfSlice();
            }
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public int readInt(long pos) throws IOException {
        try {
            final MemorySegment segment;
            final int offsetInBlock;

            try {
                segment = getCacheBlockWithOffset(pos);
                offsetInBlock = lastOffsetInBlock;

                // Check if the int spans beyond the current cache block
                if (offsetInBlock + Integer.BYTES > segment.byteSize()) {
                    // Read spans cache block boundary, delegate to sequential readInt()
                    long savedPos = getFilePointer();
                    try {
                        seek(pos);
                        return readInt();
                    } finally {
                        seek(savedPos);
                    }
                }
                return segment.get(LAYOUT_LE_INT, offsetInBlock);
            } finally {
                releasePinnedBlockIfSlice();
            }
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public long readLong(long pos) throws IOException {
        try {
            final MemorySegment segment;
            final int offsetInBlock;

            try {
                segment = getCacheBlockWithOffset(pos);
                offsetInBlock = lastOffsetInBlock;

                // Check if the long spans beyond the current cache block
                if (offsetInBlock + Long.BYTES > segment.byteSize()) {
                    // Read spans cache block boundary, delegate to sequential readLong()
                    long savedPos = getFilePointer();
                    try {
                        seek(pos);
                        return readLong();
                    } finally {
                        seek(savedPos);
                    }
                }
                return segment.get(LAYOUT_LE_LONG, offsetInBlock);
            } finally {
                releasePinnedBlockIfSlice();
            }
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

    /** Builds the actual sliced IndexInput. * */
    CachedMemorySegmentIndexInput buildSlice(String sliceDescription, long sliceOffset, long length) {
        ensureOpen();
        // Calculate absolute base offset for the slice
        final long sliceAbsoluteBaseOffset = this.absoluteBaseOffset + sliceOffset;
        final String newResourceDescription = getFullSliceDescription(sliceDescription);

        CachedMemorySegmentIndexInput slice = new CachedMemorySegmentIndexInput(
            newResourceDescription,
            path,
            sliceAbsoluteBaseOffset,
            length,
            blockCache,
            readaheadManager,
            readaheadContext,
            true,
            blockSlotTinyCache
        );

        try {
            slice.seek(0L);
        } catch (IOException ioe) {
            throw new AssertionError(ioe);
        }

        return slice;
    }

    @Override
    @SuppressWarnings("ConvertToTryWithResources")
    public final void close() throws IOException {
        if (!isOpen) {
            return;
        }

        // Mark as closed to ensure all future accesses throw AlreadyClosedException
        isOpen = false;

        // Both master and slices must unpin their current block
        if (currentBlock != null) {
            currentBlock.unpin();
            currentBlock = null;
        }

        if (!isSlice) {
            // Master instance cleanup
            assert !isSlice : "Master instance should not be marked as slice";

            if (blockSlotTinyCache != null) {
                blockSlotTinyCache.clear();
            }

            readaheadManager.close();
        } else {
            // Slice instance cleanup
            assert isSlice : "Slice instance should be marked as slice";
            // Slices share cache and readahead manager, so don't close them
        }
    }
}

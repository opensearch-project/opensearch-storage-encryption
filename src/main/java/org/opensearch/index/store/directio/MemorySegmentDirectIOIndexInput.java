/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.opensearch.index.store.directio.DirectIOReader.getDirectOpenOption;
import static org.opensearch.index.store.directio.DirectIoConfigs.DIRECT_IO_ALIGNMENT;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;

@SuppressWarnings("preview")
public final class MemorySegmentDirectIOIndexInput extends IndexInput {
    private static final Logger LOGGER = LogManager.getLogger(MemorySegmentDirectIOIndexInput.class);

    static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;
    static final ValueLayout.OfShort LAYOUT_LE_SHORT = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfInt LAYOUT_LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfLong LAYOUT_LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfFloat LAYOUT_LE_FLOAT = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    private final Arena arena;
    private final Path path;
    private final long length;
    private final byte[] key;
    private final byte[] iv;
    private final long offset; // for slices
    private final boolean isClosable; // clones and slices are not closable
    private final FileChannel channel;

    private long filePointer = 0;
    private MemorySegment buffer = null;
    private long bufferStart = -1;

    private final int bufferSize;
    private boolean isOpen;

    public MemorySegmentDirectIOIndexInput(
        String resourceDescription,
        Path path,
        Arena arena,
        byte[] key,
        byte[] iv,
        int bufferSize,
        long size
    )
        throws IOException {
        super(resourceDescription);
        this.path = Objects.requireNonNull(path);
        this.channel = FileChannel.open(path, StandardOpenOption.READ, getDirectOpenOption());
        this.arena = arena;
        this.key = key;
        this.iv = iv;
        this.bufferSize = bufferSize;
        this.length = size;
        this.offset = 0;
        this.isClosable = true;
        this.isOpen = true;

    }

    // for clone/slice
    private MemorySegmentDirectIOIndexInput(String resourceDescription, MemorySegmentDirectIOIndexInput other, long offset, long length) {
        super(resourceDescription);
        Objects.checkFromIndexSize(offset, length, other.length);
        this.channel = other.channel;
        this.path = other.path;
        this.arena = other.arena;  // Share arena but can't close it
        this.key = other.key;
        this.iv = other.iv;
        this.bufferSize = other.bufferSize;
        this.length = length;
        this.offset = other.offset + offset;
        this.isClosable = false;
        this.filePointer = this.offset;
        this.isOpen = true;

    }

    private static ByteBuffer allocateBuffer(int bufferSize, int blockSize) {
        return ByteBuffer.allocateDirect(bufferSize + blockSize - 1).alignedSlice(blockSize).order(LITTLE_ENDIAN);
    }

    private void ensureOpen() {
        if (!isOpen || !arena.scope().isAlive()) {
            throw new AlreadyClosedException("IndexInput is closed: " + this);
        }
    }

    private void refill() throws IOException {
        long absolutePos = filePointer;
        long remainingInFile = (offset + length) - absolutePos;
        long bytesToRead = Math.min(bufferSize, remainingInFile);

        if (absolutePos > offset + length || bytesToRead <= 0) {
            throw new EOFException("read past EOF: " + this);
        }

        try {
            buffer = directIOReadAligned(channel, absolutePos, bytesToRead, arena);

            if (key != null && iv != null) {
                DirectIOReader.decryptSegment(arena, buffer, absolutePos, key, iv);
            }

            bufferStart = absolutePos;

        } catch (Throwable t) {
            throw new IOException("Failed to refill at absolutePos=" + absolutePos + ": " + this, t);
        }
    }

    public static MemorySegment directIOReadAligned(FileChannel channel, long offset, long length, Arena arena) throws IOException {
        int alignment = DIRECT_IO_ALIGNMENT;
        long alignedOffset = offset - (offset % alignment);
        long offsetDelta = offset - alignedOffset;
        long alignedLength = ((offsetDelta + length + alignment - 1) / alignment) * alignment;

        ByteBuffer alignedBuffer = allocateBuffer((int) alignedLength, alignment);

        // Read aligned data from file
        int bytesRead = channel.read(alignedBuffer, alignedOffset);

        if (bytesRead < offsetDelta + length) {
            throw new IOException("O_DIRECT read incomplete (read=" + bytesRead + ", expected=" + (offsetDelta + length) + ")");
        }

        // Create MemorySegment in arena with just the requested data
        MemorySegment result = arena.allocate(length);
        MemorySegment.copy(MemorySegment.ofBuffer(alignedBuffer), offsetDelta, result, 0, length);

        return result;
    }

    @Override
    public void close() throws IOException {
        if (isOpen && isClosable) {
            // the master IndexInput has an Arena and is able
            // to release all resources (unmap segments) - a
            // side effect is that other threads still using clones
            // will throw IllegalStateException
            if (arena != null) {
                while (arena.scope().isAlive()) {
                    try {
                        arena.close();
                        break;
                    } catch (@SuppressWarnings("unused") IllegalStateException e) {
                        Thread.onSpinWait();
                    }
                }
            }

            channel.close();
            isOpen = false;
        }
    }

    @Override
    public long getFilePointer() {
        ensureOpen();

        // Convert absolute position to relative position for slice
        // filePointer is absolute, offset is the slice start, so subtract to get relative
        long relativePointer = filePointer - offset;

        // Handle initial state - ensure we never return negative
        // (similar to Lucene's handling of -bufferSize case)
        return Math.max(relativePointer, 0);
    }

    @Override
    public void seek(long pos) throws IOException {
        ensureOpen();

        if (pos != getFilePointer()) {
            // Validate bounds (pos is relative to slice)
            if (pos < 0 || pos > length) {
                throw new EOFException("seek(" + pos + ") is out of bounds (length=" + length + ")");
            }

            final long absolutePos = pos + offset;

            // Check if the new position is within the existing buffer
            if (buffer != null && absolutePos >= bufferStart && absolutePos < bufferStart + buffer.byteSize()) {
                // Position is within current buffer - just update filePointer
                this.filePointer = absolutePos;
            } else {
                // Position is outside buffer - do an actual seek/read
                seekInternal(pos);
            }
        }

        assert pos == getFilePointer() : "seek failed: expected=" + pos + ", actual=" + getFilePointer();
    }

    private void seekInternal(long pos) throws IOException {
        // Convert relative position to absolute
        final long absolutePos = pos + offset;

        // Set filePointer to target position
        this.filePointer = absolutePos;

        // Only load buffer if not at EOF
        if (pos < length) {
            refill();  // Normal case: load buffer
        }
        // At EOF: just set position, no buffer needed
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public byte readByte() throws IOException {
        ensureOpen();

        // EOF check: ensure we can read 1 byte
        if (filePointer >= offset + length) {
            throw new EOFException("read past EOF: " + this);
        }

        if (buffer == null || filePointer < bufferStart || filePointer >= bufferStart + buffer.byteSize()) {
            refill();
        }

        byte value = buffer.get(LAYOUT_BYTE, filePointer - bufferStart);
        filePointer += 1;
        return value;
    }

    @Override
    public short readShort() throws IOException {
        ensureOpen();

        if (buffer != null && filePointer >= bufferStart && filePointer + Short.BYTES <= bufferStart + buffer.byteSize()) {
            short value = buffer.get(LAYOUT_LE_SHORT, filePointer - bufferStart);
            filePointer += Short.BYTES;
            return value;
        } else {
            return readShortFallback();
        }
    }

    @Override
    public int readInt() throws IOException {
        ensureOpen();

        if (buffer != null && filePointer >= bufferStart && filePointer + Integer.BYTES <= bufferStart + buffer.byteSize()) {
            int value = buffer.get(LAYOUT_LE_INT, filePointer - bufferStart);
            filePointer += Integer.BYTES;
            return value;
        } else {
            return readIntFallback();
        }
    }

    @Override
    public long readLong() throws IOException {
        ensureOpen();

        if (buffer != null && filePointer >= bufferStart && filePointer + Long.BYTES <= bufferStart + buffer.byteSize()) {
            long value = buffer.get(LAYOUT_LE_LONG, filePointer - bufferStart);
            filePointer += Long.BYTES;
            return value;
        } else {
            return readLongFallback();
        }
    }

    @Override
    public void readBytes(byte[] dst, int offset, int len) throws IOException {
        ensureOpen();

        if (filePointer + len > this.offset + length) {
            throw new EOFException("read past EOF: " + this);
        }

        int toRead = len;
        int dstOffset = offset;
        while (toRead > 0) {
            // Ensure we have a buffer and current position is within it
            if (buffer == null || filePointer < bufferStart || filePointer >= bufferStart + buffer.byteSize()) {
                refill();
            }

            // Calculate how much we can read from current buffer
            long bufferPos = filePointer - bufferStart;
            long available = buffer.byteSize() - bufferPos;
            int bytesToCopy = (int) Math.min(toRead, available);

            // Copy from buffer to destination array
            MemorySegment.copy(buffer, LAYOUT_BYTE, bufferPos, dst, dstOffset, bytesToCopy);

            // Update counters
            filePointer += bytesToCopy;
            dstOffset += bytesToCopy;
            toRead -= bytesToCopy;
        }
    }

    @Override
    public void readInts(int[] dst, int offset, int len) throws IOException {
        ensureOpen();

        if (filePointer + (long) len * Integer.BYTES > this.offset + length) {
            throw new EOFException("read past EOF: " + this);
        }

        int remainingDst = len;
        while (remainingDst > 0) {
            if (buffer == null || filePointer < bufferStart || filePointer >= bufferStart + buffer.byteSize()) {
                refill();
            }

            // Calculate how many complete ints we can read from current buffer
            long bufferPos = filePointer - bufferStart;
            long remainingInBuffer = buffer.byteSize() - bufferPos;
            int cnt = Math.min((int) (remainingInBuffer / Integer.BYTES), remainingDst);

            // Bulk copy ints using MemorySegment
            MemorySegment.copy(buffer, LAYOUT_LE_INT, bufferPos, dst, offset + len - remainingDst, cnt);
            filePointer += cnt * Integer.BYTES;
            remainingDst -= cnt;

            if (remainingDst > 0) {
                // Check if buffer has remaining bytes
                bufferPos = filePointer - bufferStart;
                remainingInBuffer = buffer.byteSize() - bufferPos;

                if (remainingInBuffer > 0) {
                    // Buffer has some bytes but not enough for complete int - use readInt() fallback
                    dst[offset + len - remainingDst] = readInt();
                    --remainingDst;
                } else {
                    refill();
                }
            }
        }
    }

    @Override
    public void readFloats(float[] dst, int offset, int len) throws IOException {
        ensureOpen();

        // EOF check upfront
        if (filePointer + (long) len * Float.BYTES > this.offset + length) {
            throw new EOFException("read past EOF: " + this);
        }

        int remainingDst = len;
        while (remainingDst > 0) {
            if (buffer == null || filePointer < bufferStart || filePointer >= bufferStart + buffer.byteSize()) {
                refill();
            }

            long bufferPos = filePointer - bufferStart;
            long remainingInBuffer = buffer.byteSize() - bufferPos;
            int cnt = Math.min((int) (remainingInBuffer / Float.BYTES), remainingDst);

            // Bulk copy floats using MemorySegment
            MemorySegment.copy(buffer, LAYOUT_LE_FLOAT, bufferPos, dst, offset + len - remainingDst, cnt);
            filePointer += cnt * Float.BYTES;
            remainingDst -= cnt;

            if (remainingDst > 0) {
                bufferPos = filePointer - bufferStart;
                remainingInBuffer = buffer.byteSize() - bufferPos;

                if (remainingInBuffer > 0) {
                    // Buffer has some bytes but not enough for complete float - use readInt() + convert
                    dst[offset + len - remainingDst] = Float.intBitsToFloat(readInt());
                    --remainingDst;
                } else {
                    // Buffer exhausted - refill
                    refill();
                }
            }
        }
    }

    @Override
    public void readLongs(long[] dst, int offset, int len) throws IOException {
        ensureOpen();

        if (filePointer + (long) len * Long.BYTES > this.offset + length) {
            throw new EOFException("read past EOF: " + this);
        }

        int remainingDst = len;
        while (remainingDst > 0) {
            if (buffer == null || filePointer < bufferStart || filePointer >= bufferStart + buffer.byteSize()) {
                refill();
            }

            long bufferPos = filePointer - bufferStart;
            long remainingInBuffer = buffer.byteSize() - bufferPos;
            int cnt = Math.min((int) (remainingInBuffer / Long.BYTES), remainingDst);

            MemorySegment.copy(buffer, LAYOUT_LE_LONG, bufferPos, dst, offset + len - remainingDst, cnt);
            filePointer += cnt * Long.BYTES;
            remainingDst -= cnt;

            if (remainingDst > 0) {
                bufferPos = filePointer - bufferStart;
                remainingInBuffer = buffer.byteSize() - bufferPos;

                if (remainingInBuffer > 0) {
                    // Buffer has some bytes but not enough for complete long - use readLong() fallback
                    dst[offset + len - remainingDst] = readLong();
                    --remainingDst;
                } else {
                    refill();
                }
            }
        }
    }

    @Override
    public MemorySegmentDirectIOIndexInput clone() {
        try {
            var clone = new MemorySegmentDirectIOIndexInput("clone:" + this, this, 0L, length);
            long pos = getFilePointer();

            if (pos < length) {
                clone.seekInternal(pos);
            } else {
                // At EOF: just set position without loading buffer
                clone.filePointer = pos + clone.offset;  // Set absolute position
            }

            return clone;
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if ((length | offset) < 0 || length > this.length - offset) {
            throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: " + this);
        }

        var slice = new MemorySegmentDirectIOIndexInput(sliceDescription, this, offset, length);

        if (length > 0) {
            slice.seekInternal(0L);  // Only seek if slice has content
        }
        // If length == 0, slice stays at position 0 without loading buffer

        return slice;
    }

    private short readShortFallback() throws IOException {
        byte b1 = readByte();  // LSB
        byte b2 = readByte();  // MSB
        return (short) ((b1 & 0xFF) | (b2 & 0xFF) << 8);  // : LSB | (MSB << 8)
    }

    private int readIntFallback() throws IOException {
        byte b1 = readByte();  // LSB
        byte b2 = readByte();
        byte b3 = readByte();
        byte b4 = readByte();  // MSB
        return (b1 & 0xFF) | ((b2 & 0xFF) << 8) | ((b3 & 0xFF) << 16) | ((b4 & 0xFF) << 24);
    }

    private long readLongFallback() throws IOException {
        // Read as two ints and combine (little-endian)
        int low = readInt();   // Lower 32 bits
        int high = readInt();  // Upper 32 bits
        return ((long) high << 32) | (low & 0xFFFFFFFFL);
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_loader;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.getDirectIOAlignment;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Arrays;

import org.opensearch.common.SuppressForbidden;

/**
 * Utility class for Direct I/O operations with proper alignment handling.
 * 
 * <p>This class provides methods for reading data using Direct I/O, which bypasses
 * the operating system's buffer cache for improved performance in certain scenarios.
 * Direct I/O requires proper alignment to storage device sector boundaries.
 *
 * @opensearch.internal
 */
@SuppressWarnings("preview")
@SuppressForbidden(reason = "uses custom DirectIO")
public class DirectIOReaderUtil {
    private static final OpenOption ExtendedOpenOption_DIRECT; // visible for test

    private DirectIOReaderUtil() {}

    static {
        OpenOption option;
        try {
            final Class<? extends OpenOption> clazz = Class.forName("com.sun.nio.file.ExtendedOpenOption").asSubclass(OpenOption.class);
            option = Arrays.stream(clazz.getEnumConstants()).filter(e -> e.toString().equalsIgnoreCase("DIRECT")).findFirst().orElse(null);
        } catch (@SuppressWarnings("unused") ClassNotFoundException e) {
            option = null;
        }
        ExtendedOpenOption_DIRECT = option;
    }

    /**
     * Gets the Direct I/O open option for bypassing OS buffer cache.
     *
     * @return the Direct I/O open option
     * @throws UnsupportedOperationException if Direct I/O is not available in current JDK
     */
    public static OpenOption getDirectOpenOption() {
        if (ExtendedOpenOption_DIRECT == null) {
            throw new UnsupportedOperationException(
                "com.sun.nio.file.ExtendedOpenOption.DIRECT is not available in the current JDK version."
            );
        }
        return ExtendedOpenOption_DIRECT;
    }

    /**
     * Reads data using Direct I/O with proper alignment.
     * <p>
     * Direct I/O requires alignment to storage device sector boundaries.
     * </p>
     *
     * <p><b>File Layout:</b></p>
     * <pre>
     * ┌─────┬─────┬─────┬─────┬─────┬─────┐
     * │  0  │ 512 │1024 │1536 │2048 │2560 │ ← Sector boundaries
     * └─────┴─────┴─────┴─────┴─────┴─────┘
     * </pre>
     *
     * <p><b>Incorrect: Reading from offset 1000</b></p>
     * <pre>
     *                     ↓ start here
     * ┌─────┬─────┬─────┬─────┬─────┬─────┐
     * │  0  │ 512 │1024 │1536 │2048 │2560 │
     *                 ███│█████
     * </pre>
     *
     * <p><b>Correct: Reading from offset 1024</b></p>
     * <pre>
     *                      ↓ start here  
     * ┌─────┬─────┬─────┬─────┬─────┬─────┐
     * │  0  │ 512 │1024 │1536 │2048 │2560 │
     *                     │█████│█████│
     * </pre>
     *
     * @param channel the file channel to read from
     * @param filePath the path of the file being read (used to determine filesystem block size)
     * @param offset the byte offset in the file to start reading from
     * @param length the number of bytes to read
     * @param arena the memory arena for allocating the result segment
     * @return a memory segment containing the read data
     * @throws IOException if the read operation fails
     */
    public static MemorySegment directIOReadAligned(FileChannel channel, Path filePath, long offset, long length, Arena arena)
        throws IOException {
        int alignment = getDirectIOAlignment(filePath);

        // Require alignment to be a power of 2
        if ((alignment & (alignment - 1)) != 0) {
            throw new IllegalArgumentException("Alignment must be a power of 2: " + alignment);
        }

        long alignedOffset = offset & ~(alignment - 1);        // Align down
        long offsetDelta = offset - alignedOffset;
        long adjustedLength = offsetDelta + length;
        long alignedLength = (adjustedLength + alignment - 1) & ~(alignment - 1); // Align up

        if (alignedLength > Integer.MAX_VALUE) {
            throw new IOException("Aligned read size too large: " + alignedLength);
        }

        MemorySegment alignedSegment = arena.allocate(alignedLength, alignment);
        ByteBuffer directBuffer = alignedSegment.asByteBuffer();

        int bytesRead = channel.read(directBuffer, alignedOffset);
        if (bytesRead < 0) {
            // EOF, return empty segment
            return arena.allocate(0);
        }

        // Clamp to available
        int available = Math.max(0, bytesRead - (int) offsetDelta);
        int toCopy = (int) Math.min(length, available);

        return alignedSegment.asSlice(offsetDelta, toCopy);
    }

    /**
     * Reads data using standard buffered I/O (not Direct I/O).
     * 
     * <p>This method uses the standard file channel read operation which goes through
     * the operating system's buffer cache. It's used as a fallback when Direct I/O
     * is not available or appropriate.
     *
     * @param channel the file channel to read from
     * @param offset the byte offset in the file to start reading from
     * @param size the number of bytes to read
     * @param arena the memory arena for managing the result segment lifecycle
     * @return a memory segment containing the read data
     * @throws IOException if the read operation fails or doesn't read the expected amount
     */
    public static MemorySegment bufferedRead(FileChannel channel, long offset, long size, Arena arena) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate((int) size);
        int read = channel.read(buf, offset);
        if (read != size) {
            throw new IOException("Failed to fully read chunk via buffered I/O. expected=" + size + " read=" + read);
        }
        buf.flip();
        return MemorySegment.ofBuffer(buf).reinterpret(size, arena, null);
    }
}

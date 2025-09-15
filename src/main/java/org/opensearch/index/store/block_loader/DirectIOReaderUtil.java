/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_loader;

import static org.opensearch.index.store.directio.DirectIoConfigs.DIRECT_IO_ALIGNMENT;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.util.Arrays;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.PanamaNativeAccess;

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
     */
    public static MemorySegment directIOReadAligned(FileChannel channel, long offset, long length, Arena arena) throws IOException {
        int alignment = Math.max(DIRECT_IO_ALIGNMENT, PanamaNativeAccess.getPageSize());

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

        MemorySegment finalSegment = arena.allocate(toCopy);
        if (toCopy > 0) {
            MemorySegment.copy(alignedSegment, offsetDelta, finalSegment, 0, toCopy);
        }

        return finalSegment;
    }

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

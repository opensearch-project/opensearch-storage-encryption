/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIoConfigs.DIRECT_IO_ALIGNMENT;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.OpenOption;
import java.util.Arrays;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.cipher.OpenSslNativeCipher;
import org.opensearch.index.store.mmap.PanamaNativeAccess;

@SuppressWarnings("preview")
@SuppressForbidden(reason = "uses custom DirectIO")
public class DirectIOReader {
    private static final Logger LOGGER = LogManager.getLogger(DirectIOReader.class);
    private static final TieredDirectIOBufferPool BUFFER_POOL = new TieredDirectIOBufferPool();

    private static final OpenOption ExtendedOpenOption_DIRECT; // visible for test

    private DirectIOReader() {}

    static {
        OpenOption option;
        try {
            final Class<? extends OpenOption> clazz = Class.forName("com.sun.nio.file.ExtendedOpenOption").asSubclass(OpenOption.class);
            option = Arrays.stream(clazz.getEnumConstants()).filter(e -> e.toString().equalsIgnoreCase("DIRECT")).findFirst().orElse(null);
        } catch (@SuppressWarnings("unused") Exception e) {
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
     * @param fd     File descriptor (already opened with {@code O_DIRECT})
     * @param offset File offset to read from
     * @param length Number of bytes to read
     * @param arena  Memory arena for allocation
     * @return MemorySegment containing the read data
     * @throws Throwable if the I/O operation fails
     */
    public static MemorySegment directIOReadAligned(int fd, long offset, long length, Arena arena) throws Throwable {
        int alignment = DIRECT_IO_ALIGNMENT;

        // Align the offset down to the nearest alignment boundary.
        // Example: 1003 -> 512 (floor to nearest 512)
        long alignedOffset = offset - (offset % alignment);

        // Calculate how far into the aligned buffer the actual requested data starts.
        // This tells us how many bytes to skip from the beginning of the buffer to reach the real data.
        // Example: offsetDelta = 1003 - 512 = 491
        long offsetDelta = offset - alignedOffset;

        // Add offsetDelta to the original requested length.
        // This gives the total number of bytes we need to read to ensure we can safely slice out the real region.
        // Example: adjustedLength = 491 + 32768 (assume file size) = 33259
        long adjustedLength = offsetDelta + length;

        // Round up adjustedLength to the nearest alignment boundary.
        // Ensures the allocated buffer is large enough and still aligned.
        // Example: alignedLength = ceil(33259 / 512) * 512 = 33792
        long alignedLength = ((adjustedLength + alignment - 1) / alignment) * alignment;

        // Get a reusable buffer from pool
        MemorySegment readBuffer = BUFFER_POOL.acquire(alignedLength);

        try {
            // Read into the pooled buffer
            long bytesRead = (long) PanamaNativeAccess.PREAD.invoke(fd, readBuffer, alignedLength, alignedOffset);

            if (bytesRead < 0) {
                throw new IOException("O_DIRECT pread failed with result: " + bytesRead + " for alignedOffset: " + alignedOffset);
            }

            if (bytesRead < offsetDelta + length) {
                throw new IOException("O_DIRECT pread incomplete (read=" + bytesRead + ", expected=" + (offsetDelta + length) + ")");
            }

            // Allocate final segment in the target arena
            MemorySegment finalSegment = arena.allocate(length);

            // Copy from pooled buffer to final segment
            MemorySegment.copy(readBuffer, offsetDelta, finalSegment, 0, length);

            return finalSegment;

        } finally {
            // Always return the buffer to the pool
            BUFFER_POOL.release(readBuffer);
        }

    }

    public static void decryptSegment(Arena arena, MemorySegment segment, long segmentOffsetInFile, byte[] key, byte[] iv)
        throws Throwable {
        final long size = segment.byteSize();

        final int twoMB = 1 << 21; // 2 MiB
        final int fourMB = 1 << 22; // 4 MiB
        final int eightMB = 1 << 23; // 8 MiB
        final int sixteenMB = 1 << 24; // 16 MiB

        // Fast-path: no parallelism for ≤ 4 MiB
        if (size <= (4L << 20)) {
            long start = System.nanoTime();

            OpenSslNativeCipher.decryptInPlace(arena, segment.address(), size, key, iv, segmentOffsetInFile);

            long end = System.nanoTime();
            long durationMs = (end - start) / 1_000_000;
            LOGGER.debug("Eager decryption of {} MiB at offset {} took {} ms", size / 1048576.0, segmentOffsetInFile, durationMs);
            return;
        }

        // Use Openssl for large block decrytion.
        final int chunkSize;
        if (size <= (8L << 20)) {
            chunkSize = twoMB;
        } else if (size <= (32L << 20)) {
            chunkSize = fourMB;
        } else if (size <= (64L << 20)) {
            chunkSize = eightMB;
        } else {
            chunkSize = sixteenMB;
        }

        final int numChunks = (int) ((size + chunkSize - 1) / chunkSize);

        // parallel decryptions.
        IntStream.range(0, numChunks).parallel().forEach(i -> {
            long offset = (long) i * chunkSize;
            long length = Math.min(chunkSize, size - offset);
            long fileOffset = segmentOffsetInFile + offset;
            long addr = segment.address() + offset;

            try {
                OpenSslNativeCipher.decryptInPlace(addr, length, key, iv, fileOffset);
            } catch (Throwable t) {
                throw new RuntimeException("Decryption failed at offset: " + fileOffset, t);
            }
        });
    }

}

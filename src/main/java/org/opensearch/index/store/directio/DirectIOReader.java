/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIoConfigs.DIRECT_IO_ALIGNMENT;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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

        long start = System.nanoTime();
        long acquireStart = start;

        MemorySegment alignedSegment = arena.allocate(alignedLength, alignment);
        ByteBuffer directBuffer = alignedSegment.asByteBuffer();

        long acquireEnd = System.nanoTime();

        long readStart = acquireEnd;
        int bytesRead = channel.read(directBuffer, alignedOffset);
        long readEnd = System.nanoTime();

        if (bytesRead < offsetDelta + length) {
            throw new IOException(
                "Incomplete read: read="
                    + bytesRead
                    + ", expected="
                    + (offsetDelta + length)
                    + ", offset="
                    + offset
                    + ", alignedOffset="
                    + alignedOffset
            );
        }

        long allocStart = readEnd;
        MemorySegment finalSegment = arena.allocate(length);
        long allocEnd = System.nanoTime();

        long copyStart = allocEnd;
        MemorySegment.copy(alignedSegment, offsetDelta, finalSegment, 0, length);
        long copyEnd = System.nanoTime();

        LOGGER
            .debug(
                String
                    .format(
                        "DirectIORead(offset=%d, length=%d): total=%d µs | acquire=%d µs | read=%d µs | allocate=%d µs | copy=%d µs",
                        offset,
                        length,
                        (copyEnd - start) / 1_000,
                        (acquireEnd - acquireStart) / 1_000,
                        (readEnd - readStart) / 1_000,
                        (allocEnd - allocStart) / 1_000,
                        (copyEnd - copyStart) / 1_000
                    )
            );

        return finalSegment;
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

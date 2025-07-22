/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.Provider;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_cache.MemorySegmentPool;
import org.opensearch.index.store.block_cache.Pool;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;
import org.opensearch.index.store.cipher.OpenSslNativeCipher;
import static org.opensearch.index.store.directio.DirectIOReader.getDirectOpenOption;
import static org.opensearch.index.store.directio.DirectIoConfigs.CHUNK_SIZE_POWER;
import static org.opensearch.index.store.directio.DirectIoConfigs.DIRECT_IO_ALIGNMENT;
import org.opensearch.index.store.iv.KeyIvResolver;
import org.opensearch.index.store.mmap.PanamaNativeAccess;

@SuppressWarnings("preview")
@SuppressForbidden(reason = "uses custom DirectIO")
public final class CryptoDirectIODirectory extends FSDirectory {
    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectIODirectory.class);
    private final AtomicLong nextTempFileCounter = new AtomicLong();

    private final Pool<MemorySegment> memorySegmentPool;
    private final BlockCache<RefCountedMemorySegment> blockCache;
    private final KeyIvResolver keyIvResolver;

    public CryptoDirectIODirectory(
        Path path,
        LockFactory lockFactory,
        Provider provider,
        KeyIvResolver keyIvResolver,
        Pool<MemorySegment> memorySegmentPool,
        BlockCache<RefCountedMemorySegment> blockCache
    )
        throws IOException {
        super(path, lockFactory);
        this.keyIvResolver = keyIvResolver;
        this.memorySegmentPool = memorySegmentPool;
        this.blockCache = blockCache;

        startTelemetry();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        ensureCanRead(name);

        Path file = getDirectory().resolve(name);
        long size = Files.size(file);
        if (size == 0) {
            throw new IOException("Cannot open empty file with DirectIO: " + file);
        }

        boolean confined = context == IOContext.READONCE;
        Arena arena = confined ? Arena.ofConfined() : Arena.ofShared();

        int chunkSizePower = CHUNK_SIZE_POWER;
        long chunkSize = 1L << chunkSizePower;
        int numChunks = (int) ((size + chunkSize - 1) >>> chunkSizePower);

        // CHANGE: Use RefCountedMemorySegment array
        RefCountedMemorySegment[] segments = new RefCountedMemorySegment[numChunks];

        boolean success = false;
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, getDirectOpenOption())) {

            // ONLY load partial chunks (typically just the last segment)
            long offset = 0;
            for (int i = 0; i < numChunks; i++) {
                long remaining = size - offset;
                long segmentSize = Math.min(chunkSize, remaining);

                if (segmentSize < chunkSize) {
                    // Partial chunk - load eagerly (small and uncacheable)
                    MemorySegment segment = directIOReadAligned(channel, offset, segmentSize, arena);
                    decryptSegment(arena, segment, offset);

                    // CHANGE: Wrap in RefCountedMemorySegment for consistency
                    RefCountedMemorySegment refSeg = new RefCountedMemorySegment(
                        segment,
                        (int) segmentSize,
                        seg -> { /* No-op releaser since arena manages this */ }
                    );
                    segments[i] = refSeg;
                }
                // Full chunks remain null - loaded lazily via cache
                offset += segmentSize;
            }

            IndexInput in = CryptoDirectIOMemoryIndexInput
                .newInstance(
                    "CryptoMemorySegmentIndexInput(path=\"" + file + "\")",
                    file,
                    arena,
                    blockCache,
                    segments,
                    size, // ← FIX: Use size, not offset
                    chunkSizePower
                );

            success = true;
            return in;

        } catch (Throwable t) {
            LOGGER.error("DirectIO failed for file: {}", file, t);
            throw new IOException("Failed to open DirectIO file: " + file, t);
        } finally {
            if (!success) {
                arena.close();
            }
        }
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

    public void decryptSegment(Arena arena, MemorySegment segment, long segmentOffsetInFile) throws Throwable {
        final long size = segment.byteSize();

        final int twoMB = 1 << 21; // 2 MiB
        final int fourMB = 1 << 22; // 4 MiB
        final int eightMB = 1 << 23; // 8 MiB
        final int sixteenMB = 1 << 24; // 16 MiB

        final byte[] key = this.keyIvResolver.getDataKey().getEncoded();
        final byte[] iv = this.keyIvResolver.getIvBytes();

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

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {

        if (name.contains("segments_") || name.endsWith(".si")) {
            return super.createOutput(name, context);
        }

        ensureOpen();
        Path path = directory.resolve(name);

        return new CryptoDirectIOIndexOutput(path, name, this.keyIvResolver, this.memorySegmentPool, this.blockCache);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        if (prefix.contains("segments_") || prefix.endsWith(".si")) {
            return super.createTempOutput(prefix, suffix, context);
        }

        ensureOpen();
        String name = getTempFileName(prefix, suffix, nextTempFileCounter.getAndIncrement());
        Path path = directory.resolve(name);

        return new CryptoDirectIOIndexOutput(path, name, this.keyIvResolver, this.memorySegmentPool, this.blockCache);

    }

    @Override
    public synchronized void close() throws IOException {
        isOpen = false;
        deletePendingFiles();
    }


    private void logCacheAndPoolStats() {
        try {

            if (blockCache instanceof CaffeineBlockCache) {
                String cacheStats = ((CaffeineBlockCache<?>) blockCache).cacheStats();
                LOGGER.info("{} ", cacheStats);
            }

            if (memorySegmentPool instanceof MemorySegmentPool memorySegmentPool1) {
                MemorySegmentPool.PoolStats poolStats = memorySegmentPool1.getStats();
                LOGGER.info("{} \n {}", poolStats.toString());
            }

        } catch (Exception e) {
            LOGGER.warn("Failed to log cache/pool stats", e);
        }
    }

    private void startTelemetry() {
        Thread loggerThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(60_000); // 30 seconds
                   logCacheAndPoolStats();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Throwable t) {
                    LOGGER.warn("Error in buffer pool stats logger", t);
                }
            }
        });

        loggerThread.setDaemon(true);
        loggerThread.setName("DirectIOBufferPoolStatsLogger");
        loggerThread.start();
    }
}

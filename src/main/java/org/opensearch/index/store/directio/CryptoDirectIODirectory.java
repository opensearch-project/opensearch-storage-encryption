/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIOReader.getDirectOpenOption;
import static org.opensearch.index.store.directio.DirectIoConfigs.SEGMENT_SIZE_POWER;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.Provider;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockLoader;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_cache.MemorySegmentPool;
import org.opensearch.index.store.block_cache.Pool;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;
import org.opensearch.index.store.iv.KeyIvResolver;

@SuppressWarnings("preview")
@SuppressForbidden(reason = "uses custom DirectIO")
public final class CryptoDirectIODirectory extends FSDirectory {
    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectIODirectory.class);
    private final AtomicLong nextTempFileCounter = new AtomicLong();

    private final Pool<MemorySegment> memorySegmentPool;
    private final BlockCache<RefCountedMemorySegment> blockCache;
    private final BlockLoader<RefCountedMemorySegment> blockLoader;

    private final KeyIvResolver keyIvResolver;
    private final Path path;

    public CryptoDirectIODirectory(
        Path path,
        LockFactory lockFactory,
        Provider provider,
        KeyIvResolver keyIvResolver,
        Pool<MemorySegment> memorySegmentPool,
        BlockCache<RefCountedMemorySegment> blockCache,
        BlockLoader<RefCountedMemorySegment> blockLoader
    )
        throws IOException {
        super(path, lockFactory);
        this.keyIvResolver = keyIvResolver;
        this.memorySegmentPool = memorySegmentPool;
        this.blockCache = blockCache;
        this.path = path;
        this.blockLoader = blockLoader;
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

        long chunkSize = 1L << SEGMENT_SIZE_POWER;
        int numChunks = (int) ((size + chunkSize - 1) >>> SEGMENT_SIZE_POWER);

        MemorySegment[] segments = new MemorySegment[numChunks];
        RefCountedMemorySegment[] refSegments = new RefCountedMemorySegment[numChunks];

        boolean success = false;
        FileChannel fc = FileChannel.open(file, StandardOpenOption.READ, getDirectOpenOption());

        try {
            long offset = 0;

            for (int i = 0; i < numChunks; i++) {
                long remaining = size - offset;
                long segmentSize = Math.min(chunkSize, remaining);

                if (segmentSize < chunkSize || i == 0 || i == numChunks - 1) {
                    MemorySegment segment = DirectIOReader.directIOReadAligned(fc, offset, segmentSize, arena);
                    segments[i] = segment;
                }

                offset += segmentSize;
            }

            IndexInput in = CryptoDirectIOMemoryIndexInput
                .newInstance(
                    "CryptoMemorySegmentIndexInput(path=\"" + file + "\")",
                    fc,
                    file,
                    arena,
                    blockCache,
                    blockLoader,
                    segments,
                    refSegments,
                    size,
                    SEGMENT_SIZE_POWER,
                    keyIvResolver.getDataKey().getEncoded(),
                    keyIvResolver.getIvBytes()
                );

            success = true;
            return in;

        } catch (Throwable t) {
            LOGGER.error("DirectIO failed for file: {}", file, t);
            throw new IOException("Failed to open DirectIO file: " + file, t);
        } finally {
            if (!success) {
                try {
                    fc.close();
                } catch (IOException e) {
                    LOGGER.warn("Failed to close channel on error", e);
                }
                arena.close();
            }
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {

        if (name.contains("segments_") || name.endsWith(".si")) {
            return super.createOutput(name, context);
        }

        ensureOpen();
        Path path = directory.resolve(name);
        OutputStream fos = Files.newOutputStream(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);

        final boolean shouldAddToBufferPool = true;

        return new BufferIOWithCaching(
            name,
            path,
            fos,
            this.keyIvResolver.getDataKey().getEncoded(),
            keyIvResolver.getIvBytes(),
            this.memorySegmentPool,
            this.blockCache,
            shouldAddToBufferPool
        );

    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        if (prefix.contains("segments_") || prefix.endsWith(".si")) {
            return super.createTempOutput(prefix, suffix, context);
        }

        ensureOpen();
        String name = getTempFileName(prefix, suffix, nextTempFileCounter.getAndIncrement());
        Path path = directory.resolve(name);
        OutputStream fos = Files.newOutputStream(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        final boolean shouldAddToBufferPool = false;

        return new BufferIOWithCaching(
            name,
            path,
            fos,
            this.keyIvResolver.getDataKey().getEncoded(),
            keyIvResolver.getIvBytes(),
            this.memorySegmentPool,
            this.blockCache,
            shouldAddToBufferPool
        );
    }

    @Override
    public synchronized void close() throws IOException {
        isOpen = false;
        deletePendingFiles();
    }

    private void logCacheAndPoolStats() {
        try {

            if (blockCache instanceof CaffeineBlockCache && memorySegmentPool instanceof MemorySegmentPool memorySegmentPool1) {
                String cacheStats = ((CaffeineBlockCache<?>) blockCache).cacheStats();

                MemorySegmentPool.PoolStats poolStats = memorySegmentPool1.getStats();

                if (poolStats.pressureRatio * 100 > 60) {
                    LOGGER.info("{} {} \n {}", poolStats, cacheStats, path);
                }
            }

        } catch (Exception e) {
            LOGGER.warn("Failed to log cache/pool stats", e);
        }
    }

    private void startTelemetry() {
        Thread loggerThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(60_000); // 60 seconds
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

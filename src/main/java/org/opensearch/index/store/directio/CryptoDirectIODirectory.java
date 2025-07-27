/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIOReader.directIOReadAligned;
import static org.opensearch.index.store.directio.DirectIOReader.getDirectOpenOption;
import static org.opensearch.index.store.directio.DirectIoConfigs.CHUNK_SIZE_POWER;

import java.io.IOException;
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

    public static final MemorySegment UNMAPPED_SEGMENT = MemorySegment.ofArray(new byte[0]);

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

        int chunkSizePower = CHUNK_SIZE_POWER;
        long chunkSize = 1L << chunkSizePower;
        int numChunks = (int) ((size + chunkSize - 1) >>> chunkSizePower);

        MemorySegment[] segments = new MemorySegment[numChunks];
        RefCountedMemorySegment[] inAccessMemorySegments = new RefCountedMemorySegment[numChunks];

        boolean success = false;
        FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, getDirectOpenOption());
        try {
            // ONLY load partial chunks (typically just the last segment)
            long offset = 0;

            for (int i = 0; i < numChunks; i++) {
                long remaining = size - offset;
                long segmentSize = Math.min(chunkSize, remaining);

                if (segmentSize < chunkSize || i == 0) {
                    MemorySegment segment = directIOReadAligned(channel, offset, segmentSize, arena);
                    DirectIOReader
                        .decryptSegment(arena, segment, offset, keyIvResolver.getDataKey().getEncoded(), keyIvResolver.getIvBytes());
                    segments[i] = segment;
                } else {
                    segments[i] = UNMAPPED_SEGMENT;  // Lazy-loaded segment marker
                }

                offset += segmentSize;
            }

            IndexInput in = CryptoDirectIOMemoryIndexInput
                .newInstance(
                    "CryptoMemorySegmentIndexInput(path=\"" + file + "\")",
                    channel,
                    file,
                    arena,
                    blockCache,
                    blockLoader,
                    segments,
                    inAccessMemorySegments,
                    size,
                    chunkSizePower,
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
                    channel.close();
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

        final boolean shouldAddToBufferPool = true;
        return new CryptoDirectIOIndexOutput(
            path,
            name,
            this.keyIvResolver,
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

        final boolean shouldAddToBufferPool = false;

        return new CryptoDirectIOIndexOutput(
            path,
            name,
            this.keyIvResolver,
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

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.CryptoDirectIOIndexInputHelper.decryptSegment;
import static org.opensearch.index.store.directio.CryptoDirectIOIndexInputHelper.directIOReadAligned;
import static org.opensearch.index.store.directio.DirectIoUtils.CHUNK_SIZE_POWER;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.util.Optional;
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
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_cache.MemorySegmentPool;
import org.opensearch.index.store.block_cache.Pool;
import org.opensearch.index.store.iv.KeyIvResolver;
import org.opensearch.index.store.mmap.MemorySegmentIndexInput;
import org.opensearch.index.store.mmap.PanamaNativeAccess;

@SuppressWarnings("preview")
@SuppressForbidden(reason = "uses custom DirectIO")
public final class CryptoDirectIODirectory extends FSDirectory {
    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectIODirectory.class);
    private final AtomicLong nextTempFileCounter = new AtomicLong();

    private final Pool<MemorySegment> memorySegmentPool;
    private final BlockCache<MemorySegment> blockCache;
    private final KeyIvResolver keyIvResolver;

    public CryptoDirectIODirectory(
        Path path,
        LockFactory lockFactory,
        Provider provider,
        KeyIvResolver keyIvResolver,
        Pool<MemorySegment> memorySegmentPool,
        BlockCache<MemorySegment> blockCache
    )
        throws IOException {
        super(path, lockFactory);
        this.keyIvResolver = keyIvResolver;
        this.memorySegmentPool = memorySegmentPool;
        this.blockCache = blockCache;
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
        boolean success = false;

        int fd = -1;
        try {
            MemorySegment[] segments = new MemorySegment[numChunks];

            fd = PanamaNativeAccess.openFileWithODirect(file.toAbsolutePath().toString(), true, arena);

            long offset = 0;
            for (int i = 0; i < numChunks; i++) {
                long remaining = size - offset;
                long segmentSize = Math.min(chunkSize, remaining);

                BlockCacheKey cacheKey = new DirectIOBlockCacheKey(file, offset);
                Optional<BlockCacheValue<MemorySegment>> valueOpt = blockCache.getOrLoad(cacheKey, (int) segmentSize);

                if (valueOpt.isPresent() && valueOpt.get().block() != null) {
                    segments[i] = valueOpt.get().block();
                }
                // This will happen if no segment pool is full.
                else {
                    logCacheAndPoolStats(file);
                    MemorySegment segment = directIOReadAligned(fd, offset, segmentSize, arena);
                    decryptSegment(arena, segment, offset, this.keyIvResolver.getDataKey().getEncoded(), this.keyIvResolver.getIvBytes());
                    segments[i] = segment;
                }

                offset += segmentSize;
            }

            IndexInput in = MemorySegmentIndexInput
                .newInstance("CryptoDirectIOIndexInput(path=\"" + file + "\")", arena, segments, size, chunkSizePower);

            success = true;
            return in;

        } catch (Throwable t) {
            LOGGER.error("DirectIO decryption failed for file: {}", file, t);
            throw new IOException("Failed to direct-io/decrypt: " + file, t);
        } finally {
            // Close file descriptor after all chunks are processed
            if (fd >= 0) {
                try {
                    PanamaNativeAccess.closeFile(fd);
                } catch (Throwable closeEx) {
                    LOGGER.warn("Failed to close file descriptor for: {}", file, closeEx);
                }
            }

            if (success == false) {
                arena.close();
            }
        }
    }

    private void logCacheAndPoolStats(Path file) {
        try {

            if (blockCache instanceof CaffeineBlockCache) {
                String cacheStats = ((CaffeineBlockCache<?>) blockCache).cacheStats();
                LOGGER.info("{} ", cacheStats);
            }

            if (memorySegmentPool instanceof MemorySegmentPool memorySegmentPool1) {
                MemorySegmentPool.PoolStats poolStats = memorySegmentPool1.getStats();
                LOGGER.info("{} \n {}", poolStats.toString(), file);

            }

        } catch (Exception e) {
            LOGGER.warn("Failed to log cache/pool stats", e);
        }
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
}

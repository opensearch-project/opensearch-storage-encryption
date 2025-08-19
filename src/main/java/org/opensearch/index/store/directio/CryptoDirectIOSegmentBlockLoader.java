/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIOReader.directIOReadAligned;
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_MASK;
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE;
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE_POWER;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.block_cache.BlockLoader;
import org.opensearch.index.store.block_cache.Pool;
import org.opensearch.index.store.iv.KeyIvResolver;

@SuppressWarnings("preview")
public class CryptoDirectIOSegmentBlockLoader implements BlockLoader<MemorySegment> {
    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectIOSegmentBlockLoader.class);

    private final Pool<MemorySegment> segmentPool;
    private final KeyIvResolver keyIvResolver;

    public CryptoDirectIOSegmentBlockLoader(Pool<MemorySegment> segmentPool, KeyIvResolver keyIvResolver) {
        this.segmentPool = segmentPool;
        this.keyIvResolver = keyIvResolver;
    }

    @Override
    public MemorySegment[] load(Path filePath, long startOffset, long blockCount) throws Exception {
        if (!Files.exists(filePath)) {
            throw new NoSuchFileException(filePath.toString());
        }

        if ((startOffset & CACHE_BLOCK_MASK) != 0) {
            throw new IllegalArgumentException("startOffset must be block-aligned: " + startOffset);
        }

        if (blockCount <= 0) {
            throw new IllegalArgumentException("blockCount must be positive: " + blockCount);
        }

        if (segmentPool.isUnderPressure()) {
            throw new PoolPressureException("Memory segment pool is under pressure");
        }

        MemorySegment[] result = new MemorySegment[(int) blockCount];
        long readLength = blockCount << CACHE_BLOCK_SIZE_POWER;

        try (
            Arena arena = Arena.ofConfined();
            FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ, DirectIOReader.getDirectOpenOption())
        ) {
            MemorySegment bulkEncrypted = directIOReadAligned(channel, startOffset, readLength, arena);
            long bytesRead = bulkEncrypted.byteSize();

            // Fast-path: blockCount == 1 and partial read is acceptable (e.g., tail block)
            if (blockCount == 1 && bytesRead <= CACHE_BLOCK_SIZE) {
                MemorySegment pooled = null;
                try {
                    pooled = segmentPool.tryAcquire(10, TimeUnit.MILLISECONDS);
                    if (pooled == null) {
                        throw new PoolAcquireFailedException("Failed to acquire memory segment from pool within timeout");
                    }

                    int toCopy = (int) bytesRead;
                    if (toCopy > 0) {
                        MemorySegment.copy(bulkEncrypted, 0, pooled, 0, toCopy);
                    }

                    result[0] = pooled;

                    LOGGER.debug("Short block read (EOF safe): path={} offset={} bytesRead={}", filePath, startOffset, bytesRead);
                    return result;

                } catch (Exception e) {
                    if (pooled != null) {
                        segmentPool.release(pooled);
                    }
                    throw e;
                }
            }

            // For multi-block reads, short read is unexpected — likely corruption
            if (bytesRead < readLength) {
                throw new BlockLoadFailedException(
                    "Short read: expected " + readLength + " bytes, got " + bytesRead + " for blockCount=" + blockCount
                );
            }

            int blockIndex = 0;
            try {
                for (; blockIndex < blockCount; blockIndex++) {
                    MemorySegment pooled = segmentPool.tryAcquire(10, TimeUnit.MILLISECONDS);
                    if (pooled == null) {
                        throw new PoolAcquireFailedException("Failed to acquire memory segment from pool within timeout");
                    }

                    result[blockIndex] = pooled;
                    long offsetInBulk = (long) blockIndex << CACHE_BLOCK_SIZE_POWER;
                    MemorySegment.copy(bulkEncrypted, offsetInBulk, pooled, 0, CACHE_BLOCK_SIZE);
                }
            } catch (InterruptedException | PoolAcquireFailedException e) {
                releaseSegments(result, blockIndex);
                throw new BlockLoadFailedException("Failed to load block during bulk read", e);
            }

            LOGGER
                .debug(
                    "Bulk read: path={} offset={} length={} blocksLoaded={}/{}",
                    filePath,
                    startOffset,
                    readLength,
                    blockIndex,
                    blockCount
                );

            return result;

        } catch (NoSuchFileException ex) {
            throw ex;
        } catch (Exception ex) {
            LOGGER.error("Failed bulk read: path={} offset={} length={} err={}", filePath, startOffset, readLength, ex.toString());
            throw ex;
        }
    }

    private void releaseSegments(MemorySegment[] segments, int upTo) {
        for (int i = 0; i < upTo; i++) {
            if (segments[i] != null) {
                segmentPool.release(segments[i]);
            }
        }
    }

}

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

            if (bytesRead == 0) {
                throw new BlockLoadFailedException("EOF or empty read at offset " + startOffset);
            }

            int blockIndex = 0;
            long bytesCopied = 0;

            try {
                while (blockIndex < blockCount && bytesCopied < bytesRead) {
                    MemorySegment pooled = segmentPool.tryAcquire(10, TimeUnit.MILLISECONDS);
                    if (pooled == null) {
                        throw new PoolAcquireFailedException("Timeout acquiring segment from pool");
                    }

                    int remaining = (int) (bytesRead - bytesCopied);
                    int toCopy = Math.min(CACHE_BLOCK_SIZE, remaining);

                    if (toCopy > 0) {
                        MemorySegment.copy(bulkEncrypted, bytesCopied, pooled, 0, toCopy);
                    }

                    result[blockIndex++] = pooled;
                    bytesCopied += toCopy;
                }

            } catch (InterruptedException | PoolAcquireFailedException e) {
                releaseSegments(result, blockIndex);
                throw new BlockLoadFailedException("Failed to load blocks", e);
            }

            LOGGER
                .debug(
                    "Bulk read (no padding): path={} offset={} bytesRead={} blocks={}/{}",
                    filePath,
                    startOffset,
                    bytesRead,
                    blockIndex,
                    blockCount
                );

            return result;

        } catch (NoSuchFileException e) {
            throw e;
        } catch (Exception e) {
            LOGGER.error("Bulk read failed: path={} offset={} length={} err={}", filePath, startOffset, readLength, e.toString());
            throw e;
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

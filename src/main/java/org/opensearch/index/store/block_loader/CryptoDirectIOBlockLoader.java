/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_loader;

import static org.opensearch.index.store.block_loader.DirectIOReaderUtil.directIOReadAligned;
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
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.cipher.MemorySegmentDecryptor;
import org.opensearch.index.store.iv.KeyIvResolver;
import org.opensearch.index.store.pool.Pool;

public class CryptoDirectIOBlockLoader implements BlockLoader<RefCountedMemorySegment> {
    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectIOBlockLoader.class);

    private final Pool<RefCountedMemorySegment> segmentPool;
    private final KeyIvResolver keyIvResolver;

    public CryptoDirectIOBlockLoader(Pool<RefCountedMemorySegment> segmentPool, KeyIvResolver keyIvResolver) {
        this.segmentPool = segmentPool;
        this.keyIvResolver = keyIvResolver;
    }

    @Override
    public RefCountedMemorySegment[] load(Path filePath, long startOffset, long blockCount) throws Exception {
        if (!Files.exists(filePath)) {
            throw new NoSuchFileException(filePath.toString());
        }

        if ((startOffset & CACHE_BLOCK_MASK) != 0) {
            throw new IllegalArgumentException("startOffset must be block-aligned: " + startOffset);
        }

        if (blockCount <= 0) {
            throw new IllegalArgumentException("blockCount must be positive: " + blockCount);
        }

        RefCountedMemorySegment[] result = new RefCountedMemorySegment[(int) blockCount];
        long readLength = blockCount << CACHE_BLOCK_SIZE_POWER;

        try (
            Arena arena = Arena.ofConfined();
            FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ, DirectIOReaderUtil.getDirectOpenOption())
        ) {
            MemorySegment readBytes = directIOReadAligned(channel, startOffset, readLength, arena);
            long bytesRead = readBytes.byteSize();

            // decrypt the block to cache.
            MemorySegmentDecryptor
                .decryptInPlace(
                    arena,
                    readBytes.address(),
                    readBytes.byteSize(),
                    keyIvResolver.getDataKey().getEncoded(),
                    keyIvResolver.getIvBytes(),
                    startOffset
                );

            if (bytesRead == 0) {
                throw new RuntimeException("EOF or empty read at offset " + startOffset);
            }

            int blockIndex = 0;
            long bytesCopied = 0;

            try {
                while (blockIndex < blockCount && bytesCopied < bytesRead) {
                    RefCountedMemorySegment handle = segmentPool.tryAcquire(10, TimeUnit.MILLISECONDS);
                    if (handle == null) {
                        throw new RuntimeException("Failed to acquire a block");
                    }

                    MemorySegment pooled = handle.segment();

                    int remaining = (int) (bytesRead - bytesCopied);
                    int toCopy = Math.min(CACHE_BLOCK_SIZE, remaining);

                    if (toCopy > 0) {
                        MemorySegment.copy(readBytes, bytesCopied, pooled, 0, toCopy);
                    }

                    result[blockIndex++] = handle;  // Store the handle, not the segment
                    bytesCopied += toCopy;
                }

            } catch (InterruptedException e) {
                releaseHandles(result, blockIndex);
                throw new RuntimeException("Failed to load blocks", e);
            }

            return result;

        } catch (NoSuchFileException e) {
            throw e;
        } catch (Exception e) {
            LOGGER.error("Bulk read failed: path={} offset={} length={} err={}", filePath, startOffset, readLength, e.toString());
            throw e;
        }
    }

    private void releaseHandles(RefCountedMemorySegment[] handles, int upTo) {
        for (int i = 0; i < upTo; i++) {
            if (handles[i] != null) {
                handles[i].decRef();
            }
        }
    }
}

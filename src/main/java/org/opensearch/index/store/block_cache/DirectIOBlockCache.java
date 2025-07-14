/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import static org.opensearch.index.store.directio.DirectIoUtils.DIRECT_IO_ALIGNMENT;
import static org.opensearch.index.store.directio.DirectIoUtils.getDirectOpenOption;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.mmap.PanamaNativeAccess;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

@SuppressWarnings("preview")
public final class DirectIOBlockCache implements BlockCache {
    private static final Logger LOGGER = LogManager.getLogger(DirectIOBlockCache.class);

    private final Cache<BlockCacheKey, BlockCacheValue> cache;
    private final MemorySegmentPool segmentPool;

    public DirectIOBlockCache(MemorySegmentPool pool, long maxBlocks) {
        this.segmentPool = pool;
        this.cache = Caffeine
            .newBuilder()
            .maximumSize(maxBlocks)
            .executor(Runnable::run) // inline executor to avoid thread switching
            .removalListener((BlockCacheKey key, BlockCacheValue value, RemovalCause cause) -> {
                if (value != null) {
                    // release segment back to pool
                    // eviction is the only path back to the pool, which brings consistency.
                    value.close();
                } else {
                    // Defensive logging, should not happen
                    LOGGER.warn("BlockCache eviction with null value: key={} cause={}", key, cause);
                }
            })
            .build();
    }

    @Override
    public BlockCacheValue get(BlockCacheKey key) {
        return cache.getIfPresent(key);
    }

    @Override
    public BlockCacheValue getOrLoad(BlockCacheKey key, int size) throws IOException {
        try {
            return cache.get(key, k -> {
                try {
                    return loadBlock(k, size);
                } catch (Exception e) {
                    switch (e) {
                        case IOException iOException -> throw new UncheckedIOException(iOException);
                        case RuntimeException runtimeException -> throw runtimeException;
                        default -> throw new RuntimeException("Unexpected exception during block load for key: " + k, e);
                    }
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        } catch (RuntimeException e) {
            throw new IOException("Failed to load block for key: " + key, e);
        }
    }

    // todo make prefetch async.
    @Override
    public void prefetch(BlockCacheKey key, int size) {
        cache.asMap().computeIfAbsent(key, k -> {
            try {
                return loadBlock(k, size);
            } catch (IOException e) {
                LOGGER.warn("Failed to prefetch block for key: {}", key, e);
                return null;
            } catch (RuntimeException e) {
                LOGGER.error("Unexpected runtime exception during prefetch for key: {}", key, e);
                return null;
            } catch (Exception e) {
                LOGGER.error("Unexpected checked exception during prefetch for key: {}", key, e);
                return null;
            }
        });
    }

    @Override
    public void put(BlockCacheKey key, BlockCacheValue value) {
        cache.put(key, value);
    }

    @Override
    public void invalidate(BlockCacheKey key) {
        cache.invalidate(key);
    }

    @Override
    public void clear() {
        cache.invalidateAll();
    }

    private BlockCacheValue loadBlock(BlockCacheKey key, int blockSizeToLoad) throws Exception {
        long offset = key.alignedOffset();

        MemorySegment segment = segmentPool.acquire();

        try (FileChannel channel = FileChannel.open(key.filePath(), StandardOpenOption.READ, getDirectOpenOption())) {
            directIOReadAlignedInto(channel, offset, segment);
            return new PooledBlockCacheValue(segment, blockSizeToLoad, segmentPool);
        } catch (IOException | RuntimeException e) {
            segmentPool.release(segment);
            throw e;
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
    public static void directIOReadAlignedInto(FileChannel channel, long offset, MemorySegment dest) throws IOException {
        int alignment = Math.max(DIRECT_IO_ALIGNMENT, PanamaNativeAccess.getPageSize());

        long alignedOffset = offset & ~(alignment - 1);
        long offsetDelta = offset - alignedOffset;
        long adjustedLength = offsetDelta + dest.byteSize();
        long alignedLength = (adjustedLength + alignment - 1) & ~(alignment - 1);

        if (alignedLength > Integer.MAX_VALUE) {
            throw new IOException("Aligned read size too large: " + alignedLength);
        }

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment alignedSegment = arena.allocate(alignedLength, alignment);
            ByteBuffer buffer = alignedSegment.asByteBuffer();

            int bytesRead = channel.read(buffer, alignedOffset);
            if (bytesRead < offsetDelta + dest.byteSize()) {
                throw new IOException(
                    "Incomplete read: expected="
                        + (offsetDelta + dest.byteSize())
                        + ", actual="
                        + bytesRead
                        + ", alignedOffset="
                        + alignedOffset
                );
            }

            MemorySegment.copy(alignedSegment, offsetDelta, dest, 0, dest.byteSize());
        }
    }

}

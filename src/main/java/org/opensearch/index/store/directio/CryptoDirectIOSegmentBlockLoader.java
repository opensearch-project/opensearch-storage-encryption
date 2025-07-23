/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIOReader.directIOReadAligned;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.BlockLoader;
import org.opensearch.index.store.block_cache.Pool;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.RefCountedMemorySegmentCacheValue;
import org.opensearch.index.store.iv.KeyIvResolver;

@SuppressWarnings("preview")
public class CryptoDirectIOSegmentBlockLoader implements BlockLoader<RefCountedMemorySegment> {
    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectIOSegmentBlockLoader.class);

    private final Pool<MemorySegment> segmentPool;
    private final KeyIvResolver keyIvResolver;

    public CryptoDirectIOSegmentBlockLoader(Pool<MemorySegment> segmentPool, KeyIvResolver keyIvResolver) {
        this.segmentPool = segmentPool;
        this.keyIvResolver = keyIvResolver;
    }

    @Override
    public Optional<BlockCacheValue<RefCountedMemorySegment>> load(BlockCacheKey key, int size) throws Exception {
        long offset = key.offset();

        // Try to acquire a pooled segment for decrypted output
        MemorySegment pooled = segmentPool.tryAcquire(5, TimeUnit.MILLISECONDS);
        if (pooled == null) {
            return Optional.empty(); // Pool exhausted
        }

        try (
            Arena arena = Arena.ofConfined();
            FileChannel channel = FileChannel.open(key.filePath(), StandardOpenOption.READ, DirectIOReader.getDirectOpenOption())
        ) {

            MemorySegment encrypted = directIOReadAligned(channel, offset, size, arena);

            if (encrypted.byteSize() < size) {
                throw new IllegalArgumentException("Encrypted segment too small: expected " + size + ", got " + encrypted.byteSize());
            }

            // Decrypt in-place
            DirectIOReader.decryptSegment(arena, encrypted, offset, keyIvResolver.getDataKey().getEncoded(), keyIvResolver.getIvBytes());

            // Copy decrypted bytes into pooled segment
            MemorySegment.copy(encrypted, 0, pooled, 0, size);

            RefCountedMemorySegment refSegment = new RefCountedMemorySegment(pooled, size, segment -> segmentPool.release(pooled));

            RefCountedMemorySegmentCacheValue cacheValue = new RefCountedMemorySegmentCacheValue(refSegment);
            refSegment.decRef();
            return Optional.of(cacheValue);

        } catch (Throwable t) {
            segmentPool.release(pooled);
            LOGGER.warn("Failed to load or decrypt block at offset {} from file {}: {}", offset, key.filePath(), t.toString());
            return Optional.empty();
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;

@SuppressWarnings("preview")
public final class PinRegistry {
    private static final Logger LOGGER = LogManager.getLogger(PinRegistry.class);

    private final BlockCache<RefCountedMemorySegment> cache;
    private final Path path;
    private final MemorySegment[] slots;
    private final int totalBlocks;

    PinRegistry(BlockCache<RefCountedMemorySegment> cache, Path path, long fileLength) {
        this.cache = cache;
        this.path = path;
        this.totalBlocks = (int) ((fileLength + (1L << CACHE_BLOCK_SIZE_POWER) - 1) >>> CACHE_BLOCK_SIZE_POWER);
        this.slots = new MemorySegment[totalBlocks];
    }

    public MemorySegment acquire(long blockOff) throws IOException {
        final int idx = (int) (blockOff >>> CACHE_BLOCK_SIZE_POWER);
        if (idx < 0 || idx >= totalBlocks) {
            throw new IOException("Block offset OOB: off=" + blockOff + " idx=" + idx + " len=" + totalBlocks);
        }

        final MemorySegment slotVal = slots[idx];
        if (slotVal != null) {
            return slotVal;
        }

        final DirectIOBlockCacheKey key = new DirectIOBlockCacheKey(path, blockOff);

        Optional<BlockCacheValue<RefCountedMemorySegment>> maybeCache = cache.get(key);

        if (maybeCache.isPresent()) {
            BlockCacheValue<RefCountedMemorySegment> cacheVal = maybeCache.get();
            slots[idx] = cacheVal.value().segment();
            return slots[idx];
        }

        BlockCacheValue<RefCountedMemorySegment> cacheVal = cache.getOrLoad(key);

        slots[idx] = cacheVal.value().segment();

        return slots[idx];
    }
}

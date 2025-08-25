/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;

@SuppressWarnings("preview")
public final class PinRegistry {

    private final BlockCache<RefCountedMemorySegment> cache;
    private final Path path;
    private final Slot[] slots;
    private final int totalBlocks;
    private final AtomicInteger owners = new AtomicInteger(1);

    PinRegistry(BlockCache<RefCountedMemorySegment> cache, Path path, long fileLength) {
        this.cache = cache;
        this.path = path;
        this.totalBlocks = (int) ((fileLength + (1L << CACHE_BLOCK_SIZE_POWER) - 1) >>> CACHE_BLOCK_SIZE_POWER);
        this.slots = new Slot[totalBlocks];
        for (int i = 0; i < totalBlocks; i++)
            slots[i] = new Slot();
    }

    PinRegistry retainOwner() {
        owners.incrementAndGet();
        return this;
    }

    void releaseOwner() {
        if (owners.decrementAndGet() == 0)
            releaseOwners();
    }

    void releaseOwners() {
        for (int i = 0; i < totalBlocks; i++) {
            BlockCacheValue<RefCountedMemorySegment> v = slots[i].clearAndGet();
            if (v != null)
                v.unpin();
        }
    }

    MemorySegment acquire(long blockOff, long fileLength) throws IOException {
        final int idx = (int) (blockOff >>> CACHE_BLOCK_SIZE_POWER);
        if (idx < 0 || idx >= totalBlocks) {
            throw new IOException("Block offset OOB: off=" + blockOff + " idx=" + idx + " len=" + totalBlocks);
        }

        final Slot slotVal = slots[idx];
        BlockCacheValue<RefCountedMemorySegment> cur = slotVal.getAcquire();
        if (cur != null) {
            return cur.value().segment();
        }

        final DirectIOBlockCacheKey key = new DirectIOBlockCacheKey(path, blockOff);

        // a couple of quick tries before backing off
        for (int attempt = 0; attempt < 3; attempt++) {
            BlockCacheValue<RefCountedMemorySegment> cacheVal = cache.getOrLoad(key);

            if (cacheVal.tryPin()) {
                if (slotVal.casNullTo(cacheVal)) {
                    return cacheVal.value().segment();
                } else {
                    // someone published first
                    cacheVal.unpin();
                    BlockCacheValue<RefCountedMemorySegment> published = slotVal.getAcquire();
                    if (published != null) {
                        return published.value().segment();
                    }
                }
            } else {
                // someone else published it.
                BlockCacheValue<RefCountedMemorySegment> published = slotVal.getAcquire();
                if (published != null) {
                    return published.value().segment();
                }
            }

            if (attempt == 0) {
                Thread.onSpinWait();
            } else {
                LockSupport.parkNanos(200_000L);
            }
        }

        throw new IOException("Failed to pin block after retries, off=" + blockOff + " path=" + path);
    }
}

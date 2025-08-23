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
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;

@SuppressWarnings("preview")
public final class PinRegistry {
    private final BlockCache<RefCountedMemorySegment> cache;
    private final Path path;
    private final AtomicReferenceArray<BlockCacheValue<RefCountedMemorySegment>> pinned;
    private final AtomicInteger owners = new AtomicInteger(1);

    PinRegistry(BlockCache<RefCountedMemorySegment> cache, Path path, long fileLength) {
        this.cache = cache;
        this.path = path;
        // Calculate exact number of blocks needed: ceil(fileLength / CACHE_BLOCK_SIZE)
        int numBlocks = (int) ((fileLength + (1L << CACHE_BLOCK_SIZE_POWER) - 1) >>> CACHE_BLOCK_SIZE_POWER);
        this.pinned = new AtomicReferenceArray<>(numBlocks);
    }

    // Called by slices/clones to share the registry
    PinRegistry retainOwner() {
        owners.incrementAndGet();
        return this;
    }

    void releaseOwner() {
        int newCount = owners.decrementAndGet();
        assert newCount >= 0 : "Owner count went negative: " + newCount;

        if (newCount == 0) {
            releaseOwners();
        }
    }

    void releaseOwners() {
        int totalSlots = pinned.length();

        for (int i = 0; i < totalSlots; i++) {
            BlockCacheValue<RefCountedMemorySegment> val = pinned.getAndSet(i, null);
            if (val != null) {
                val.unpin();
            }
        }
    }

    MemorySegment acquire(long blockOff) throws IOException {
        final DirectIOBlockCacheKey key = new DirectIOBlockCacheKey(path, blockOff);
        final int arrayIndex = (int) (blockOff >>> CACHE_BLOCK_SIZE_POWER);

        // Bounds check for safety
        if (arrayIndex < 0 || arrayIndex >= pinned.length()) {
            throw new IOException("Block offset out of bounds: off=" + blockOff + " index=" + arrayIndex + " length=" + pinned.length());
        }

        // Fast path: already pinned
        BlockCacheValue<RefCountedMemorySegment> val = pinned.get(arrayIndex);
        if (val != null) {
            return val.value().segment();
        }

        // Retry loop to handle cache pressure
        for (int attempt = 0; attempt < 3; attempt++) {
            val = cache.getOrLoad(key);

            // Load and try to pin
            if (val.tryPin()) {
                // Atomic compare-and-set to handle concurrent access
                if (pinned.compareAndSet(arrayIndex, null, val)) {
                    return val.value().segment();
                } else {
                    // Another thread won, unpin our attempt and use theirs
                    val.unpin();
                    BlockCacheValue<RefCountedMemorySegment> existing = pinned.get(arrayIndex);
                    if (existing != null) {
                        return existing.value().segment();
                    }
                    // Fall through to retry
                }
            }

            // Check if someone else pinned while we were trying
            BlockCacheValue<RefCountedMemorySegment> existing = pinned.get(arrayIndex);
            if (existing != null) {
                return existing.value().segment();
            }

            // Brief pause before retry to let cache pressure subside
            if (attempt < 2) {
                LockSupport.parkNanos(200_000L); // 200 microseconds
            }
        }

        throw new IOException("Failed to pin block after retries, off=" + blockOff + " path=" + path);
    }
}

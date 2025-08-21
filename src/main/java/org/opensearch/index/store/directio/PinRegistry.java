/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;

@SuppressWarnings("preview")
public final class PinRegistry {
    private final BlockCache<RefCountedMemorySegment> cache;
    private final Path path;
    private final ConcurrentHashMap<DirectIOBlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> pinned = new ConcurrentHashMap<>();
    private final AtomicInteger owners = new AtomicInteger(1);

    PinRegistry(BlockCache<RefCountedMemorySegment> cache, Path path) {
        this.cache = cache;
        this.path = path;
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
        int pinnedCount = pinned.size();

        var it = pinned.values().iterator();
        int unpinnedCount = 0;
        while (it.hasNext()) {
            try {
                it.next().unpin();
                unpinnedCount++;
            } finally {
                it.remove();
            }
        }

        assert unpinnedCount == pinnedCount : "Unpinned count (" + unpinnedCount + ") != initial pinned count (" + pinnedCount + ")";
        assert pinned.isEmpty() : "Pinned map should be empty after releaseOwners()";
    }

    MemorySegment acquire(long blockOff) throws IOException {
        final DirectIOBlockCacheKey key = new DirectIOBlockCacheKey(path, blockOff);

        // fast path: already pinned
        BlockCacheValue<RefCountedMemorySegment> val = pinned.get(key);
        if (val != null)
            return val.value().segment();

        // Retry loop to handle cache pressure
        for (int attempt = 0; attempt < 3; attempt++) {
            val = cache.getOrLoad(key);

            // load and try to pin
            if (val.tryPin()) {
                BlockCacheValue<RefCountedMemorySegment> prev = pinned.putIfAbsent(key, val);
                if (prev != null) { // another thread won
                    val.unpin();
                    return prev.value().segment();
                }
                return val.value().segment();
            }

            // Check if someone else pinned while we were trying
            BlockCacheValue<RefCountedMemorySegment> existing = pinned.get(key);
            if (existing != null)
                return existing.value().segment();

            // Brief pause before retry to let cache pressure subside
            if (attempt < 2) {
                LockSupport.parkNanos(200_000L); // 200 microseconds
            }
        }

        throw new IOException("Failed to pin block after retries, off=" + blockOff + " path=" + path);
    }
}

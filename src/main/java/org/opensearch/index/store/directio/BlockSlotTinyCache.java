/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.IOException;
import java.nio.file.Path;

import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;

/**
 * Fast L1 cache for recently accessed blocks.
 * Sits in front of the main Caffeine cache to reduce cache lookup overhead.
 *
 * <h2>Generation-based Staleness Detection</h2>
 * Each cached entry stores a generation number snapshot from the underlying
 * RefCountedMemorySegment. The generation counter is incremented when a segment
 * is evicted from the main cache (via close()), invalidating all L1 cache references.
 *
 * This prevents returning stale data in two scenarios:
 *
 * <h3>Scenario 1: Eviction without reuse</h3>
 * 1. File A's block cached in L1 → RefCountedMemorySegment@X (gen=5)
 * 2. Main cache evicts → close() increments gen to 6
 * 3. L1 checks: cached gen(5) ≠ current gen(6) → reload from main cache
 *
 * <h3>Scenario 2: Eviction with pool reuse</h3>
 * 1. File A's block cached in L1 → RefCountedMemorySegment@X (gen=5)
 * 2. Main cache evicts → close() increments gen to 6, segment returned to pool
 * 3. Pool reuses segment for File B → RefCountedMemorySegment@X (gen=6, new data)
 * 4. Reader requests File A → L1 checks: cached gen(5) ≠ current gen(6) → reload
 *
 */
public class BlockSlotTinyCache {

    private static final int SLOT_COUNT = 32;
    private static final int SLOT_MASK = SLOT_COUNT - 1;

    /**
     * Result of a cache lookup operation, containing the value and whether it was found in cache.
     *
     * @param value the cached block value
     * @param wasCacheHit true if the value was found in cache, false otherwise
     */
    public record LookupResult(BlockCacheValue<RefCountedMemorySegment> value, boolean wasCacheHit) {
    }

    private record Slot(long blockIdx, BlockCacheValue<RefCountedMemorySegment> val, int generation) {
    }

    private final BlockCache<RefCountedMemorySegment> cache;
    private final Path path;
    private final Slot[] slots;

    // Pre-allocated keys for hot slots to reduce allocation pressure
    private final FileBlockCacheKey[] slotKeys;

    private long lastBlockIdx = -1;
    private BlockCacheValue<RefCountedMemorySegment> lastVal;
    private int lastGeneration = -1;

    BlockSlotTinyCache(BlockCache<RefCountedMemorySegment> cache, Path path, long fileLength) {
        this.cache = cache;
        this.path = path;
        this.slots = new Slot[SLOT_COUNT];
        this.slotKeys = new FileBlockCacheKey[SLOT_COUNT];
    }

    /**
     * Acquires a reference-counted memory segment for the specified block offset.
     *
     * <p>This method implements a fast L1 cache lookup with generation-based staleness detection.
     * It first checks the last accessed block, then checks the slot cache, and finally falls back
     * to the main cache if needed.
     *
     * @param blockOff the byte offset within the file, must be block-aligned
     * @return a LookupResult containing the cached value and whether it was found in any cache tier
     * @throws IOException if an I/O error occurs while loading the block from the underlying storage
     */
    public LookupResult acquireRefCountedValue(long blockOff) throws IOException {
        final long blockIdx = blockOff >>> CACHE_BLOCK_SIZE_POWER;

        // Fast path: last accessed (avoid slot calculation if possible)

        if (blockIdx == lastBlockIdx && lastVal != null) {
            RefCountedMemorySegment seg = lastVal.value();
            if (seg.getGeneration() == lastGeneration) {
                return new LookupResult(lastVal, true);
            }
        }

        final int slotIdx = (int) (blockIdx & SLOT_MASK);

        // Slot lookup - single memory access, better cache locality
        Slot slot = slots[slotIdx];
        if (slot != null && slot.blockIdx == blockIdx) {
            BlockCacheValue<RefCountedMemorySegment> val = slot.val;
            if (val != null) {
                // Check generation to detect segment eviction or reuse
                int currentGen = val.value().getGeneration();
                if (currentGen == slot.generation) {
                    // Cache both slot and last reference atomically
                    lastBlockIdx = blockIdx;
                    lastVal = val;
                    lastGeneration = currentGen;
                    return new LookupResult(val, true);
                }
                // Generation mismatch - segment was evicted/recycled, fall through to reload
            }
        }

        // Cache miss in L1 - check main cache
        FileBlockCacheKey key = slotKeys[slotIdx];
        if (key == null || key.fileOffset() != blockOff) {
            key = new FileBlockCacheKey(path, blockOff);
            slotKeys[slotIdx] = key; // Cache for future use
        }

        // Check main cache first to determine hit/miss
        BlockCacheValue<RefCountedMemorySegment> val = cache.get(key);
        boolean wasCacheHit = (val != null);

        if (!wasCacheHit) {
            val = cache.getOrLoad(key);
        }

        // Single update point - create new slot record with current generation
        int generation = val.value().getGeneration();
        slots[slotIdx] = new Slot(blockIdx, val, generation);
        lastBlockIdx = blockIdx;
        lastVal = val;
        lastGeneration = generation;

        return new LookupResult(val, wasCacheHit);
    }

    /**
     * Clears all cached entries and resets the cache state.
     * 
     * <p>This method invalidates all slots, clears the last accessed block reference,
     * and releases all cached keys. It should be called when the cache needs to be
     * completely reset, such as when the underlying file is closed or invalidated.
     */
    public void clear() {
        lastBlockIdx = -1;
        lastVal = null;
        lastGeneration = -1;
        for (int i = 0; i < SLOT_COUNT; i++) {
            slots[i] = null;
            slotKeys[i] = null; // Clear cached keys
        }
    }
}

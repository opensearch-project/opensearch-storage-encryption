/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.locks.LockSupport;

import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;

/**
 * An optimized tiny L1 block cache in front of the main Caffeine L2 cache for fast lookups.
 *
 * Sequential and near-sequential reads (common in Lucene) tend to access the same blocks repeatedly.
 * Even with a large Caffeine cache, every lookup involves ConcurrentHashMap operations, 
 * and other overhead which we want to avoid on a very hot paths.
 * This tiny cache eliminates that overhead for the most recently accessed blocks.
 *
 *
 * PIN/UNPIN LIFECYCLE FOR A SINGLE IndexInput
 * -------------------------------------------
 *  When and index input moves from one
 * block to another, it pins the new block and unpins the previous one. This keeps
 * memory usage extremely small: at any moment, *only one block is pinned per
 * IndexInput*.
 *
 * Typical flow:
 *    1. acquireRefCountedValue(offset) → returns a *pinned* block
 *    2. IndexInput reads from that block
 *    3. When moving to a new block:
 *          oldBlock.unpin()        // refCount--
 *          newBlock.tryPin()       // refCount++
 *
 *
 * Blocks (MemorySegments) come from a pool. When a block is evicted from the main
 * cache, the pool increments the segment's generation and may reuse it for a
 * completely different file or offset.
 *
 * This means cached references can become stale:
 *    • same memory
 *    • different contents
 *    • different file/offset
 *
 * To prevent returning a recycled segment, the tiny cache records the generation at
 * the moment the slot was filled and compares it against the current generation:
 *
 *        cachedGeneration == segment.getGeneration()
 *
 * If they differ, the segment was evicted/reused → the slot is ignored and we reload.
 *
 *
 * BlockSlotTinyCache only stores *pointers* (blockIdx, value, generation). It never
 * keeps segments pinned. Only the IndexInput pins the block it is actively reading.
 *
 * On lookup:
 *    • First check thread-local MRU (fastest path).
 *    • Then check the slot (fast path).
 *    • Both require: matching blockIdx + matching generation + successful tryPin().
 *    • If any check fails, we fall back to the main cache and fill the slot/MRU.
 *
 * Because the tiny cache never holds pinned blocks, it never blocks eviction, and
 * generation mismatches automatically invalidate stale entries.
 *
 */

public class BlockSlotTinyCache {

    /**
     * Mutable holder for cache hit/miss status to avoid allocation on hot path.
     * Reuse a single instance index-input to track whether
     * the last acquired value call hit cache or loaded from disk.
     */
    public static final class CacheHitHolder {
        private boolean wasCacheHit;

        public void reset() {
            wasCacheHit = false;
        }

        public boolean wasCacheHit() {
            return wasCacheHit;
        }

        void setWasCacheHit(boolean hit) {
            this.wasCacheHit = hit;
        }
    }

    // 32 slots provides good hit rate while keeping memory footprint tiny (~8KB with padding).
    // fits easily into cpu LI cache.
    private static final int SLOT_COUNT = 32;
    private static final int SLOT_MASK = SLOT_COUNT - 1;

    /**
     * Mutable slot object reused in place.
     *
     * PADDING OPTIMIZATION:
     * CPU cache lines are typically 64 bytes. When multiple threads access adjacent memory locations,
     * they can cause "false sharing" - where updating one slot invalidates another thread's cache line,
     * causing expensive cross-core cache coherency traffic. 
     *
     * Each padding field (long) is 8 bytes:
     * - 7 longs before = 56 bytes padding
     * - 3 hot fields (long + reference + int) = ~24 bytes (with alignment)
     * - 7 longs after = 56 bytes padding
     * Total: ~136 bytes per slot, ensuring hot fields sit on their own cache line.
     *
     * This matters because multiple threads may simultaneously access different slots in the array,
     * and without padding, they'd constantly invalidate each other's CPU caches.
     */
    private static final class Slot {
        // Padding before the hot fields (helps isolate from object header / neighbors)
        @SuppressWarnings("unused")
        long p1, p2, p3, p4, p5, p6, p7;

        // Hot fields - accessed on every slot check
        long blockIdx = -1;         // Which block this slot caches
        BlockCacheValue<RefCountedMemorySegment> val;  // The cached value
        int generation;              // Generation number to detect pool reuse

        // Padding after the hot fields (helps keep next Slot on a different cache line)
        @SuppressWarnings("unused")
        long q1, q2, q3, q4, q5, q6, q7;
    }

    /**
     * Thread-local mutable MRU (Most Recently Used) entry.
     *
     * WHY THREAD-LOCAL:
     * This is an optimization over using (varhandles or volaties). THREAD-LOCAL (small) 
     * give us zero synchronization cost which has huge benefit on very hot path. Each thread tracks its
     * most recent block access independently. Since Lucene tends to read sequentially within
     * a thread (e.g., scanning a posting list).
     *
     */
    private static final class LastAccessed {
        long blockIdx = -1;
        BlockCacheValue<RefCountedMemorySegment> val;
        int generation;
    }

    private final BlockCache<RefCountedMemorySegment> cache;
    private final Path path;

    private final Slot[] slots;
    private final FileBlockCacheKey[] slotKeys;

    /** Thread-local MRU; no allocation after initialization. */
    private final ThreadLocal<LastAccessed> lastAccessed = ThreadLocal.withInitial(LastAccessed::new);

    public BlockSlotTinyCache(BlockCache<RefCountedMemorySegment> cache, Path path, long fileLength) {
        this.cache = cache;
        this.path = path;

        this.slots = new Slot[SLOT_COUNT];
        for (int i = 0; i < SLOT_COUNT; i++) {
            slots[i] = new Slot();
        }

        this.slotKeys = new FileBlockCacheKey[SLOT_COUNT];
    }

    /**
     * Returns an already-pinned block. Caller "must" unpin() when done.
     *
     * @param blockOff the block offset to acquire
     * @return the pinned block cache value
     * @throws IOException if unable to acquire the block
     */
    public BlockCacheValue<RefCountedMemorySegment> acquireRefCountedValue(long blockOff) throws IOException {
        return acquireRefCountedValue(blockOff, null);
    }

    /**
     * Returns an already-pinned block with hit/miss tracking. Caller "must" unpin() when done.
     *
     * @param blockOff the block offset to acquire
     * @param hitHolder optional holder to record cache hit status (null to skip tracking)
     * @return the pinned block cache value
     * @throws IOException if unable to acquire the block
     */
    public BlockCacheValue<RefCountedMemorySegment> acquireRefCountedValue(long blockOff, CacheHitHolder hitHolder) throws IOException {

        final long blockIdx = blockOff >>> CACHE_BLOCK_SIZE_POWER;

        // TIER 1: Thread-local MRU check (fastest path - zero synchronization)
        LastAccessed last = lastAccessed.get();
        if (last.blockIdx == blockIdx) {
            BlockCacheValue<RefCountedMemorySegment> v = last.val;
            // Generation check ensures the pooled segment wasn't recycled and reused.
            if (v != null && v.value().getGeneration() == last.generation && v.tryPin()) {
                if (hitHolder != null) {
                    hitHolder.setWasCacheHit(true); // L1 hit
                }
                return v;
            }
        }

        // TIER 2: Shared slot array check (fast path - plain reads with cache-line padding)
        // Apply a small XOR fold to mix high bits into the low bits before masking.
        // Without this mixing, sequential block indices alias every SLOT_COUNT blocks,
        // causing predictable collisions. The fold spreads access patterns more evenly.
        final int slotIdx = (int) ((blockIdx ^ (blockIdx >>> 17)) & SLOT_MASK);

        Slot slot = slots[slotIdx];
        if (slot.blockIdx == blockIdx) {
            BlockCacheValue<RefCountedMemorySegment> v = slot.val;
            if (v != null) {
                int currentGen = v.value().getGeneration();
                // Generation check is critical: memory segments are pooled and reused.
                // Without this check, we could return a segment that was recycled and now
                // contains completely different (or partially overwritten) data.
                if (currentGen == slot.generation && v.tryPin()) {
                    // Promote to thread-local MRU for next access (note: we do in-place update, no allocations)
                    last.blockIdx = blockIdx;
                    last.val = v;
                    last.generation = currentGen;
                    if (hitHolder != null) {
                        hitHolder.setWasCacheHit(true); // L1 slot hit
                    }
                    return v;
                }
            }
        }

        // TIER 3: Main Caffeine cache lookup with retry logic
        // This path is slower but unavoidable for true cache misses or highly contended blocks.
        final int maxAttempts = 10;
        BlockCacheValue<RefCountedMemorySegment> val = null;
        boolean wasInCache = false;

        // KEY REUSE OPTIMIZATION:
        // FileBlockCacheKey is small but not free to allocate. Since we're hashing by slot,
        // there's a good chance the same slot will be used for the same file/offset again.
        // Reusing the key saves object allocation overhead.
        FileBlockCacheKey key = slotKeys[slotIdx];
        if (key == null || key.fileOffset() != blockOff) {
            key = new FileBlockCacheKey(path, blockOff);
            slotKeys[slotIdx] = key;
        }

        // RETRY LOOP:
        // Under heavy concurrent load, a segment might be evicted from the pool between
        // when we get it from the cache and when we try to pin it. This is rare but possible
        // when the pool is small relative to working set size. Exponential backoff gives
        // time for churn to settle.
        for (int attempts = 0; attempts < maxAttempts; attempts++) {
            val = cache.get(key);
            if (val == null) {
                // Not in cache at all - load from disk (decrypt, decompress if needed)
                val = cache.getOrLoad(key);
                wasInCache = false; // Had to load from disk
            } else {
                wasInCache = true; // Found in main cache
            }

            if (val != null && val.tryPin()) {
                int gen = val.value().getGeneration();

                // Populate both caches for next access (in-place updates, zero allocations)
                slot.blockIdx = blockIdx;
                slot.val = val;
                slot.generation = gen;

                last.blockIdx = blockIdx;
                last.val = val;
                last.generation = gen;

                if (hitHolder != null) {
                    hitHolder.setWasCacheHit(wasInCache); // L2 cache or disk load
                }

                return val;
            }

            // todo: we can also use thread-spin-wait
            if (attempts < maxAttempts - 1) {
                LockSupport.parkNanos(50_000L << attempts);
            }
        }

        throw new IOException("Unable to pin memory segment for block offset " + blockOff + " after " + maxAttempts + " attempts");
    }

    /**
     * Reset the tiny cache.
     *
     * Called when the file is closed or when we need to invalidate cached entries.
     * Thread-local entries are removed (other threads will lazily refresh on next access).
     * Shared slot entries are cleared immediately to prevent stale references.
     *
     * Note: This is not synchronized because:
     * 1. It's typically called during shutdown/close when concurrent access is unlikely
     * 2. In the rare case of concurrent access during clear, worst case is a cache miss
     *    (incorrect generation will be detected and we'll fall through to main cache)
     */
    public void clear() {
        lastAccessed.remove();
        for (int i = 0; i < SLOT_COUNT; i++) {
            Slot s = slots[i];
            s.blockIdx = -1;
            s.val = null;
            s.generation = 0;
            slotKeys[i] = null;
        }
    }
}

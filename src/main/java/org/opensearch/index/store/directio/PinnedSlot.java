/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import java.lang.invoke.VarHandle;

import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;

/**
 * A lock-free slot for holding a single pinned cache block.
 * 
 * <p>This class provides efficient, concurrent access to cached memory segments using
 * VarHandle-based atomic operations. Each slot corresponds to a specific block
 * offset in a file and holds at most one pinned cache value.
 * 
 * <h3>Design Principles:</h3>
 * <ul>
 *   <li><b>Lock-free:</b> All operations use atomic compareAndSet/volatile reads</li>
 *   <li><b>Winner-takes-all:</b> First thread to pin a block wins, others back off</li>
 * </ul>
 * 
 * <h3>Memory Ordering:</h3>
 * <ul>
 *   <li>{@link #casNullTo} provides full acquire/release semantics</li>
 *   <li>{@link #getAcquire} provides acquire semantics via volatile read</li>
 *   <li>{@link #clearAndGet} provides acquire/release semantics via atomic exchange</li>
 * </ul>
 * 
 * @opensearch.internal
 */
final class PinnedSlot {
    volatile BlockCacheValue<RefCountedMemorySegment> val;

    private static final VarHandle VH_VAL;
    static {
        try {
            VH_VAL = java.lang.invoke.MethodHandles.lookup()
                .findVarHandle(PinnedSlot.class, "val", BlockCacheValue.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Atomically sets the slot value from null to the given value.
     * 
     * <p>This operation uses compare-and-set semantics with strict memory ordering.
     * It will only succeed if the current value is null, making it ideal for
     * winner-takes-all scenarios where multiple threads compete to pin a block.
     * 
     * @param newVal the cache value to store (must not be null)
     * @return true if the operation succeeded (value was null and is now newVal),
     *         false if another thread already set a value
     */
    boolean casNullTo(BlockCacheValue<RefCountedMemorySegment> newVal) {
        return VH_VAL.compareAndSet(this, null, newVal);
    }

    /**
     * Returns the current slot value with acquire memory ordering.
     * 
     * <p>This provides a consistent view of the slot state with proper memory
     * ordering guarantees. The volatile read ensures that any writes that
     * happened-before the publication of this value are visible.
     * 
     * @return the current cache value, or null if no block is pinned
     */
    BlockCacheValue<RefCountedMemorySegment> getAcquire() {
        // Volatile read provides acquire semantics - all writes that happened-before
        // the store are visible after this load completes
        return (BlockCacheValue<RefCountedMemorySegment>) VH_VAL.getVolatile(this);
    }

    /**
     * Atomically clears the slot and returns the previous value.
     * 
     * <p>This operation uses atomic exchange (getAndSet) semantics, providing
     * full memory ordering. It's typically used during cleanup to ensure
     * proper unpinning of cached blocks.
     * 
     * @return the previous cache value, or null if the slot was already empty
     */
    BlockCacheValue<RefCountedMemorySegment> clearAndGet() {
        return (BlockCacheValue<RefCountedMemorySegment>) VH_VAL.getAndSet(this, null);
    }
}

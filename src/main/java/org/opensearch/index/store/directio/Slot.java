/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import java.lang.invoke.VarHandle;

import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;

final class Slot {
    // Single volatile field = cheapest possible published state
    volatile BlockCacheValue<RefCountedMemorySegment> val;

    private static final VarHandle VH_VAL;
    static {
        try {
            VH_VAL = java.lang.invoke.MethodHandles.lookup().findVarHandle(Slot.class, "val", BlockCacheValue.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    boolean casNullTo(BlockCacheValue<RefCountedMemorySegment> newVal) {
        return VH_VAL.compareAndSet(this, null, newVal);
    }

    BlockCacheValue<RefCountedMemorySegment> getAcquire() {
        // Volatile read is enough; use getOpaque() if you want even cheaper, but volatile is safer.
        return (BlockCacheValue<RefCountedMemorySegment>) VH_VAL.getVolatile(this);
    }

    BlockCacheValue<RefCountedMemorySegment> clearAndGet() {
        return (BlockCacheValue<RefCountedMemorySegment>) VH_VAL.getAndSet(this, null);
    }
}

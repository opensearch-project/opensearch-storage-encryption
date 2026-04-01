/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCacheValue;

/**
 * Entry stored in the RadixBlockTable L1 cache.
 *
 * Wraps a {@link BlockCacheValue} together with the generation of the underlying
 * {@link RefCountedMemorySegment} at the time the entry was published to L1.
 *
 * <p>The publish-time generation is needed to detect stale entries: when the L2
 * cache evicts a block, the segment's generation is bumped via {@code close()}.
 * On L1 read, if the current generation differs from {@code publishGeneration},
 * the entry is stale and must be treated as an L1 miss.
 *
 * <p>This is a lightweight alternative to BlockSlotTinyCache's packed stamp
 * approach ({@code [generation:32 | hash:32]}). Since RadixBlockTable uses
 * direct indexing (no hash collisions), the hash component is unnecessary —
 * only the generation snapshot is needed.
 */
final class L1CacheEntry {
    final BlockCacheValue<RefCountedMemorySegment> value;
    final int publishGeneration;

    L1CacheEntry(BlockCacheValue<RefCountedMemorySegment> value, int publishGeneration) {
        this.value = value;
        this.publishGeneration = publishGeneration;
    }
}

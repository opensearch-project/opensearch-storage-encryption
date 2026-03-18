/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import java.nio.file.Path;

import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCacheValue;

/**
 * Thread-local single-block pin holder that eliminates per-read CAS overhead for slices.
 *
 * <h2>Solution</h2>
 * PinScope holds at most ONE pinned block per thread. Consecutive reads to the same
 * block reuse the existing pin (0 CAS). Block transitions unpin the old block and pin
 * the new one (2 CAS, but only every ~1024 reads for 8-byte reads in an 8KB block).
 *
 * @opensearch.internal
 */
public final class PinScope {

    private static final ThreadLocal<PinScope> CURRENT = ThreadLocal.withInitial(PinScope::new);

    private BlockCacheValue<RefCountedMemorySegment> pinnedBlock;
    private long pinnedBlockOffset = -1L;
    private Path pinnedPath;

    private PinScope() {}

    public static PinScope current() {
        return CURRENT.get();
    }

    /**
     * Returns the currently pinned block if it matches the requested file and offset.
     */
    public BlockCacheValue<RefCountedMemorySegment> getIfMatch(Path path, long blockOffset) {
        if (blockOffset == pinnedBlockOffset && pinnedBlock != null && path == pinnedPath) {
            return pinnedBlock;
        }
        return null;
    }

    /**
     * Pins a new block, unpinning any previously held block.
     * The caller must have already pinned the block — PinScope takes ownership of that pin.
     */
    public void pin(Path path, long blockOffset, BlockCacheValue<RefCountedMemorySegment> block) {
        final BlockCacheValue<RefCountedMemorySegment> blockCacheValue = pinnedBlock;
        if (blockCacheValue != null && blockCacheValue != block) {
            blockCacheValue.unpin();
        }
        pinnedBlock = block;
        pinnedBlockOffset = blockOffset;
        pinnedPath = path;
    }

    /**
     * Releases the currently pinned block, if any.
     */
    public void release() {
        final BlockCacheValue<RefCountedMemorySegment> blockCacheValue = pinnedBlock;
        if (blockCacheValue != null) {
            pinnedBlock = null;
            pinnedBlockOffset = -1L;
            pinnedPath = null;
            blockCacheValue.unpin();
        }
    }
}

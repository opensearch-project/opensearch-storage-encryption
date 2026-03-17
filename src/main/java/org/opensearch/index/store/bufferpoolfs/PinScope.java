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
 * <h2>Problem</h2>
 * After PR #129, every {@code readLong()} on a slice does pin (CAS refcount++) then
 * unpin (CAS refcount--) = 2 CAS ops per read. With millions of reads per aggregation
 * query, this causes a 1.5-2.8x regression.
 *
 * <h2>Solution</h2>
 * PinScope holds at most ONE pinned block per thread. Consecutive reads to the same
 * block reuse the existing pin (0 CAS). Block transitions unpin the old block and pin
 * the new one (2 CAS, but only every ~1024 reads for 8-byte reads in an 8KB block).
 *
 * <h2>Why this is safe</h2>
 * <ul>
 *   <li>Lucene's IndexInput contract: single-threaded per instance. One thread uses
 *       one slice at a time, so one pin per thread is sufficient.</li>
 *   <li>Max simultaneous pins = number of search threads (~20), not number of slices
 *       (50K+). Pool exhaustion from pin accumulation cannot occur.</li>
 *   <li>PinScope holds a pin (refCount &gt; 0), so the block cannot be returned to
 *       pool or recycled while held.</li>
 * </ul>
 *
 * <h2>CAS reduction</h2>
 * 8KB block / 8 bytes per read = ~1024 reads per block. 2M reads → ~2000 block
 * transitions → ~4000 CAS ops. Down from 4,000,000.
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
     * Safe without generation check: PinScope holds a pin so the block cannot be recycled.
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
        final BlockCacheValue<RefCountedMemorySegment> old = pinnedBlock;
        if (old != null && old != block) {
            old.unpin();
        }
        pinnedBlock = block;
        pinnedBlockOffset = blockOffset;
        pinnedPath = path;
    }

    /**
     * Releases the currently pinned block, if any.
     */
    public void release() {
        final BlockCacheValue<RefCountedMemorySegment> b = pinnedBlock;
        if (b != null) {
            pinnedBlock = null;
            pinnedBlockOffset = -1L;
            pinnedPath = null;
            b.unpin();
        }
    }
}

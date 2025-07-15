/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIoUtils.DIRECT_IO_ALIGNMENT;

import java.nio.file.Path;

import org.opensearch.index.store.block_cache.BlockCacheKey;

public final class DirectIOBlockCacheKey implements BlockCacheKey {
    private final Path filePath;
    private final long alignedOffset;

    public DirectIOBlockCacheKey(Path filePath, long offset) {
        this.filePath = filePath;
        this.alignedOffset = alignDown(offset);
    }

    private static long alignDown(long offset) {
        return offset & ~(DIRECT_IO_ALIGNMENT - 1);
    }

    @Override
    public Path filePath() {
        return filePath;
    }

    @Override
    public long alignedOffset() {
        return alignedOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof BlockCacheKey other))
            return false;
        return alignedOffset == other.alignedOffset() && filePath.equals(other.filePath());
    }

    @Override
    public int hashCode() {
        return 31 * filePath.hashCode() + Long.hashCode(alignedOffset);
    }

    @Override
    public String toString() {
        return "DirectIOBlockCacheKey{" + filePath + "@" + alignedOffset + "}";
    }
}

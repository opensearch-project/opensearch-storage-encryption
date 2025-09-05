/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import java.nio.file.Path;

import org.opensearch.index.store.block_cache.BlockCacheKey;

/**
 * Cache key for DirectIO block cache entries.
 * 
 * <p>Each cache key uniquely identifies a block within a file by combining:
 * <ul>
 * <li>File name - extracted from the file path for fast comparison</li>
 * <li>File offset - identifies the position within the file where the block starts</li>
 * </ul>
 * 
 * <p>Hash code is precomputed for optimal performance in concurrent environments.
 */
public final class DirectIOBlockCacheKey implements BlockCacheKey {

    private final Path filePath;
    private final long fileOffset;
    private final String fileName;
    private int hash; // 0 means "not yet computed"

    public DirectIOBlockCacheKey(Path filePath, long fileOffset) {
        this.filePath = filePath.toAbsolutePath().normalize();
        this.fileOffset = fileOffset;
        this.fileName = this.filePath.getFileName().toString();
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0) {
            // compute once
            h = 31 * fileName.hashCode() + Long.hashCode(fileOffset);
            if (h == 0)
                h = 1; // avoid sentinel clash
            hash = h;
        }
        return h;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof DirectIOBlockCacheKey other))
            return false;
        return fileOffset == other.fileOffset && fileName.equals(other.fileName);
    }

    @Override
    public String toString() {
        return "DirectIOBlockCacheKey[filePath=" + filePath + ", fileOffset=" + fileOffset + "]";
    }

    @Override
    public long offset() {
        return fileOffset;
    }

    public Path filePath() {
        return filePath;
    }

    public long fileOffset() {
        return fileOffset;
    }
}

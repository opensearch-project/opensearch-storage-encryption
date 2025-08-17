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
 * <li>File path - identifies which file the block belongs to</li>
 * <li>File offset - identifies the position within the file where the block starts</li>
 * </ul>
 * 
 * <p>The file path is automatically normalized to absolute form to ensure consistent
 * cache key matching regardless of how the path was originally specified.
 * 
 * @param filePath the absolute, normalized path to the file containing this block
 * @param fileOffset the byte offset within the file where this block starts
 */
public record DirectIOBlockCacheKey(Path filePath, long fileOffset) implements BlockCacheKey {

    /**
     * constructor that normalizes the file path to ensure consistent cache key matching.
     * 
     * <p>The path is converted to absolute form and normalized to remove redundant elements
     * like "." and ".." segments. This ensures that cache keys for the same file are identical
     * regardless of how the path was originally specified (relative vs absolute, with or
     * without redundant path elements).
     */
    public DirectIOBlockCacheKey {
        filePath = filePath.toAbsolutePath().normalize();
    }

    /**
     * Returns the file offset for this cache key.
     * 
     * @return the byte offset within the file where this block starts
     */
    @Override
    public long offset() {
        return fileOffset;
    }
}

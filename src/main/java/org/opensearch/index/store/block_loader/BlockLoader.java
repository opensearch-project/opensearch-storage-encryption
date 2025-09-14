/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_loader;

import java.io.IOException;
import java.nio.file.Path;

import org.opensearch.index.store.block_cache.BlockCacheKey;

public interface BlockLoader<T> {

    /**
     * Thrown when the memory segment pool is under pressure and cannot allocate segments.
     */
    class PoolPressureException extends IOException {
        public PoolPressureException(String message) {
            super(message);
        }

        public PoolPressureException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Thrown when unable to acquire a memory segment from the pool within timeout.
     */
    class PoolAcquireFailedException extends IOException {
        public PoolAcquireFailedException(String message) {
            super(message);
        }

        public PoolAcquireFailedException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Thrown when block loading fails due to I/O or other loading errors.
     */
    class BlockLoadFailedException extends IOException {
        public BlockLoadFailedException(String message) {
            super(message);
        }

        public BlockLoadFailedException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Load one or more blocks efficiently, returning raw memory segments.
     * 
     * @param filePath file to read from
     * @param startOffset starting file offset (should be block-aligned)
     * @param blockCount number of blocks to read
     * @return array of loaded memory segments (length equals blockCount)
     */
    T[] load(Path filePath, long startOffset, long blockCount) throws Exception;

    /**
     *  Loads a single block.
     */
    default T load(BlockCacheKey key) throws Exception {
        T[] result = load(key.filePath(), key.offset(), 1);  // Load 1 block
        if (result.length == 0 || result[0] == null) {
            throw new IOException("Failed to load block for key: " + key);
        }
        return result[0];
    }
}

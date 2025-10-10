/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_loader;

import java.io.IOException;
import java.nio.file.Path;

import org.opensearch.index.store.block_cache.BlockCacheKey;

/**
 * Interface for loading blocks of data from files into memory.
 * 
 * <p>BlockLoader implementations are responsible for efficiently reading file data
 * and providing it in a format suitable for caching. This typically involves
 * managing memory allocation from pools and handling various I/O operations.
 *
 * @param <T> the type of loaded block data (e.g., RefCountedMemorySegment)
 * @opensearch.internal
 */
public interface BlockLoader<T> {

    /**
     * Thrown when the memory segment pool is under pressure and cannot allocate segments.
     */
    class PoolPressureException extends IOException {
        /**
         * Constructs a new PoolPressureException with the specified detail message.
         *
         * @param message the detail message explaining the pool pressure condition
         */
        public PoolPressureException(String message) {
            super(message);
        }

        /**
         * Constructs a new PoolPressureException with the specified detail message and cause.
         *
         * @param message the detail message explaining the pool pressure condition
         * @param cause the underlying cause of the exception
         */
        public PoolPressureException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Thrown when unable to acquire a memory segment from the pool within timeout.
     */
    class PoolAcquireFailedException extends IOException {
        /**
         * Constructs a new PoolAcquireFailedException with the specified detail message.
         *
         * @param message the detail message explaining the acquisition failure
         */
        public PoolAcquireFailedException(String message) {
            super(message);
        }

        /**
         * Constructs a new PoolAcquireFailedException with the specified detail message and cause.
         *
         * @param message the detail message explaining the acquisition failure
         * @param cause the underlying cause of the exception
         */
        public PoolAcquireFailedException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Thrown when block loading fails due to I/O or other loading errors.
     */
    class BlockLoadFailedException extends IOException {
        /**
         * Constructs a new BlockLoadFailedException with the specified detail message.
         *
         * @param message the detail message explaining the loading failure
         */
        public BlockLoadFailedException(String message) {
            super(message);
        }

        /**
         * Constructs a new BlockLoadFailedException with the specified detail message and cause.
         *
         * @param message the detail message explaining the loading failure
         * @param cause the underlying cause of the exception
         */
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
     * @throws Exception if loading fails due to I/O errors, pool pressure, or other issues
     */
    T[] load(Path filePath, long startOffset, long blockCount) throws Exception;

    /**
     * Loads a single block using the provided cache key.
     *
     * @param key the cache key identifying the block to load
     * @return the loaded block data
     * @throws Exception if loading fails due to I/O errors, pool pressure, or other issues
     */
    default T load(BlockCacheKey key) throws Exception {
        T[] result = load(key.filePath(), key.offset(), 1);  // Load 1 block
        if (result.length == 0 || result[0] == null) {
            throw new IOException("Failed to load block for key: " + key);
        }
        return result[0];
    }
}

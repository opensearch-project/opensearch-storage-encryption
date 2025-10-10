/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.nio.file.Path;

/**
 * Key interface for identifying cached blocks in a {@link BlockCache}.
 * 
 * <p>A cache key consists of a file path and an offset within that file,
 * uniquely identifying a specific block of data that can be cached.
 *
 * @opensearch.internal
 */
public interface BlockCacheKey {
    /**
     * Returns the file path associated with this cache key.
     *
     * @return the file path for the cached block
     */
    Path filePath();

    /**
     * Returns the byte offset within the file for this cache key.
     *
     * @return the offset in bytes from the beginning of the file
     */
    long offset();
}

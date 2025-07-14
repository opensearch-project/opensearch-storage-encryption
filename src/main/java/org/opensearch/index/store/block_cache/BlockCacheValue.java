/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.lang.foreign.MemorySegment;

@SuppressWarnings("preview")
public interface BlockCacheValue extends AutoCloseable {

    /**
     * The memory segment containing cached block data.
     */
    MemorySegment segment();

    /**
     * Returns the size (in bytes) of the valid data within the segment.
     */
    int length();

    @Override
    void close();
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.util.Optional;

@FunctionalInterface
public interface BlockLoader<T> {
    Optional<BlockCacheValue<T>> load(BlockCacheKey key, int blockSize) throws Exception;
}

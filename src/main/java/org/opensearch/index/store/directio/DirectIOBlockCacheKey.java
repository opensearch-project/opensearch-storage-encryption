/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import java.nio.file.Path;

import org.opensearch.index.store.block_cache.BlockCacheKey;

public record DirectIOBlockCacheKey(Path filePath, long fileOffset) implements BlockCacheKey {

    @Override
    public long offset() {
        return fileOffset;
    }
}

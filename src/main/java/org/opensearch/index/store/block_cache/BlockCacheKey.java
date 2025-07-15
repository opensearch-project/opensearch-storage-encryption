/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.nio.file.Path;

public interface BlockCacheKey {
    Path filePath();

    long alignedOffset();
}

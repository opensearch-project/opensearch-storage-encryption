/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.lang.foreign.MemorySegment;

@FunctionalInterface
@SuppressWarnings("preview")
public interface SegmentReleaser {
    void release(MemorySegment segment);
}

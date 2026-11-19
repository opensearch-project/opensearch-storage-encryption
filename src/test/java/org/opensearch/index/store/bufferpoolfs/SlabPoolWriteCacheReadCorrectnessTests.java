/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * Same as {@link SlabPoolReadCorrectnessTests}, with the write cache on: blocks written by the files are
 * cached in slab slots during the write, so reads start from those slots before eviction recycles them.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class SlabPoolWriteCacheReadCorrectnessTests extends SlabPoolReadCorrectnessTests {

    @Override
    protected boolean writeCacheEnabled() {
        return true;
    }
}

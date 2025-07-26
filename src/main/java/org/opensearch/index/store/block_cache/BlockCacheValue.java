/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

public interface BlockCacheValue<T> extends AutoCloseable {

    T block();

    T borrowBlock();

    int length();

    @Override
    void close();
}

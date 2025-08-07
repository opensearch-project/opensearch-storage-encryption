/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead;

import java.nio.file.Path;

/**
 * A single block prefetch request for async readahead.
 */
public interface ReadaheadRequest {

    /**
     * @return the file path to read from
     */
    Path path();

    /**
     * @return the aligned file offset to start reading
     */
    long offset();

    /**
     * @return the length in bytes to prefetch (aligned to block size)
     */
    int length();
}

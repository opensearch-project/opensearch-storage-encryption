/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import java.nio.file.Path;

import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;

// NoOpReadaheadManager.java
public enum NoOpReadaheadManager implements ReadaheadManager {
    INSTANCE;

    @Override
    public ReadaheadContext register(Path path, long fileLength) {
        return NoOpReadaheadContext.INSTANCE;
    }

    @Override
    public void cancel(ReadaheadContext context) {}

    @Override
    public void cancel(Path path) {}

    @Override
    public void close() {}
}

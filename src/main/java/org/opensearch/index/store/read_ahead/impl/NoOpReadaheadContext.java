/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadPolicy;

// NoOpReadaheadContext.java  
public enum NoOpReadaheadContext implements ReadaheadContext {
    INSTANCE;

    @Override
    public void onAccess(long blockOffset, boolean wasHit) {}

    @Override
    public void triggerReadahead(long fileOffset) {}

    @Override
    public void reset() {}

    @Override
    public void cancel() {}

    @Override
    public boolean isReadAheadEnabled() {
        return false;
    }

    @Override
    public ReadaheadPolicy policy() {
        return null;
    }

    @Override
    public void close() {}
}

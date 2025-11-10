/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

public enum SegmentType {
    PRIMARY("primary"),
    SECONDARY("secondary");

    private final String value;

    SegmentType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}

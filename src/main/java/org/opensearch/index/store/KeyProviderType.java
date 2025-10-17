/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

/**
 * Enum representing key provider types for encryption.
 * 
 * @opensearch.internal
 */
public enum KeyProviderType {
    /**
     * Dummy key provider for testing purposes.
     */
    DUMMY("dummy");

    private final String value;

    KeyProviderType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}

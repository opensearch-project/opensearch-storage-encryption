/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.metrics;

public enum ErrorType {
    INTERNAL_ERROR("internal_error"),
    KMS_KEY_ERROR("kms_key_error");

    private final String value;

    ErrorType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}

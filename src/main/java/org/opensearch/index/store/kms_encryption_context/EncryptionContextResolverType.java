/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.kms_encryption_context;

/**
 * Enum representing the type of encryption context resolver to use.
 *
 * @opensearch.internal
 */
public enum EncryptionContextResolverType {
    /**
     * Amazon-specific resolver that extracts encryption context from repository settings.
     */
    AMAZON,

    /**
     * No-op resolver that returns empty encryption context.
     */
    NONE;

    /**
     * Parse a string value to EncryptionContextResolverType.
     *
     * @param value the string value (case-insensitive)
     * @return the corresponding enum value
     * @throws IllegalArgumentException if the value is not recognized
     */
    public static EncryptionContextResolverType fromString(String value) {
        if (value == null) {
            throw new IllegalArgumentException("EncryptionContextResolverType cannot be null");
        }
        try {
            return valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown EncryptionContextResolverType: " + value + ". Valid values are: AMAZON, NONE");
        }
    }
}

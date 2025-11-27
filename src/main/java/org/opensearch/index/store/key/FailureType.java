/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.key;

/**
 * Classifies the type of key loading/refresh failure.
 * Used to determine whether to apply index blocks or allow operations to continue.
 * 
 * @opensearch.internal
 */
public enum FailureType {
    /**
     * Transient errors that should not trigger blocks.
     * Examples: throttling, rate limits, temporary network issues.
     * System continues using cached keys during these failures.
     */
    TRANSIENT,

    /**
     * Critical errors that should trigger blocks to protect data.
     * Examples: disabled keys, revoked keys, access denied, key not found.
     * System cannot safely operate without valid keys.
     */
    CRITICAL
}

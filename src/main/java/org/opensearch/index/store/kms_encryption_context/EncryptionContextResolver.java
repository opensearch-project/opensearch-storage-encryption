/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.kms_encryption_context;

/**
 * Interface for resolving default encryption context from cluster metadata.
 * This abstraction allows for different implementations based on deployment environment
 * (e.g., AWS-specific vs. community OpenSearch).
 *
 * @opensearch.internal
 */
public interface EncryptionContextResolver {

    /**
     * Resolve the default encryption context from cluster metadata.
     * This method is called when an encrypted index is created to determine
     * if there's a cluster-wide default encryption context that should be used.
     *
     * @return the default encryption context, or empty string if none is found
     */
    String resolveDefaultEncryptionContext();

    /**
     * Get the name/type of this resolver for logging purposes.
     *
     * @return the resolver name
     */
    String getName();
}

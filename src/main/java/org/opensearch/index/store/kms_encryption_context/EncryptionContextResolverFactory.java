/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.kms_encryption_context;

import org.opensearch.cluster.service.ClusterService;

/**
 * Factory for creating EncryptionContextResolver instances based on configuration.
 * This allows different deployments (Amazon vs. community OpenSearch) to use different resolvers.
 *
 * <p>In community OpenSearch, the default is {@link EncryptionContextResolverType#NONE}.
 * Amazon will patch this to default to {@link EncryptionContextResolverType#AMAZON}.
 *
 * @opensearch.internal
 */
public class EncryptionContextResolverFactory {
    /**
     * Default resolver type for community OpenSearch.
     */
    private static final EncryptionContextResolverType DEFAULT_RESOLVER_TYPE = EncryptionContextResolverType.NONE;

    /**
     * Create an EncryptionContextResolver based on the configured default type.
     *
     * @param clusterService the cluster service (may be null for no-op resolver)
     * @return the appropriate EncryptionContextResolver instance
     */
    public static EncryptionContextResolver create(ClusterService clusterService) {
        return create(DEFAULT_RESOLVER_TYPE, clusterService);
    }

    /**
     * Create an EncryptionContextResolver for the specified type.
     * This method is primarily for testing purposes.
     *
     * @param type the resolver type
     * @param clusterService the cluster service (may be null for no-op resolver)
     * @return the appropriate EncryptionContextResolver instance
     */
    static EncryptionContextResolver create(EncryptionContextResolverType type, ClusterService clusterService) {
        switch (type) {
            case AMAZON -> {
                return new AmazonEncryptionContextResolver(clusterService);
            }
            case NONE -> {
                return new NoOpEncryptionContextResolver();
            }
            default -> throw new IllegalStateException("Unknown EncryptionContextResolverType: " + type);
        }
    }
}

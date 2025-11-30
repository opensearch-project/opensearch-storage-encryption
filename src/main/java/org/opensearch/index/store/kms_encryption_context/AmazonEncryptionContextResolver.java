/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.kms_encryption_context;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;

/**
 * AWS-specific implementation of EncryptionContextResolver.
 * This resolver extracts encryption context from Amazon S3 repository settings
 * using the "amazon_es_kms_enc_ctx" setting.
 *
 * <p>This allows Amazon OpenSearch Service to maintain their specific encryption
 * context handling logic separately from the core plugin code.
 *
 * @opensearch.internal
 */
public class AmazonEncryptionContextResolver implements EncryptionContextResolver {

    private static final Logger LOGGER = LogManager.getLogger(AmazonEncryptionContextResolver.class);
    private static final String AMAZON_ENC_CTX_SETTING = "amazon_es_kms_enc_ctx";

    private final ClusterService clusterService;

    /**
     * Creates a new Amazon-specific encryption context resolver.
     *
     * @param clusterService the cluster service for accessing repository metadata
     */
    public AmazonEncryptionContextResolver(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String resolveDefaultEncryptionContext() {
        if (clusterService == null) {
            LOGGER.debug("ClusterService not available, cannot fetch default encryption context");
            return "";
        }

        try {
            Metadata metadata = clusterService.state().metadata();
            RepositoriesMetadata repositoriesMetadata = metadata.custom(RepositoriesMetadata.TYPE);

            if (repositoriesMetadata == null) {
                LOGGER.debug("No repositories metadata found in cluster state");
                return "";
            }

            // Search for the first repository with amazon_es_kms_enc_ctx setting
            for (RepositoryMetadata repo : repositoriesMetadata.repositories()) {
                String encCtx = repo.settings().get(AMAZON_ENC_CTX_SETTING);
                if (encCtx != null && !encCtx.isEmpty()) {
                    LOGGER.info("Found Amazon default encryption context from repository '{}': {}", repo.name(), encCtx);
                    return encCtx;
                }
            }

            LOGGER.debug("No repository with {} found", AMAZON_ENC_CTX_SETTING);
            return "";
        } catch (Exception e) {
            LOGGER.warn("Failed to fetch Amazon encryption context from cluster metadata", e);
            return "";
        }
    }

    @Override
    public String getName() {
        return "AmazonEncryptionContextResolver";
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.kms_encryption_context;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * No-op implementation of EncryptionContextResolver.
 * This resolver always returns an empty encryption context and is used
 * when no default encryption context resolution is needed (e.g., community OpenSearch).
 *
 * @opensearch.internal
 */
public class NoOpEncryptionContextResolver implements EncryptionContextResolver {

    private static final Logger LOGGER = LogManager.getLogger(NoOpEncryptionContextResolver.class);

    @Override
    public String resolveDefaultEncryptionContext() {
        LOGGER.debug("No-op resolver: returning empty encryption context");
        return "";
    }

    @Override
    public String getName() {
        return "NoOpEncryptionContextResolver";
    }
}

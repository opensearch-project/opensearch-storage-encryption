/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.translog;

import java.io.IOException;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import org.opensearch.index.store.key.KeyResolver;

/**
 * A factory for creating crypto-enabled translogs that use unified key management.
 * This factory creates translog instances that use the same KeyResolver as index files
 * for consistent key management across all encrypted components.
 */
public class CryptoTranslogFactory implements TranslogFactory {

    private final KeyResolver keyResolver;

    /**
     * Constructor for CryptoTranslogFactory.
     *
     * @param keyResolver the unified key resolver (same as used by index files)
     */
    public CryptoTranslogFactory(KeyResolver keyResolver) {
        this.keyResolver = keyResolver;
    }

    @Override
    public Translog newTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        BooleanSupplier startedPrimarySupplier
    ) throws IOException {

        CryptoTranslog cryptoTranslog = new CryptoTranslog(
            config,
            translogUUID,
            deletionPolicy,
            globalCheckpointSupplier,
            primaryTermSupplier,
            persistedSequenceNumberConsumer,
            keyResolver
        );

        return cryptoTranslog;
    }

    @Override
    public Translog newTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        BooleanSupplier startedPrimarySupplier,
        TranslogOperationHelper translogOperationHelper
    ) throws IOException {

        CryptoTranslog cryptoTranslog = new CryptoTranslog(
            config,
            translogUUID,
            deletionPolicy,
            globalCheckpointSupplier,
            primaryTermSupplier,
            persistedSequenceNumberConsumer,
            translogOperationHelper,
            keyResolver
        );

        return cryptoTranslog;
    }
}

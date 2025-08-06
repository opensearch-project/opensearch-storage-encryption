/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.translog;

import java.io.IOException;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.iv.KeyIvResolver;

/**
 * A Translog implementation that provides AES-GCM encryption capabilities.
 * 
 * This class extends LocalTranslog and injects a CryptoChannelFactory during construction
 * to ensure that all translog file operations go through encrypted channels.
 *
 * Translog files (.tlog) are encrypted using AES-GCM with 8KB authenticated chunks.
 * Each chunk includes a 16-byte authentication tag for data integrity verification.
 * Checkpoint files (.ckp) remain unencrypted for performance and compatibility.
 *
 * Uses unified KeyIvResolver (same as index files) for consistent
 * key management across all encrypted components.
 *
 * @opensearch.internal
 */
public class CryptoTranslog extends LocalTranslog {

    private static final Logger logger = LogManager.getLogger(CryptoTranslog.class);

    private final KeyIvResolver keyIvResolver;
    private final String translogUUID;

    /**
     * Creates a new CryptoTranslog with AES-GCM encryption.
     *
     * @param config the translog configuration
     * @param translogUUID the translog UUID
     * @param deletionPolicy the deletion policy
     * @param globalCheckpointSupplier the global checkpoint supplier
     * @param primaryTermSupplier the primary term supplier
     * @param persistedSequenceNumberConsumer the persisted sequence number consumer
     * @param keyIvResolver the key and IV resolver for encryption (unified with index files)
     * @throws IOException if translog creation fails
     */
    public CryptoTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        KeyIvResolver keyIvResolver
    )
        throws IOException {

        super(
            config,
            translogUUID,
            deletionPolicy,
            globalCheckpointSupplier,
            primaryTermSupplier,
            persistedSequenceNumberConsumer,
            TranslogOperationHelper.DEFAULT,
            createCryptoChannelFactory(keyIvResolver, translogUUID)
        );

        // Strict validation after super() - never allow null components
        if (keyIvResolver == null || translogUUID == null) {
            throw new IllegalArgumentException(
                "Cannot create CryptoTranslog without keyIvResolver and translogUUID. "
                    + "Required for translog encryption. keyIvResolver="
                    + keyIvResolver
                    + ", translogUUID="
                    + translogUUID
            );
        }

        // Initialize instance fields
        this.keyIvResolver = keyIvResolver;
        this.translogUUID = translogUUID;

        logger.info("CryptoTranslog initialized for translog: {}", translogUUID);
    }

    /**
     * Creates a new CryptoTranslog with AES-GCM encryption and custom TranslogOperationHelper.
     *
     * @param config the translog configuration
     * @param translogUUID the translog UUID
     * @param deletionPolicy the deletion policy
     * @param globalCheckpointSupplier the global checkpoint supplier
     * @param primaryTermSupplier the primary term supplier
     * @param persistedSequenceNumberConsumer the persisted sequence number consumer
     * @param translogOperationHelper the translog operation helper
     * @param keyIvResolver the key and IV resolver for encryption (unified with index files)
     * @throws IOException if translog creation fails
     */
    public CryptoTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        TranslogOperationHelper translogOperationHelper,
        KeyIvResolver keyIvResolver
    )
        throws IOException {

        super(
            config,
            translogUUID,
            deletionPolicy,
            globalCheckpointSupplier,
            primaryTermSupplier,
            persistedSequenceNumberConsumer,
            translogOperationHelper,
            createCryptoChannelFactory(keyIvResolver, translogUUID)
        );

        // Strict validation after super() - never allow null components
        if (keyIvResolver == null || translogUUID == null) {
            throw new IllegalArgumentException(
                "Cannot create CryptoTranslog without keyIvResolver and translogUUID. "
                    + "Required for translog encryption. keyIvResolver="
                    + keyIvResolver
                    + ", translogUUID="
                    + translogUUID
            );
        }

        // Initialize instance fields
        this.keyIvResolver = keyIvResolver;
        this.translogUUID = translogUUID;

        logger.info("CryptoTranslog initialized for translog: {}", translogUUID);
    }

    /**
     * Helper method to create CryptoChannelFactory for constructor use.
     * This is needed because Java requires super() to be the first statement.
     * Returns ChannelFactory interface type to match LocalTranslog constructor signature.
     */
    private static ChannelFactory createCryptoChannelFactory(KeyIvResolver keyIvResolver, String translogUUID) throws IOException {
        try {
            CryptoChannelFactory channelFactory = new CryptoChannelFactory(keyIvResolver, translogUUID);
            logger.debug("CryptoChannelFactory initialized for translog: {}", translogUUID);
            return channelFactory;
        } catch (Exception e) {
            logger.error("Failed to initialize CryptoChannelFactory: {}", e.getMessage(), e);
            throw new IOException(
                "Failed to initialize crypto channel factory for translog encryption. " + "Cannot proceed without encryption!",
                e
            );
        }
    }

    /**
     * Ensure proper cleanup of crypto resources.
     */
    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            logger.debug("CryptoTranslog closed - encrypted translog files");
        }
    }
}

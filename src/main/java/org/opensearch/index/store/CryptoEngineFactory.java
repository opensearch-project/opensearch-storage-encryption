/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.store.iv.DefaultKeyIvResolver;
import org.opensearch.index.store.iv.KeyIvResolver;
import org.opensearch.index.translog.CryptoTranslogFactory;

/**
 * A factory that creates engines with crypto-enabled translogs for cryptofs indices.
 */
public class CryptoEngineFactory implements EngineFactory {

    private static final Logger logger = LogManager.getLogger(CryptoEngineFactory.class);

    /**
     * Default constructor.
     */
    public CryptoEngineFactory() {}

    /**
     * {@inheritDoc}
     */
    @Override
    public Engine newReadWriteEngine(EngineConfig config) {

        try {
            // Create a separate KeyIvResolver for translog encryption
            KeyIvResolver keyIvResolver = createTranslogKeyIvResolver(config);

            // Create the crypto translog factory using the same KeyIvResolver as the directory
            CryptoTranslogFactory cryptoTranslogFactory = new CryptoTranslogFactory(keyIvResolver);

            // Create new engine config by copying all fields from existing config
            // but replace the translog factory with our crypto version
            EngineConfig cryptoConfig = config
                .toBuilder()
                .translogFactory(cryptoTranslogFactory)  // <- Replace with our crypto factory
                .build();

            // Return the default engine with crypto-enabled translog
            return new InternalEngine(cryptoConfig);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create crypto engine", e);
        }
    }

    /**
     * Create a separate KeyIvResolver for translog encryption.
     */
    private KeyIvResolver createTranslogKeyIvResolver(EngineConfig config) throws IOException {
        // Create a separate key resolver for translog files

        // Use the translog location for key storage
        Path translogPath = config.getTranslogConfig().getTranslogPath();
        Directory keyDirectory = FSDirectory.open(translogPath);

        // Create crypto directory factory to get the key provider
        CryptoDirectoryFactory directoryFactory = new CryptoDirectoryFactory();

        // Create a dedicated key resolver for translog
        return new DefaultKeyIvResolver(
            keyDirectory,
            config.getIndexSettings().getValue(CryptoDirectoryFactory.INDEX_CRYPTO_PROVIDER_SETTING),
            directoryFactory.getKeyProvider(config.getIndexSettings())
        );
    }

}

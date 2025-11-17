/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import java.io.IOException;
import java.nio.file.Path;
import java.security.Provider;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.key.ShardKeyResolverRegistry;
import org.opensearch.index.translog.CryptoTranslogFactory;

/**
 * A factory that creates engines with crypto-enabled translogs for cryptofs indices.
 */
public class CryptoEngineFactory implements EngineFactory {
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
            // Create a separate KeyResolver for translog encryption
            KeyResolver keyResolver = createTranslogKeyResolver(config);

            // Create the crypto translog factory using the same KeyResolver as the directory
            CryptoTranslogFactory cryptoTranslogFactory = new CryptoTranslogFactory(keyResolver);

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
     * Create a separate KeyResolver for translog encryption.
     */
    private KeyResolver createTranslogKeyResolver(EngineConfig config) throws IOException {
        // Create a separate key resolver for translog files

        // Use the translog location for key storage
        // Use index-level keys for translog encryption - same as directory encryption
        Path translogPath = config.getTranslogConfig().getTranslogPath();
        Path indexDirectory = translogPath.getParent().getParent(); // Go up two levels: translog -> shard -> index

        // Get the same settings that CryptoDirectoryFactory uses
        Provider provider = config.getIndexSettings().getValue(CryptoDirectoryFactory.INDEX_CRYPTO_PROVIDER_SETTING);
        MasterKeyProvider keyProvider = getKeyProvider(config);

        // Create directory for index-level keys (same as CryptoDirectoryFactory)
        Directory indexKeyDirectory = FSDirectory.open(indexDirectory);

        // Use shared resolver registry to get the SAME resolver instance as CryptoDirectoryFactory
        String indexUuid = config.getIndexSettings().getIndex().getUUID();
        int shardId = config.getShardId().getId();
        return ShardKeyResolverRegistry.getOrCreateResolver(indexUuid, indexKeyDirectory, provider, keyProvider, shardId);
    }

    /**
     * Get the MasterKeyProvider - copied from CryptoDirectoryFactory logic
     */
    private MasterKeyProvider getKeyProvider(EngineConfig config) {
        // Reuse the same logic as CryptoDirectoryFactory
        return new CryptoDirectoryFactory().getKeyProvider(config.getIndexSettings());
    }

}

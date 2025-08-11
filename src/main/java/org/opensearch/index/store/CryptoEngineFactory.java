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
import org.opensearch.index.codec.CodecService;
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
            EngineConfig cryptoConfig = new EngineConfig.Builder()
                .shardId(config.getShardId())
                .threadPool(config.getThreadPool())
                .indexSettings(config.getIndexSettings())
                .warmer(config.getWarmer())
                .store(config.getStore())
                .mergePolicy(config.getMergePolicy())
                .analyzer(config.getAnalyzer())
                .similarity(config.getSimilarity())
                .codecService(getCodecService(config))
                .eventListener(config.getEventListener())
                .queryCache(config.getQueryCache())
                .queryCachingPolicy(config.getQueryCachingPolicy())
                .translogConfig(config.getTranslogConfig())
                .translogDeletionPolicyFactory(config.getCustomTranslogDeletionPolicyFactory())
                .flushMergesAfter(config.getFlushMergesAfter())
                .externalRefreshListener(config.getExternalRefreshListener())
                .internalRefreshListener(config.getInternalRefreshListener())
                .indexSort(config.getIndexSort())
                .circuitBreakerService(config.getCircuitBreakerService())
                .globalCheckpointSupplier(config.getGlobalCheckpointSupplier())
                .retentionLeasesSupplier(config.retentionLeasesSupplier())
                .primaryTermSupplier(config.getPrimaryTermSupplier())
                .tombstoneDocSupplier(config.getTombstoneDocSupplier())
                .readOnlyReplica(config.isReadOnlyReplica())
                .startedPrimarySupplier(config.getStartedPrimarySupplier())
                .translogFactory(cryptoTranslogFactory)  // <- Replace with our crypto factory
                .leafSorter(config.getLeafSorter())
                .documentMapperForTypeSupplier(config.getDocumentMapperForTypeSupplier())
                .indexReaderWarmer(config.getIndexReaderWarmer())
                .clusterApplierService(config.getClusterApplierService())
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

    /**
     * Helper method to create a CodecService from existing EngineConfig.
     * Since EngineConfig doesn't expose CodecService directly, we create a new one
     * using the same IndexSettings.
     */
    private CodecService getCodecService(EngineConfig config) {
        // Create a CodecService using the same IndexSettings as the original config
        // We pass null for MapperService and use a simple logger since we're just
        // preserving the existing codec behavior
        return new CodecService(null, config.getIndexSettings(), logger);
    }
}

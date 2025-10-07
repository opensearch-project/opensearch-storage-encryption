/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.store.iv.IndexKeyResolverRegistry;
import org.opensearch.index.store.iv.NodeLevelKeyCache;
import org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

/**
 * A plugin that enables index level encryption and decryption.
 */
public class CryptoDirectoryPlugin extends Plugin implements IndexStorePlugin, EnginePlugin {

    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectoryPlugin.class);

    private volatile ThreadContext threadContext;

    /**
     * Default constructor.
     */
    public CryptoDirectoryPlugin() {
        super();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays
            .asList(
                CryptoDirectoryFactory.INDEX_KMS_TYPE_SETTING,
                CryptoDirectoryFactory.INDEX_CRYPTO_PROVIDER_SETTING,
                CryptoDirectoryFactory.NODE_DATA_KEY_TTL_SECONDS_SETTING
            );
    }

    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        return Collections.singletonMap("cryptofs", new CryptoDirectoryFactory(() -> threadContext));
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if ("cryptofs".equals(indexSettings.getValue(IndexModule.INDEX_STORE_TYPE_SETTING))) {
            return Optional.of(new CryptoEngineFactory());
        }
        return Optional.empty();
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver expressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        CryptoDirectoryFactory.initializeSharedPool();
        NodeLevelKeyCache.initialize(environment.settings());

        this.threadContext = threadPool.getThreadContext();
        LOGGER.info("Initialized CryptoDirectoryPlugin with thread context: {}", threadContext);

        return Collections.emptyList();
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        Settings indexSettings = indexModule.getSettings();
        String storeType = indexSettings.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey());
        if ("cryptofs".equals(storeType)) {
            indexModule.addIndexEventListener(new IndexEventListener() {
                @Override
                public void beforeIndexRemoved(IndexService indexService, IndexRemovalReason reason) {
                    String indexUuid = indexService.index().getUUID();
                    IndexKeyResolverRegistry.removeResolver(indexUuid);
                }
            });
        }
    }
}

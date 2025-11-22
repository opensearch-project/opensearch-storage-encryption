/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.key.MasterKeyHealthMonitor;
import org.opensearch.index.store.key.NodeLevelKeyCache;
import org.opensearch.index.store.key.ShardKeyResolverRegistry;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.pool.PoolSizeCalculator;
import org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.TelemetryAwarePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

/**
 * A plugin that enables index level encryption and decryption.
 */
public class CryptoDirectoryPlugin extends Plugin implements IndexStorePlugin, EnginePlugin, TelemetryAwarePlugin {
    private NodeEnvironment nodeEnvironment;

    /**
     * The default constructor.
     */
    public CryptoDirectoryPlugin() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Setting<?>> getSettings() {
        return Arrays
            .asList(
                CryptoDirectoryFactory.INDEX_KEY_PROVIDER_SETTING,
                CryptoDirectoryFactory.INDEX_CRYPTO_PROVIDER_SETTING,
                CryptoDirectoryFactory.INDEX_KMS_ARN_SETTING,
                CryptoDirectoryFactory.INDEX_KMS_ENC_CTX_SETTING,
                CryptoDirectoryFactory.NODE_KEY_REFRESH_INTERVAL_SETTING,
                CryptoDirectoryFactory.NODE_KEY_EXPIRY_INTERVAL_SETTING,
                PoolSizeCalculator.NODE_POOL_SIZE_PERCENTAGE_SETTING,
                PoolSizeCalculator.NODE_CACHE_TO_POOL_RATIO_SETTING,
                PoolSizeCalculator.NODE_WARMUP_PERCENTAGE_SETTING
            );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        return Collections.singletonMap("cryptofs", new CryptoDirectoryFactory());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        // Only provide our custom engine factory for cryptofs indices
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
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        MetricsRegistry metricsRegistry
    ) {
        this.nodeEnvironment = nodeEnvironment;

        // Initialize health monitor first (creates monitor)
        MasterKeyHealthMonitor.initialize(environment.settings(), client, clusterService);

        // Initialize cache second (depends on health monitor reference)
        NodeLevelKeyCache.initialize(environment.settings(), MasterKeyHealthMonitor.getInstance());

        // Start health monitoring now that everything is initialized
        MasterKeyHealthMonitor.start();

        // Pool resources are lazily initialized on first cryptofs shard creation
        // This prevents allocation on dedicated master nodes which never create shards
        CryptoDirectoryFactory.setNodeSettings(environment.settings());
        CryptoMetricsService.initialize(metricsRegistry);

        return Collections.emptyList();
    }

    @Override
    public void close() {
        MasterKeyHealthMonitor.shutdown();
        // Close shared pool resources if they were initialized
        // the shared pool is initilized only when atleast one index
        // level enc enabled index is created.
        CryptoDirectoryFactory.closeSharedPool();
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        Settings indexSettings = indexModule.getSettings();
        String storeType = indexSettings.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey());

        if ("cryptofs".equals(storeType)) {
            indexModule.addIndexEventListener(new IndexEventListener() {
                /*
                 * Cache invalidation for closed shards is handled automatically
                 * by CryptoDirectIODirectory.close() when the directory is closed.
                 */
                @Override
                public void afterIndexRemoved(Index index, IndexSettings idxSettings, IndexRemovalReason reason) {
                    if (reason != IndexRemovalReason.DELETED) {
                        return;
                    }

                    BlockCache<?> cache = CryptoDirectoryFactory.getSharedBlockCache();
                    if (cache != null && nodeEnvironment != null) {
                        for (Path indexPath : nodeEnvironment.indexPaths(index)) {
                            cache.invalidateByPathPrefix(indexPath);
                        }
                    }

                    /*
                    * The resolvers should be removed only when the index is actually deleted (DELETED reason).
                    * We should NOT remove resolvers when shards are relocated (NO_LONGER_ASSIGNED) or during
                    * node restarts, as other nodes may still need the resolver for their shards.
                    * 
                    * This prevents race conditions during:
                    * - Shard relocation between nodes
                    * - Node restarts with replica recovery
                    * - Cluster topology changes
                    * */
                    int nShards = idxSettings.getNumberOfShards();
                    for (int i = 0; i < nShards; i++) {
                        ShardKeyResolverRegistry.removeResolver(index.getUUID(), i, index.getName());
                        NodeLevelKeyCache.getInstance().evict(index.getUUID(), i, index.getName());
                    }
                }
            });
        }
    }
}

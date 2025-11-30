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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.store.action.GetIndexCountForKeyAction;
import org.opensearch.index.store.action.TransportGetIndexCountForKeyAction;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.key.MasterKeyHealthMonitor;
import org.opensearch.index.store.key.NodeLevelKeyCache;
import org.opensearch.index.store.key.ShardKeyResolverRegistry;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.pool.PoolSizeCalculator;
import org.opensearch.index.store.rest.RestGetIndexCountForKeyAction;
import org.opensearch.index.store.rest.RestRegisterCryptoAction;
import org.opensearch.index.store.rest.RestUnregisterCryptoAction;
import org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.TelemetryAwarePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

/**
 * A plugin that enables index level encryption and decryption.
 */
public class CryptoDirectoryPlugin extends Plugin implements IndexStorePlugin, EnginePlugin, TelemetryAwarePlugin, ActionPlugin {
    private static final Logger log = LogManager.getLogger(CryptoDirectoryPlugin.class);

    /**
     * Setting key for enabling the crypto plugin.
     */
    public static final String CRYPTO_PLUGIN_ENABLED = "plugins.crypto.enabled";

    /**
     * Setting for controlling whether the crypto plugin is enabled.
     */
    public static final Setting<Boolean> CRYPTO_PLUGIN_ENABLED_SETTING = Setting
        .boolSetting(CRYPTO_PLUGIN_ENABLED, false, Setting.Property.NodeScope, Setting.Property.Filtered, Setting.Property.Final);

    private NodeEnvironment nodeEnvironment;
    private final boolean enabled;

    /**
     * Constructor with settings.
     * @param settings OpenSearch node settings
     */
    public CryptoDirectoryPlugin(Settings settings) {
        super();
        this.enabled = settings.getAsBoolean(CRYPTO_PLUGIN_ENABLED, false);

        if (enabled) {
            log.info("OpenSearch Crypto Directory Plugin is enabled and ready for encryption operations");
        } else {
            log
                .warn(
                    "OpenSearch Crypto Directory Plugin installed but disabled. "
                        + "No encryption/decryption will be performed. "
                        + "To enable encryption, set '{}' to true in opensearch.yml",
                    CRYPTO_PLUGIN_ENABLED
                );
        }
    }

    /**
     * Check if the plugin is disabled.
     * @return true if the plugin is disabled, false otherwise
     */
    public boolean isDisabled() {
        return !enabled;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = Arrays
            .asList(
                CRYPTO_PLUGIN_ENABLED_SETTING,
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
        return settings;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        if (isDisabled()) {
            log.debug("Crypto Directory Plugin is disabled. No directory factories will be registered.");
            return Collections.emptyMap();
        }
        log.debug("Crypto Directory Plugin is enabled. Registering cryptofs directory factory.");
        return Collections.singletonMap(CryptoDirectoryFactory.STORE_TYPE, new CryptoDirectoryFactory());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (isDisabled()) {
            return Optional.empty();
        }

        // Only provide our custom engine factory for cryptofs indices
        if (CryptoDirectoryFactory.STORE_TYPE.equals(indexSettings.getValue(IndexModule.INDEX_STORE_TYPE_SETTING))) {
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
        if (isDisabled()) {
            log.debug("Crypto Directory Plugin is disabled. Skipping component initialization.");
            return Collections.emptyList();
        }

        this.nodeEnvironment = nodeEnvironment;

        // Set cluster service for accessing cluster metadata (e.g., repository settings)
        CryptoDirectoryFactory.setClusterService(clusterService);

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
        if (isDisabled()) {
            log.debug("Crypto Directory Plugin is disabled. No cleanup needed.");
            return;
        }

        MasterKeyHealthMonitor.shutdown();
        // Close shared pool resources if they were initialized
        // the shared pool is initialized only when at least one index
        // level enc enabled index is created.
        CryptoDirectoryFactory.closeSharedPool();
    }

    @Override
    public List<ActionHandler<?, ?>> getActions() {
        return Arrays.asList(new ActionHandler<>(GetIndexCountForKeyAction.INSTANCE, TransportGetIndexCountForKeyAction.class));
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return Arrays.asList(new RestRegisterCryptoAction(), new RestUnregisterCryptoAction(), new RestGetIndexCountForKeyAction());
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (isDisabled()) {
            log
                .debug(
                    "Crypto Directory Plugin is disabled. Skipping index module initialization for index: {}",
                    indexModule.getIndex().getName()
                );
            return;
        }

        Settings indexSettings = indexModule.getSettings();
        String storeType = indexSettings.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey());

        if (CryptoDirectoryFactory.STORE_TYPE.equals(storeType)) {
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

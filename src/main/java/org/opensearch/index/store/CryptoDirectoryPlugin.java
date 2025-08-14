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
import java.util.function.Supplier;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
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
import org.opensearch.index.store.metrics.CryptoMetrics;

/**
 * A plugin that enables index level encryption and decryption.
 */
public class CryptoDirectoryPlugin extends Plugin implements IndexStorePlugin, TelemetryAwarePlugin {

    /**
     * The default constructor.
     */
    public CryptoDirectoryPlugin() {
        super();
        CryptoMetrics.getInstance();
    }

    /**
     * {@inheritDoc}
     */
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
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        MetricsRegistry metricsRegistry
    ) {
        CryptoMetrics.getInstance(tracer, metricsRegistry);
        return Collections.emptyList();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(CryptoDirectoryFactory.INDEX_KMS_TYPE_SETTING, CryptoDirectoryFactory.INDEX_CRYPTO_PROVIDER_SETTING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        return java.util.Collections.singletonMap("cryptofs", new CryptoDirectoryFactory());
    }
}
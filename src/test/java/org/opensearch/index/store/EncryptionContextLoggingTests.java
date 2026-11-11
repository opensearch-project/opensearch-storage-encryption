/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.store.kms_encryption_context.AmazonEncryptionContextResolver;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;

/**
 * KMS encryption context values can carry tenant or account identifiers, so they must not reach
 * the logs at any level.
 */
public class EncryptionContextLoggingTests extends OpenSearchTestCase {

    private static final String DEFAULT_CONTEXT_VALUE = "tenant-7f3a91";
    private static final String INDEX_CONTEXT_VALUE = "project-c40e22";

    public void testDefaultContextIsNotLogged() throws Exception {
        assertContextNotLogged(Settings.EMPTY);
    }

    public void testMergedContextIsNotLogged() throws Exception {
        assertContextNotLogged(
            Settings.builder().put("index.store.crypto.kms.encryption_context", "project=" + INDEX_CONTEXT_VALUE).build()
        );
    }

    private void assertContextNotLogged(Settings indexSettings) throws Exception {
        // The community build uses the no-op resolver; the Amazon resolver is the one that reads a default context
        CryptoDirectoryFactory
            .setEncryptionContextResolver(
                new AmazonEncryptionContextResolver(clusterServiceWithDefaultContext("tenant=" + DEFAULT_CONTEXT_VALUE))
            );
        Logger[] loggers = {
            LogManager.getLogger(CryptoDirectoryFactory.class),
            LogManager.getLogger(AmazonEncryptionContextResolver.class) };
        Level[] previous = new Level[loggers.length];
        for (int i = 0; i < loggers.length; i++) {
            previous[i] = loggers[i].getLevel();
            Loggers.setLevel(loggers[i], Level.TRACE);
        }
        CapturingExpectation captured = new CapturingExpectation();
        try (MockLogAppender appender = MockLogAppender.createForLoggers(loggers)) {
            appender.addExpectation(captured);
            try {
                new CryptoDirectoryFactory().getKeyProvider(indexSettings("kms", indexSettings));
            } catch (RuntimeException e) {
                // No KMS key provider is registered in unit tests; only the logging before the lookup matters
            }
        } finally {
            CryptoDirectoryFactory.setEncryptionContextResolver(null);
            for (int i = 0; i < loggers.length; i++) {
                Loggers.setLevel(loggers[i], previous[i]);
            }
        }
        assertFalse("expected the resolver and factory to log at debug", captured.messages.isEmpty());
        for (String message : captured.messages) {
            assertFalse(message, message.contains(DEFAULT_CONTEXT_VALUE));
            assertFalse(message, message.contains(INDEX_CONTEXT_VALUE));
        }
    }

    private static final class CapturingExpectation implements MockLogAppender.LoggingExpectation {
        final List<String> messages = new ArrayList<>();

        @Override
        public void match(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }

        @Override
        public void assertMatched() {}
    }

    private static ClusterService clusterServiceWithDefaultContext(String context) {
        Settings repoSettings = Settings.builder().put("type", "s3").put("amazon_es_kms_enc_ctx", context).build();
        RepositoriesMetadata repositories = new RepositoriesMetadata(
            Collections.singletonList(new RepositoryMetadata("snapshots", "s3", repoSettings))
        );
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metadata()).thenReturn(metadata);
        when(metadata.custom(RepositoriesMetadata.TYPE)).thenReturn(repositories);
        return clusterService;
    }

    private static IndexSettings indexSettings(String keyProvider, Settings extra) {
        Settings settings = Settings
            .builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.store.type", "cryptofs")
            .put("index.store.crypto.key_provider", keyProvider)
            .put(extra)
            .build();
        return IndexSettingsModule
            .newIndexSettings(
                IndexMetadata.builder("logged-index").settings(settings).build(),
                CryptoDirectoryFactory.INDEX_KEY_PROVIDER_SETTING,
                CryptoDirectoryFactory.INDEX_KMS_ENC_CTX_SETTING
            );
    }
}

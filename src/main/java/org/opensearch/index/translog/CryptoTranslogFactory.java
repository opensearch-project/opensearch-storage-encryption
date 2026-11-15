/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.translog;

import java.io.IOException;
import java.security.Provider;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

/**
 * A factory for creating crypto-enabled translogs that use unified key management.
 * This factory creates translog instances that use the same KeyResolver as index files
 * for consistent key management across all encrypted components.
 *
 * Supports both local-only and remote store translogs:
 * - When remote store is disabled: creates CryptoTranslog (local only)
 * - When remote store is enabled: creates CryptoRemoteFsTranslog (with remote sync)
 */
public class CryptoTranslogFactory implements TranslogFactory {

    private final KeyResolver keyResolver;
    private final Provider provider;
    private final Repository repository;
    private final ThreadPool threadPool;
    private final RemoteTranslogTransferTracker remoteTranslogTransferTracker;
    private final RemoteStoreSettings remoteStoreSettings;

    /**
     * Constructor for CryptoTranslogFactory with local-only support.
     *
     * @param keyResolver the unified key resolver (same as used by index files)
     * @param provider the JCE provider for translog cipher and digest
     */
    public CryptoTranslogFactory(KeyResolver keyResolver, Provider provider) {
        this.keyResolver = keyResolver;
        this.provider = Objects.requireNonNull(provider, "provider");
        this.repository = null;
        this.threadPool = null;
        this.remoteTranslogTransferTracker = null;
        this.remoteStoreSettings = null;
    }

    /**
     * Constructor for CryptoTranslogFactory with remote store support.
     *
     * @param keyResolver the unified key resolver (same as used by index files)
     * @param provider the JCE provider for translog cipher and digest
     * @param repositoriesServiceSupplier supplier for the repositories service
     * @param threadPool the thread pool
     * @param repositoryName the name of the remote repository
     * @param remoteTranslogTransferTracker the remote translog transfer tracker
     * @param remoteStoreSettings the remote store settings
     */
    public CryptoTranslogFactory(
        KeyResolver keyResolver,
        Provider provider,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        ThreadPool threadPool,
        String repositoryName,
        RemoteTranslogTransferTracker remoteTranslogTransferTracker,
        RemoteStoreSettings remoteStoreSettings
    ) {
        this.keyResolver = keyResolver;
        this.provider = Objects.requireNonNull(provider, "provider");
        this.threadPool = threadPool;
        this.remoteTranslogTransferTracker = remoteTranslogTransferTracker;
        this.remoteStoreSettings = remoteStoreSettings;

        Repository repository;
        try {
            repository = repositoriesServiceSupplier.get().repository(repositoryName);
        } catch (RepositoryMissingException ex) {
            throw new IllegalArgumentException("Repository should be created before creating index with remote_store enabled setting", ex);
        }
        this.repository = repository;
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
        assert config.getIndexSettings().isDerivedSourceEnabled() == false; // For derived source supported index, primary method must be
                                                                            // used
        return this
            .newTranslog(
                config,
                translogUUID,
                deletionPolicy,
                globalCheckpointSupplier,
                primaryTermSupplier,
                persistedSequenceNumberConsumer,
                startedPrimarySupplier,
                TranslogOperationHelper.DEFAULT
            );
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

        // Check if remote translog is enabled
        boolean isRemoteTranslogEnabled = config.getIndexSettings().isRemoteTranslogStoreEnabled();

        if (isRemoteTranslogEnabled && repository != null) {
            // Create remote translog with encryption
            assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
            BlobStoreRepository blobStoreRepository = ((BlobStoreRepository) repository);

            // TODO: Add support for CryptoRemoteFsTimestampAwareTranslog when RemoteStoreSettings.isPinnedTimestampsEnabled()
            // For now, we use CryptoRemoteFsTranslog for both cases
            return new CryptoRemoteFsTranslog(
                config,
                translogUUID,
                deletionPolicy,
                globalCheckpointSupplier,
                primaryTermSupplier,
                persistedSequenceNumberConsumer,
                blobStoreRepository,
                threadPool,
                startedPrimarySupplier,
                remoteTranslogTransferTracker,
                remoteStoreSettings,
                translogOperationHelper,
                keyResolver,
                provider
            );
        } else {
            // Create local translog with encryption
            return new CryptoTranslog(
                config,
                translogUUID,
                deletionPolicy,
                globalCheckpointSupplier,
                primaryTermSupplier,
                persistedSequenceNumberConsumer,
                translogOperationHelper,
                keyResolver,
                provider
            );
        }
    }

    public Repository getRepository() {
        return repository;
    }
}

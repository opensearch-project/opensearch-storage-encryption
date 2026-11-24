/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.translog;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.TRANSLOG;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.DATA;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;

import java.io.IOException;
import java.lang.reflect.Field;
import java.security.Provider;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.index.translog.transfer.FileTransferTracker;
import org.opensearch.index.translog.transfer.TranslogTransferManager;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

/**
 * A RemoteFsTranslog implementation that provides AES-GCM encryption capabilities
 * with decrypt-before-upload for remote store.
 *
 * @opensearch.internal
 */
public class CryptoRemoteFsTranslog extends RemoteFsTranslog {

    private static final Logger logger = LogManager.getLogger(CryptoRemoteFsTranslog.class);

    private final KeyResolver keyResolver;
    private final String translogUUID;
    private final CryptoChannelFactory cryptoFactory;

    public CryptoRemoteFsTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        BlobStoreRepository blobStoreRepository,
        ThreadPool threadPool,
        BooleanSupplier startedPrimarySupplier,
        RemoteTranslogTransferTracker remoteTranslogTransferTracker,
        RemoteStoreSettings remoteStoreSettings,
        TranslogOperationHelper translogOperationHelper,
        KeyResolver keyResolver,
        Provider provider
    )
        throws IOException {
        super(
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
            createCryptoChannelFactory(keyResolver, translogUUID, provider),
            true // isServerSideEncryptionEnabled
        );

        this.keyResolver = keyResolver;
        this.translogUUID = translogUUID;
        this.cryptoFactory = (CryptoChannelFactory) this.channelFactory;

        try {
            TranslogTransferManager decryptingManager = createDecryptingTranslogTransferManager(
                blobStoreRepository,
                threadPool,
                config.getShardId(),
                fileTransferTracker,
                remoteTranslogTransferTracker,
                config.getIndexSettings().getRemoteStorePathStrategy(),
                remoteStoreSettings,
                config.getIndexSettings().isTranslogMetadataEnabled(),
                keyResolver,
                translogUUID,
                cryptoFactory
            );

            // Use reflection to replace the final field
            Field transferManagerField = RemoteFsTranslog.class.getDeclaredField("translogTransferManager");
            transferManagerField.setAccessible(true);
            transferManagerField.set(this, decryptingManager);
        } catch (Exception e) {
            logger.error("Failed to replace TranslogTransferManager with decrypting version", e);
            throw new IOException("Failed to initialize decrypt-before-upload capability", e);
        }
    }

    /**
     * Creates a DecryptingTranslogTransferManager to replace parent's manager.
     */
    private static TranslogTransferManager createDecryptingTranslogTransferManager(
        BlobStoreRepository blobStoreRepository,
        ThreadPool threadPool,
        ShardId shardId,
        FileTransferTracker fileTransferTracker,
        RemoteTranslogTransferTracker tracker,
        RemoteStorePathStrategy pathStrategy,
        RemoteStoreSettings remoteStoreSettings,
        boolean isTranslogMetadataEnabled,
        KeyResolver keyResolver,
        String translogUUID,
        CryptoChannelFactory cryptoFactory
    ) {
        String indexUUID = shardId.getIndex().getUUID();
        String shardIdStr = String.valueOf(shardId.id());

        RemoteStorePathStrategy.ShardDataPathInput dataPathInput = RemoteStorePathStrategy.ShardDataPathInput
            .builder()
            .basePath(blobStoreRepository.basePath())
            .indexUUID(indexUUID)
            .shardId(shardIdStr)
            .dataCategory(TRANSLOG)
            .dataType(DATA)
            .fixedPrefix(remoteStoreSettings.getTranslogPathFixedPrefix())
            .build();
        BlobPath dataPath = pathStrategy.generatePath(dataPathInput);

        RemoteStorePathStrategy.ShardDataPathInput mdPathInput = RemoteStorePathStrategy.ShardDataPathInput
            .builder()
            .basePath(blobStoreRepository.basePath())
            .indexUUID(indexUUID)
            .shardId(shardIdStr)
            .dataCategory(TRANSLOG)
            .dataType(METADATA)
            .fixedPrefix(remoteStoreSettings.getTranslogPathFixedPrefix())
            .build();
        BlobPath mdPath = pathStrategy.generatePath(mdPathInput);

        BlobStoreTransferService transferService = new BlobStoreTransferService(
            blobStoreRepository.blobStore(true), // SSE-KMS enabled
            threadPool
        );

        return new DecryptingTranslogTransferManager(
            shardId,
            transferService,
            dataPath,
            mdPath,
            fileTransferTracker,
            tracker,
            remoteStoreSettings,
            isTranslogMetadataEnabled,
            keyResolver,
            translogUUID,
            cryptoFactory
        );
    }

    /**
     * Helper method to create CryptoChannelFactory for constructor use.
     */
    private static CryptoChannelFactory createCryptoChannelFactory(KeyResolver keyResolver, String translogUUID, Provider provider)
        throws IOException {
        try {
            return new CryptoChannelFactory(keyResolver, translogUUID, provider);
        } catch (Exception e) {
            throw new IOException(
                "Failed to initialize crypto channel factory for translog encryption. Cannot proceed without encryption!",
                e
            );
        }
    }
}

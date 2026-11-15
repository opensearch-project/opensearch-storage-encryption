/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.translog;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.translog.transfer.TranslogTransferManager;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

public class CryptoRemoteFsTranslogTests extends OpenSearchTestCase {

    private static final Provider SUN_JCE = Security.getProvider("SunJCE");

    private Path tempDir;
    private KeyResolver keyResolver;
    private String testIndexUuid;
    private ShardId testShardId;

    // Mocks
    private BlobStoreRepository mockBlobStoreRepository;
    private ThreadPool mockThreadPool;
    private TranslogConfig mockConfig;
    private TranslogDeletionPolicy mockDeletionPolicy;
    private RemoteTranslogTransferTracker mockRemoteTranslogTransferTracker;
    private RemoteStoreSettings mockRemoteStoreSettings;
    private BooleanSupplier mockStartedPrimarySupplier;
    private LongSupplier mockGlobalCheckpointSupplier;
    private LongSupplier mockPrimaryTermSupplier;
    private LongConsumer mockPersistedSequenceNumberConsumer;
    private TranslogOperationHelper mockTranslogOperationHelper;

    @Override
    @SuppressForbidden(reason = "Creating temp directory for test purposes")
    public void setUp() throws Exception {
        super.setUp();
        tempDir = Files.createTempDirectory("crypto-remote-fs-translog-test");

        // Setup test data
        testIndexUuid = "test-index-uuid-" + System.currentTimeMillis();
        testShardId = new ShardId(new Index("test-index", testIndexUuid), 0);

        keyResolver = mock(KeyResolver.class);
        javax.crypto.spec.SecretKeySpec mockKey = new javax.crypto.spec.SecretKeySpec(new byte[32], "AES");
        when(keyResolver.getDataKey()).thenReturn(mockKey); // 256-bit AES key

        setupCommonMocks();
    }

    private void setupCommonMocks() throws Exception {
        // BlobStoreRepository mock
        mockBlobStoreRepository = mock(BlobStoreRepository.class);
        BlobStore mockBlobStore = mock(BlobStore.class);
        BlobContainer mockBlobContainer = mock(BlobContainer.class);
        when(mockBlobStoreRepository.blobStore(anyBoolean())).thenReturn(mockBlobStore);
        when(mockBlobStoreRepository.basePath()).thenReturn(BlobPath.cleanPath());
        when(mockBlobStore.blobContainer(any(BlobPath.class))).thenReturn(mockBlobContainer);

        doAnswer(invocation -> {
            org.opensearch.core.action.ActionListener<java.util.List<org.opensearch.common.blobstore.BlobMetadata>> listener = invocation
                .getArgument(3);
            listener.onResponse(java.util.Collections.emptyList());
            return null;
        }).when(mockBlobContainer).listBlobsByPrefixInSortedOrder(any(), anyInt(), any(), any());

        mockThreadPool = mock(ThreadPool.class);
        java.util.concurrent.ExecutorService mockExecutor = mock(java.util.concurrent.ExecutorService.class);
        when(mockThreadPool.executor(any(String.class))).thenReturn(mockExecutor);
        doAnswer(invocation -> {
            Runnable task = invocation.getArgument(0);
            task.run(); // Execute synchronously in test
            return null;
        }).when(mockExecutor).execute(any(Runnable.class));

        ByteSizeValue bufferSize = new ByteSizeValue(8, ByteSizeUnit.KB);
        Settings indexSettings = Settings
            .builder()
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(testShardId.getIndex(), indexSettings);
        mockConfig = new TranslogConfig(
            testShardId,
            tempDir,
            idxSettings,
            BigArrays.NON_RECYCLING_INSTANCE,
            bufferSize,
            "test-node",
            false
        );

        mockDeletionPolicy = mock(TranslogDeletionPolicy.class);
        mockRemoteTranslogTransferTracker = mock(RemoteTranslogTransferTracker.class);
        mockRemoteStoreSettings = mock(RemoteStoreSettings.class);
        when(mockRemoteStoreSettings.getTranslogPathFixedPrefix()).thenReturn("");
        when(mockRemoteStoreSettings.getClusterRemoteTranslogTransferTimeout()).thenReturn(TimeValue.timeValueSeconds(30));

        mockStartedPrimarySupplier = () -> true;
        mockGlobalCheckpointSupplier = () -> 0L;
        mockPrimaryTermSupplier = () -> 1L;
        mockPersistedSequenceNumberConsumer = seqNo -> {};
        mockTranslogOperationHelper = mock(TranslogOperationHelper.class);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testConstructorSuccessfulInitialization() throws Exception {
        String translogUUID = Translog.createEmptyTranslog(tempDir, 0L, testShardId, mockPrimaryTermSupplier.getAsLong());

        // Create the CryptoRemoteFsTranslog
        CryptoRemoteFsTranslog translog = new CryptoRemoteFsTranslog(
            mockConfig,
            translogUUID,
            mockDeletionPolicy,
            mockGlobalCheckpointSupplier,
            mockPrimaryTermSupplier,
            mockPersistedSequenceNumberConsumer,
            mockBlobStoreRepository,
            mockThreadPool,
            mockStartedPrimarySupplier,
            mockRemoteTranslogTransferTracker,
            mockRemoteStoreSettings,
            mockTranslogOperationHelper,
            keyResolver,
            SUN_JCE
        );

        assertNotNull("CryptoRemoteFsTranslog should be created successfully", translog);

        // Verify the CryptoChannelFactory is set
        Field channelFactoryField = Translog.class.getDeclaredField("channelFactory");
        channelFactoryField.setAccessible(true);
        ChannelFactory channelFactory = (ChannelFactory) channelFactoryField.get(translog);
        assertNotNull("ChannelFactory should not be null", channelFactory);
        assertTrue("ChannelFactory should be instance of CryptoChannelFactory", channelFactory instanceof CryptoChannelFactory);

        // Verify the translogTransferManager has been replaced with DecryptingTranslogTransferManager
        Field transferManagerField = RemoteFsTranslog.class.getDeclaredField("translogTransferManager");
        transferManagerField.setAccessible(true);
        TranslogTransferManager transferManager = (TranslogTransferManager) transferManagerField.get(translog);

        assertNotNull("TranslogTransferManager should not be null", transferManager);
        assertTrue(
            "TranslogTransferManager should be instance of DecryptingTranslogTransferManager",
            transferManager instanceof DecryptingTranslogTransferManager
        );

        // Cleanup
        translog.close();
    }

    public void testConstructorFailsWithNullKeyResolver() throws Exception {
        // Create empty translog
        String translogUUID = Translog.createEmptyTranslog(tempDir, 0L, testShardId, mockPrimaryTermSupplier.getAsLong());

        // Null keyResolver causes NullPointerException when parent tries to open translog files
        Exception exception = expectThrows(Exception.class, () -> {
            new CryptoRemoteFsTranslog(
                mockConfig,
                translogUUID,
                mockDeletionPolicy,
                mockGlobalCheckpointSupplier,
                mockPrimaryTermSupplier,
                mockPersistedSequenceNumberConsumer,
                mockBlobStoreRepository,
                mockThreadPool,
                mockStartedPrimarySupplier,
                mockRemoteTranslogTransferTracker,
                mockRemoteStoreSettings,
                mockTranslogOperationHelper,
                null,  // Null keyResolver
                SUN_JCE
            );
        });

        assertNotNull("Exception should be thrown for null keyResolver", exception);
        assertTrue(
            "Exception should be NullPointerException or contain NPE in cause chain",
            exception instanceof NullPointerException || (exception.getCause() != null && containsNullPointerException(exception))
        );
    }

    private boolean containsNullPointerException(Throwable throwable) {
        Throwable cause = throwable;
        while (cause != null) {
            if (cause instanceof NullPointerException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    public void testConstructorFailsWhenCryptoFactoryCreationFails() {
        String translogUUID = null;

        IOException exception = expectThrows(IOException.class, () -> {
            new CryptoRemoteFsTranslog(
                mockConfig,
                translogUUID,
                mockDeletionPolicy,
                mockGlobalCheckpointSupplier,
                mockPrimaryTermSupplier,
                mockPersistedSequenceNumberConsumer,
                mockBlobStoreRepository,
                mockThreadPool,
                mockStartedPrimarySupplier,
                mockRemoteTranslogTransferTracker,
                mockRemoteStoreSettings,
                mockTranslogOperationHelper,
                keyResolver,
                SUN_JCE
            );
        });

        assertNotNull("Exception should be thrown when crypto factory creation fails", exception);
        assertTrue(
            "Exception message should mention crypto channel factory initialization failure",
            exception.getMessage().contains("Failed to initialize crypto channel factory")
        );
        assertTrue(
            "Exception message should mention cannot proceed without encryption",
            exception.getMessage().contains("Cannot proceed without encryption")
        );
    }

    public void testTranslogTransferManagerReplacedSuccessfully() throws Exception {
        // Create empty translog
        String translogUUID = Translog.createEmptyTranslog(tempDir, 0L, testShardId, mockPrimaryTermSupplier.getAsLong());

        // Create the CryptoRemoteFsTranslog
        CryptoRemoteFsTranslog translog = new CryptoRemoteFsTranslog(
            mockConfig,
            translogUUID,
            mockDeletionPolicy,
            mockGlobalCheckpointSupplier,
            mockPrimaryTermSupplier,
            mockPersistedSequenceNumberConsumer,
            mockBlobStoreRepository,
            mockThreadPool,
            mockStartedPrimarySupplier,
            mockRemoteTranslogTransferTracker,
            mockRemoteStoreSettings,
            mockTranslogOperationHelper,
            keyResolver,
            SUN_JCE
        );

        // Access the translogTransferManager field
        Field transferManagerField = RemoteFsTranslog.class.getDeclaredField("translogTransferManager");
        transferManagerField.setAccessible(true);
        TranslogTransferManager transferManager = (TranslogTransferManager) transferManagerField.get(translog);

        // Verify it's the DecryptingTranslogTransferManager
        assertNotNull("TranslogTransferManager should not be null", transferManager);
        assertTrue(
            "TranslogTransferManager must be DecryptingTranslogTransferManager",
            transferManager instanceof DecryptingTranslogTransferManager
        );

        // Verify the DecryptingTranslogTransferManager has the correct components
        DecryptingTranslogTransferManager decryptingManager = (DecryptingTranslogTransferManager) transferManager;

        // Access private fields to verify configuration
        Field keyResolverField = DecryptingTranslogTransferManager.class.getDeclaredField("keyResolver");
        keyResolverField.setAccessible(true);
        KeyResolver managerKeyResolver = (KeyResolver) keyResolverField.get(decryptingManager);

        assertNotNull("DecryptingTranslogTransferManager should have keyResolver", managerKeyResolver);
        assertEquals("KeyResolver should be the same instance", keyResolver, managerKeyResolver);

        Field translogUUIDField = DecryptingTranslogTransferManager.class.getDeclaredField("translogUUID");
        translogUUIDField.setAccessible(true);
        String managerTranslogUUID = (String) translogUUIDField.get(decryptingManager);

        assertNotNull("DecryptingTranslogTransferManager should have translogUUID", managerTranslogUUID);
        assertEquals("TranslogUUID should match", translogUUID, managerTranslogUUID);

        Field cryptoFactoryField = DecryptingTranslogTransferManager.class.getDeclaredField("cryptoFactory");
        cryptoFactoryField.setAccessible(true);
        CryptoChannelFactory managerCryptoFactory = (CryptoChannelFactory) cryptoFactoryField.get(decryptingManager);

        assertNotNull("DecryptingTranslogTransferManager should have cryptoFactory", managerCryptoFactory);

        // Cleanup
        translog.close();
    }
}

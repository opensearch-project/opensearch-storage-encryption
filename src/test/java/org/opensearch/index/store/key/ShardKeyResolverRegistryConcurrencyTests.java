/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.key;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.IndicesAdminClient;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

/**
 * Tests for concurrent shard creation to verify the race condition fix
 * in ShardKeyResolverRegistry.
 * 
 * This test ensures that when multiple shards of the same index are created
 * concurrently, only one thread initializes the shared index-level keyfile.
 */
@ThreadLeakFilters(filters = org.opensearch.index.store.CaffeineThreadLeakFilter.class)
public class ShardKeyResolverRegistryConcurrencyTests extends OpenSearchTestCase {

    private Path tempDir;
    private MasterKeyProvider mockKeyProvider;
    private Provider cryptoProvider;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        // Create temporary directory for test
        tempDir = createTempDir();

        // Set up mock key provider
        mockKeyProvider = mock(MasterKeyProvider.class);

        // Configure mock to return valid key pair and decrypted key
        byte[] plaintextKey = new byte[32];
        byte[] encryptedKey = new byte[48];
        when(mockKeyProvider.generateDataPair()).thenReturn(new DataKeyPair(plaintextKey, encryptedKey));
        when(mockKeyProvider.decryptKey(any())).thenReturn(plaintextKey);

        // Get crypto provider
        cryptoProvider = Security.getProvider("SunJCE");

        // Initialize CryptoMetricsService
        CryptoMetricsService.initialize(mock(MetricsRegistry.class));

        // Initialize NodeLevelKeyCache with mock Client and ClusterService
        MasterKeyHealthMonitor.reset();
        NodeLevelKeyCache.reset();
        Client mockClient = mock(Client.class);
        ClusterService mockClusterService = mock(ClusterService.class);

        // Setup mock Client chain for block operations
        AdminClient mockAdminClient = mock(AdminClient.class);
        IndicesAdminClient mockIndicesAdminClient = mock(IndicesAdminClient.class);
        @SuppressWarnings("unchecked")
        ActionFuture<AcknowledgedResponse> mockFuture = (ActionFuture<AcknowledgedResponse>) mock(ActionFuture.class);

        when(mockClient.admin()).thenReturn(mockAdminClient);
        when(mockAdminClient.indices()).thenReturn(mockIndicesAdminClient);
        when(mockIndicesAdminClient.updateSettings(any())).thenReturn(mockFuture);
        when(mockFuture.actionGet()).thenReturn(mock(AcknowledgedResponse.class));

        MasterKeyHealthMonitor.initialize(Settings.EMPTY, mockClient, mockClusterService);
        NodeLevelKeyCache.initialize(Settings.EMPTY, MasterKeyHealthMonitor.getInstance());

        // Clear registry before each test
        ShardKeyResolverRegistry.clearCache();
    }

    @After
    public void tearDown() throws Exception {
        // Clear registry after each test
        ShardKeyResolverRegistry.clearCache();
        MasterKeyHealthMonitor.reset();
        NodeLevelKeyCache.reset();
        super.tearDown();
    }

    /**
     * Test that concurrent creation of multiple shards from the same index
     * results in only ONE keyfile generation (proves no race condition).
     */
    public void testConcurrentShardCreationSingleKeyGeneration() throws Exception {
        String indexUuid = "test-index-uuid";
        String indexName = "test-index";
        int numShards = 10;

        // Create shared index directory
        Directory indexDirectory = FSDirectory.open(tempDir);

        // Create keyfile path for verification
        Path keyfilePath = tempDir.resolve("keyfile");

        // Create multiple shards concurrently
        ExecutorService executor = Executors.newFixedThreadPool(numShards);
        List<Future<KeyResolver>> futures = new ArrayList<>();

        // Use CountDownLatch to maximize concurrency
        CountDownLatch startLatch = new CountDownLatch(1);

        for (int shardId = 0; shardId < numShards; shardId++) {
            final int sid = shardId;
            futures.add(executor.submit(() -> {
                startLatch.await(); // Wait for all threads to be ready
                return ShardKeyResolverRegistry
                    .getOrCreateResolver(indexUuid, indexDirectory, cryptoProvider, mockKeyProvider, sid, indexName);
            }));
        }

        // Release all threads at once to maximize concurrency
        startLatch.countDown();

        // Wait for all to complete
        Set<KeyResolver> resolvers = new HashSet<>();
        for (Future<KeyResolver> future : futures) {
            resolvers.add(future.get(10, TimeUnit.SECONDS));
        }

        executor.shutdown();
        assertTrue("Executor should terminate", executor.awaitTermination(5, TimeUnit.SECONDS));

        // Verify results
        assertEquals("Should create resolver for each shard", numShards, resolvers.size());
        assertEquals("All resolvers should be cached", numShards, ShardKeyResolverRegistry.getCacheSize());
        assertTrue("Keyfile should exist", Files.exists(keyfilePath));

        // Verify generateDataPair was called exactly once (no race condition)
        verify(mockKeyProvider, times(1)).generateDataPair();

        // Verify all shards can read the key (no exception means success)
        for (KeyResolver resolver : resolvers) {
            assertNotNull("Resolver should provide data key", resolver.getDataKey());
        }

        indexDirectory.close();
    }

    /**
     * Test that resolver cleanup properly removes index locks when the last shard is removed.
     */
    public void testIndexLockCleanupOnResolverRemoval() throws Exception {
        String indexUuid = "test-index-cleanup";
        String indexName = "test-index-cleanup";
        int numShards = 5;

        Directory indexDirectory = FSDirectory.open(tempDir);

        // Create multiple shards
        List<KeyResolver> resolvers = new ArrayList<>();
        for (int shardId = 0; shardId < numShards; shardId++) {
            resolvers
                .add(
                    ShardKeyResolverRegistry
                        .getOrCreateResolver(indexUuid, indexDirectory, cryptoProvider, mockKeyProvider, shardId, indexName)
                );
        }

        assertEquals("All shards should be registered", numShards, ShardKeyResolverRegistry.getCacheSize());

        // Remove all shards one by one
        for (int shardId = 0; shardId < numShards; shardId++) {
            KeyResolver removed = ShardKeyResolverRegistry.removeResolver(indexUuid, shardId, indexName);
            assertNotNull("Should return removed resolver", removed);
        }

        assertEquals("All shards should be removed", 0, ShardKeyResolverRegistry.getCacheSize());

        // Verify only one key generation occurred
        verify(mockKeyProvider, times(1)).generateDataPair();

        indexDirectory.close();
    }

    /**
     * Stress test: Rapid creation and deletion cycles to verify no memory leaks
     * and consistent behavior under stress.
     */
    public void testStressRaceCondition() throws Exception {
        int numIterations = 50;
        int numShards = 5;

        for (int i = 0; i < numIterations; i++) {
            String indexUuid = "stress-test-" + i;
            String indexName = "stress-index-" + i;

            Path indexDir = createTempDir();
            Directory indexDirectory = FSDirectory.open(indexDir);

            // Reset mock for each iteration
            reset(mockKeyProvider);
            byte[] plaintextKey = new byte[32];
            byte[] encryptedKey = new byte[48];
            when(mockKeyProvider.generateDataPair()).thenReturn(new DataKeyPair(plaintextKey, encryptedKey));
            when(mockKeyProvider.decryptKey(any())).thenReturn(plaintextKey);

            // Concurrent shard creation
            ExecutorService executor = Executors.newFixedThreadPool(numShards);
            CountDownLatch latch = new CountDownLatch(1);

            List<Future<?>> futures = new ArrayList<>();
            for (int shardId = 0; shardId < numShards; shardId++) {
                final int sid = shardId;
                futures.add(executor.submit(() -> {
                    try {
                        latch.await();
                        ShardKeyResolverRegistry
                            .getOrCreateResolver(indexUuid, indexDirectory, cryptoProvider, mockKeyProvider, sid, indexName);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }

            latch.countDown();

            for (Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }

            // Verify only one key generation per index
            verify(mockKeyProvider, times(1)).generateDataPair();

            // Cleanup
            for (int shardId = 0; shardId < numShards; shardId++) {
                ShardKeyResolverRegistry.removeResolver(indexUuid, shardId, indexName);
            }

            executor.shutdown();
            assertTrue("Executor should terminate", executor.awaitTermination(2, TimeUnit.SECONDS));
            indexDirectory.close();
        }

        // Verify complete cleanup
        assertEquals("All resolvers should be cleaned up", 0, ShardKeyResolverRegistry.getCacheSize());
    }

    /**
     * Test that different indices can be created concurrently without interfering
     * with each other.
     */
    public void testConcurrentDifferentIndicesCreation() throws Exception {
        int numIndices = 5;
        int shardsPerIndex = 3;

        ExecutorService executor = Executors.newFixedThreadPool(numIndices * shardsPerIndex);
        List<Future<KeyResolver>> futures = new ArrayList<>();
        CountDownLatch startLatch = new CountDownLatch(1);

        // Create multiple indices concurrently, each with multiple shards
        for (int indexNum = 0; indexNum < numIndices; indexNum++) {
            String indexUuid = "index-" + indexNum;
            String indexName = "index-name-" + indexNum;
            Path indexDir = createTempDir();
            Directory indexDirectory = FSDirectory.open(indexDir);

            for (int shardId = 0; shardId < shardsPerIndex; shardId++) {
                final int sid = shardId;
                futures.add(executor.submit(() -> {
                    startLatch.await();
                    return ShardKeyResolverRegistry
                        .getOrCreateResolver(indexUuid, indexDirectory, cryptoProvider, mockKeyProvider, sid, indexName);
                }));
            }
        }

        startLatch.countDown();

        // Wait for all to complete
        for (Future<KeyResolver> future : futures) {
            assertNotNull("Should create resolver", future.get(10, TimeUnit.SECONDS));
        }

        executor.shutdown();
        assertTrue("Executor should terminate", executor.awaitTermination(5, TimeUnit.SECONDS));

        // Verify total resolvers created
        assertEquals("Should have resolver for each shard", numIndices * shardsPerIndex, ShardKeyResolverRegistry.getCacheSize());

        // Each index should have generated exactly one key
        verify(mockKeyProvider, times(numIndices)).generateDataPair();
    }

    /**
     * When the last shard of an index leaves a node, OpenSearch deletes the index directory and its keyfile,
     * but the resolver stays registered. If a shard of the index comes back, the registry must write the
     * keyfile back so the key can still be loaded.
     */
    public void testKeyFileRestoredWhenShardReturnsAfterIndexDirectoryDeleted() throws Exception {
        String indexUuid = "test-index-returns";
        String indexName = "test-index-returns";
        Path indexPath = tempDir.resolve(indexUuid);
        Files.createDirectories(indexPath);
        Path keyfilePath = indexPath.resolve("keyfile");

        Directory firstDirectory = FSDirectory.open(indexPath);
        KeyResolver first = ShardKeyResolverRegistry
            .getOrCreateResolver(indexUuid, firstDirectory, cryptoProvider, mockKeyProvider, 0, indexName);
        byte[] originalKeyfile = Files.readAllBytes(keyfilePath);

        // The shard moves away: the node deletes the index directory, the resolver stays registered,
        // and later the cached key expires.
        IOUtils.rm(indexPath);
        NodeLevelKeyCache.getInstance().clear();

        // A shard of the index is allocated to the node again.
        Files.createDirectories(indexPath);
        Directory secondDirectory = FSDirectory.open(indexPath);
        KeyResolver second = ShardKeyResolverRegistry
            .getOrCreateResolver(indexUuid, secondDirectory, cryptoProvider, mockKeyProvider, 0, indexName);

        assertSame("The registered resolver should be reused", first, second);
        assertTrue("The keyfile should be written back", Files.exists(keyfilePath));
        assertArrayEquals("The same encrypted data key should be written back", originalKeyfile, Files.readAllBytes(keyfilePath));
        assertNotNull("The key should load from the restored keyfile", second.getDataKey());
        verify(mockKeyProvider, times(1)).generateDataPair();

        firstDirectory.close();
        secondDirectory.close();
    }

    /**
     * A registered resolver must not recreate an index directory that no longer exists on the node.
     */
    public void testKeyFileNotRestoredWhenIndexDirectoryMissing() throws Exception {
        String indexUuid = "test-index-gone";
        String indexName = "test-index-gone";
        Path indexPath = tempDir.resolve(indexUuid);
        Files.createDirectories(indexPath);

        Directory directory = FSDirectory.open(indexPath);
        ShardKeyResolverRegistry.getOrCreateResolver(indexUuid, directory, cryptoProvider, mockKeyProvider, 0, indexName);
        IOUtils.rm(indexPath);

        ShardKeyResolverRegistry.getOrCreateResolver(indexUuid, directory, cryptoProvider, mockKeyProvider, 0, indexName);

        assertFalse("The index directory should not be recreated", Files.exists(indexPath));
        directory.close();
    }

    /**
     * With several data paths the returning shard can land on a different path than the one the resolver
     * read its keyfile from. The keyfile must be written to the new path and loaded from there.
     */
    public void testKeyFileRestoredOnDifferentDataPath() throws Exception {
        String indexUuid = "test-index-other-path";
        String indexName = "test-index-other-path";
        Path firstPath = tempDir.resolve("path0").resolve(indexUuid);
        Path secondPath = tempDir.resolve("path1").resolve(indexUuid);
        Files.createDirectories(firstPath);

        Directory firstDirectory = FSDirectory.open(firstPath);
        KeyResolver first = ShardKeyResolverRegistry
            .getOrCreateResolver(indexUuid, firstDirectory, cryptoProvider, mockKeyProvider, 0, indexName);
        byte[] originalKeyfile = Files.readAllBytes(firstPath.resolve("keyfile"));

        IOUtils.rm(firstPath);
        NodeLevelKeyCache.getInstance().clear();

        Files.createDirectories(secondPath);
        Directory secondDirectory = FSDirectory.open(secondPath);
        KeyResolver second = ShardKeyResolverRegistry
            .getOrCreateResolver(indexUuid, secondDirectory, cryptoProvider, mockKeyProvider, 0, indexName);

        assertSame(first, second);
        assertArrayEquals(originalKeyfile, Files.readAllBytes(secondPath.resolve("keyfile")));
        assertFalse("The old index directory should not be recreated", Files.exists(firstPath));
        assertNotNull(second.getDataKey());
        verify(mockKeyProvider, times(1)).generateDataPair();

        firstDirectory.close();
        secondDirectory.close();
    }
}

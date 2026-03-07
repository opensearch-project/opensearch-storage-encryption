/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.tests.mockfile.ExtrasFS;
import org.opensearch.common.Randomness;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.bufferpoolfs.BufferPoolDirectory;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.hybrid.HybridCryptoDirectory;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.niofs.CryptoNIOFSDirectory;
import org.opensearch.index.store.pool.MemorySegmentPool;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.index.store.read_ahead.impl.QueuingWorker;
import org.opensearch.telemetry.metrics.MetricsRegistry;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Shared factory for constructing crypto directory instances in tests.
 * Provides real (not mocked) encryption infrastructure with random keys.
 *
 * <p>Directories returned by this factory clean up their thread pools and
 * native memory pools on {@link java.io.Closeable#close()}.
 */
public final class CryptoTestDirectoryFactory {

    /** Name of the encryption key file created by {@code DefaultKeyResolver}. */
    public static final String KEY_FILE_NAME = "keyfile";

    private static final Provider PROVIDER = Security.getProvider("SunJCE");
    private static final AtomicBoolean METRICS_INITIALIZED = new AtomicBoolean(false);

    /**
     * NIO extensions matching OpenSearch's production default
     * ({@code IndexModule.INDEX_STORE_HYBRID_NIO_EXTENSIONS}).
     */
    private static final Set<String> NIO_EXTENSIONS = Set
        .of("si", "cfe", "fnm", "fdx", "fdt", "pos", "pay", "nvm", "dvm", "tvx", "tvd", "liv", "dii", "vem");

    private CryptoTestDirectoryFactory() {}

    /** Ensures CryptoMetricsService is initialized for tests. Safe to call multiple times. */
    public static void initMetrics() {
        if (METRICS_INITIALIZED.compareAndSet(false, true)) {
            CryptoMetricsService.initialize(mock(MetricsRegistry.class));
        }
    }

    /** Creates a random 256-bit AES KeyResolver using the test's random source. */
    public static KeyResolver createKeyResolver() {
        byte[] rawKey = new byte[32];
        Randomness.get().nextBytes(rawKey);
        KeyResolver resolver = mock(KeyResolver.class);
        when(resolver.getDataKey()).thenReturn(new SecretKeySpec(rawKey, "AES"));
        return resolver;
    }

    /**
     * Shared implementation of {@code testCreateTempOutput} for crypto directories.
     * Crypto directories list an extra "keyfile" that the base test doesn't expect,
     * so this override filters it out before asserting.
     *
     * @param dir       the directory under test (caller is responsible for closing)
     * @param iters     number of temp files to create
     * @param random    the test's {@link java.util.Random} instance
     * @param ioCtxSupplier supplier for IOContext (typically {@code () -> newIOContext(random())})
     */
    public static void assertTempOutputRoundTrip(Directory dir, int iters, java.util.function.Supplier<IOContext> ioCtxSupplier)
        throws IOException {
        List<String> names = new ArrayList<>();
        for (int iter = 0; iter < iters; iter++) {
            IndexOutput out = dir.createTempOutput("foo", "bar", ioCtxSupplier.get());
            names.add(out.getName());
            out.writeVInt(iter);
            out.close();
        }
        for (int iter = 0; iter < iters; iter++) {
            IndexInput in = dir.openInput(names.get(iter), ioCtxSupplier.get());
            assertEquals(iter, in.readVInt());
            in.close();
        }

        Set<String> files = Arrays
            .stream(dir.listAll())
            .filter(file -> !ExtrasFS.isExtra(file))
            .filter(file -> !file.equals(KEY_FILE_NAME))
            .collect(Collectors.toSet());

        assertEquals(new HashSet<>(names), files);
    }

    public static CryptoNIOFSDirectory createCryptoNIOFSDirectory(Path path, LockFactory lockFactory) throws IOException {
        initMetrics();
        return new CryptoNIOFSDirectory(lockFactory, path, PROVIDER, createKeyResolver(), new EncryptionMetadataCache());
    }

    public static BufferPoolDirectory createBufferPoolDirectory(Path path, LockFactory lockFactory) throws IOException {
        initMetrics();
        KeyResolver keyResolver = createKeyResolver();
        EncryptionMetadataCache metadataCache = new EncryptionMetadataCache();
        return buildBufferPoolDirectory(path, lockFactory, keyResolver, metadataCache);
    }

    /**
     * Creates a HybridCryptoDirectory with production-matching NIO extension routing.
     * Files with extensions in {@link #NIO_EXTENSIONS} route to NIO; others to BufferPool.
     * Note: extensionless file names always route to NIO (production behavior).
     */
    public static HybridCryptoDirectory createHybridCryptoDirectory(Path path, LockFactory lockFactory) throws IOException {
        initMetrics();
        KeyResolver keyResolver = createKeyResolver();
        EncryptionMetadataCache metadataCache = new EncryptionMetadataCache();
        BufferPoolDirectory bufferPoolDir = buildBufferPoolDirectory(path, lockFactory, keyResolver, metadataCache);
        return new HybridCryptoDirectory(lockFactory, bufferPoolDir, PROVIDER, keyResolver, metadataCache, NIO_EXTENSIONS);
    }

    /**
     * Creates a HybridCryptoDirectory that routes ALL files through BufferPool,
     * including extensionless files. Use this for Lucene contract tests which
     * create files without extensions (e.g., "foobar", "byte", "int") that would
     * otherwise always route to NIO due to {@code delegeteBufferPool("")} returning false.
     *
     * <p>The NIO path is already covered by {@code CryptoDirectoryTests} which directly
     * tests {@code CryptoNIOFSDirectory}. This method ensures the BufferPool encryption
     * path gets exercised through the HybridCryptoDirectory layer.
     */
    public static HybridCryptoDirectory createBufferPoolRoutedHybridDirectory(Path path, LockFactory lockFactory) throws IOException {
        initMetrics();
        KeyResolver keyResolver = createKeyResolver();
        EncryptionMetadataCache metadataCache = new EncryptionMetadataCache();
        BufferPoolDirectory bufferPoolDir = buildBufferPoolDirectory(path, lockFactory, keyResolver, metadataCache);
        return new BufferPoolRoutedHybridDirectory(lockFactory, bufferPoolDir, PROVIDER, keyResolver, metadataCache, NIO_EXTENSIONS);
    }

    /**
     * Builds a BufferPoolDirectory with full encryption infrastructure.
     * The returned directory shuts down its executor and closes the native memory pool on close().
     */
    private static BufferPoolDirectory buildBufferPoolDirectory(
        Path path,
        LockFactory lockFactory,
        KeyResolver keyResolver,
        EncryptionMetadataCache metadataCache
    ) throws IOException {
        MemorySegmentPool pool = new MemorySegmentPool(131072, 8192);
        CryptoDirectIOBlockLoader blockLoader = new CryptoDirectIOBlockLoader(pool, keyResolver, metadataCache);

        Cache<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> caffeineCache = Caffeine
            .newBuilder()
            .maximumSize(1000)
            .expireAfterAccess(Duration.ofMinutes(5))
            .recordStats()
            .build();

        CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> blockCache = new CaffeineBlockCache<>(
            caffeineCache,
            blockLoader,
            1000
        );

        ExecutorService executor = Executors.newFixedThreadPool(4);
        Worker worker = new QueuingWorker(100, executor);

        // Anonymous subclass that cleans up executor and native pool on close
        return new BufferPoolDirectory(path, lockFactory, PROVIDER, keyResolver, pool, blockCache, blockLoader, worker, metadataCache) {
            @Override
            public synchronized void close() throws IOException {
                super.close();
                executor.shutdownNow();
                pool.close();
            }
        };
    }

    /**
     * Test-only HybridCryptoDirectory that routes ALL files through BufferPool,
     * overriding the extension-based routing logic. This ensures Lucene contract tests
     * exercise the encrypted BufferPool I/O path rather than the NIO pass-through.
     */
    static class BufferPoolRoutedHybridDirectory extends HybridCryptoDirectory {
        private final BufferPoolDirectory bpDir;

        BufferPoolRoutedHybridDirectory(
            LockFactory lockFactory,
            BufferPoolDirectory delegate,
            Provider provider,
            KeyResolver keyResolver,
            EncryptionMetadataCache cache,
            Set<String> nioExtensions
        )
            throws IOException {
            super(lockFactory, delegate, provider, keyResolver, cache, nioExtensions);
            this.bpDir = delegate;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            ensureOpen();
            ensureCanRead(name);
            return bpDir.openInput(name, context);
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            ensureOpen();
            return bpDir.createOutput(name, context);
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
            return bpDir.createTempOutput(prefix, suffix, context);
        }

        @Override
        public void deleteFile(String name) throws IOException {
            bpDir.deleteFile(name);
        }
    }
}

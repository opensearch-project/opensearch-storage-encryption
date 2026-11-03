/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.opensearch.index.store.CryptoDirectoryFactory.DEFAULT_CRYPTO_PROVIDER;

import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.After;
import org.junit.Before;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.CryptoDirectoryFactory;
import org.opensearch.index.store.DummyKeyProvider;
import org.opensearch.index.store.block.RefCountedByteBuffer;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.cipher.EncryptionMetadataCacheRegistry;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.pool.PoolBuilder;
import org.opensearch.index.store.pool.SlabByteBufferPool;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * ByteBuffer mode with the slab pool, end to end: encrypted files much larger than the block cache are
 * read back from many threads through clones and slices, so blocks are evicted and slots reused all the
 * time. Every byte is a function of (file, offset), so a slot handed out again while a reader still
 * held it would show up as wrong data.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@SuppressWarnings("unchecked")
public class SlabPoolReadCorrectnessTests extends OpenSearchTestCase {

    private static final int FILES = 4;
    private static final int FILE_BYTES = 150 << 20;

    private BufferPoolDirectory directory;
    private PoolBuilder.PoolResources poolResources;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        CryptoDirectoryFactory
            .setNodeSettings(
                Settings
                    .builder()
                    .put("node.store.crypto.write_cache_enabled", false)
                    .put("node.store.crypto.byte_buffer_mode_enabled", true)
                    .put("node.store.crypto.byte_buffer_slab_pool_enabled", true)
                    .build()
            );
        Path path = createTempDir();
        Provider provider = Security.getProvider(DEFAULT_CRYPTO_PROVIDER);
        MasterKeyProvider keyProvider = DummyKeyProvider.create();
        // The minimum pool (256 MB): the files below are ~2.3x the pool, so the cache churns constantly.
        poolResources = PoolBuilder
            .build(
                Settings
                    .builder()
                    .put("plugins.crypto.enabled", true)
                    .put("node.store.crypto.pool_size_percentage", 0.0)
                    .put("node.store.crypto.warmup_percentage", 0.0)
                    .put("node.store.crypto.cache_to_pool_ratio", 0.8)
                    .build()
            );
        assertTrue(poolResources.getPool().getClass().getName(), poolResources.getPool() instanceof SlabByteBufferPool);
        Pool<RefCountedByteBuffer> pool = (Pool<RefCountedByteBuffer>) poolResources.getPool();

        String indexUuid = randomAlphaOfLength(10);
        String indexName = randomAlphaOfLength(10);
        KeyResolver keyResolver = new TestKeyResolver(indexUuid, indexName, FSDirectory.open(path), provider, keyProvider, 0);
        EncryptionMetadataCache metadataCache = EncryptionMetadataCacheRegistry.getOrCreateCache(indexUuid, 0, indexName);
        BlockLoader<RefCountedByteBuffer> loader = new CryptoDirectIOBlockLoader<>(
            pool,
            keyResolver,
            metadataCache,
            poolResources.getFileChannelCache(),
            true
        );
        CaffeineBlockCache<RefCountedByteBuffer, RefCountedByteBuffer> shared =
            (CaffeineBlockCache<RefCountedByteBuffer, RefCountedByteBuffer>) poolResources.getBlockCache();
        BlockCache<RefCountedByteBuffer> cache = new CaffeineBlockCache<>(shared.getCache(), loader, poolResources.getMaxCacheBlocks());
        directory = new BufferPoolDirectory(
            path,
            FSLockFactory.getDefault(),
            provider,
            keyResolver,
            pool,
            cache,
            poolResources.getSharedReadaheadWorker(),
            metadataCache,
            poolResources.getFileChannelCache(),
            true
        );
    }

    @After
    public void tearDown() throws Exception {
        CryptoDirectoryFactory.setNodeSettings(Settings.EMPTY);
        directory.close();
        poolResources.close();
        super.tearDown();
    }

    private static byte expected(int file, long offset) {
        return (byte) ((offset * 31 + file * 7) ^ (offset >>> 13));
    }

    public void testConcurrentReadsUnderChurnSeeOnlyTheirOwnData() throws Exception {
        byte[] chunk = new byte[1 << 20];
        for (int f = 0; f < FILES; f++) {
            try (IndexOutput out = directory.createOutput("f" + f, IOContext.DEFAULT)) {
                for (long base = 0; base < FILE_BYTES; base += chunk.length) {
                    for (int i = 0; i < chunk.length; i++) {
                        chunk[i] = expected(f, base + i);
                    }
                    out.writeBytes(chunk, chunk.length);
                }
            }
        }
        IndexInput[] mains = new IndexInput[FILES];
        for (int f = 0; f < FILES; f++) {
            mains[f] = directory.openInput("f" + f, IOContext.DEFAULT);
            assertEquals(FILE_BYTES, mains[f].length());
        }

        AtomicReference<String> failure = new AtomicReference<>();
        AtomicLong bytesChecked = new AtomicLong();
        long deadline = System.nanoTime() + 45_000_000_000L;
        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < 8; t++) {
            final long seed = random().nextLong();
            Thread th = new Thread(() -> {
                Random rnd = new Random(seed);
                byte[] buf = new byte[64 << 10];
                try {
                    while (System.nanoTime() < deadline && failure.get() == null) {
                        int f = rnd.nextInt(FILES);
                        IndexInput in;
                        long sliceStart;
                        synchronized (mains[f]) {
                            // Alternate clones and slices, like Lucene's per-query readers.
                            if (rnd.nextBoolean()) {
                                in = mains[f].clone();
                                sliceStart = 0;
                            } else {
                                sliceStart = rnd.nextInt(FILE_BYTES / 2);
                                in = mains[f].slice("s", sliceStart, FILE_BYTES - sliceStart);
                            }
                        }
                        for (int r = 0; r < 50; r++) {
                            long pos = (long) (rnd.nextDouble() * (in.length() - buf.length));
                            int len = 1 + rnd.nextInt(buf.length);
                            in.seek(pos);
                            if (rnd.nextInt(4) == 0) {
                                for (int i = 0; i < Math.min(len, 64); i++) {
                                    byte b = in.readByte();
                                    if (b != expected(f, sliceStart + pos + i)) {
                                        failure.compareAndSet(null, "file " + f + " offset " + (sliceStart + pos + i) + ": readByte mismatch");
                                    }
                                }
                                bytesChecked.addAndGet(Math.min(len, 64));
                            } else {
                                in.readBytes(buf, 0, len);
                                for (int i = 0; i < len; i++) {
                                    if (buf[i] != expected(f, sliceStart + pos + i)) {
                                        failure.compareAndSet(null, "file " + f + " offset " + (sliceStart + pos + i) + ": readBytes mismatch");
                                        break;
                                    }
                                }
                                bytesChecked.addAndGet(len);
                            }
                        }
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e.toString());
                }
            });
            threads.add(th);
            th.start();
        }
        for (Thread th : threads) {
            th.join();
        }
        for (IndexInput in : mains) {
            in.close();
        }
        String stats = poolResources.getPool().poolStats();
        logger.info("checked {} MB; {}", bytesChecked.get() >> 20, stats);
        assertNull(failure.get() + " (" + stats + ")", failure.get());
        // Reading at least all the data once (~2.3x the pool) churns through every slot several times.
        assertTrue("checked only " + (bytesChecked.get() >> 20) + " MB", bytesChecked.get() > (long) FILES * FILE_BYTES);
    }
}

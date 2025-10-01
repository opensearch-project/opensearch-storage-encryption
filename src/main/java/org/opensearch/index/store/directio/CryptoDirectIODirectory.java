/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE;
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.Provider;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.index.store.iv.KeyIvResolver;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.index.store.read_ahead.impl.ReadaheadManagerImpl;

@SuppressForbidden(reason = "uses custom DirectIO")
public final class CryptoDirectIODirectory extends FSDirectory {
    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectIODirectory.class);
    private final AtomicLong nextTempFileCounter = new AtomicLong();

    private final Pool<RefCountedMemorySegment> memorySegmentPool;
    private final BlockCache<RefCountedMemorySegment> blockCache;
    private final Worker readAheadworker;
    private final KeyIvResolver keyIvResolver;

    public CryptoDirectIODirectory(
        Path path,
        LockFactory lockFactory,
        Provider provider,
        KeyIvResolver keyIvResolver,
        Pool<RefCountedMemorySegment> memorySegmentPool,
        BlockCache<RefCountedMemorySegment> blockCache,
        BlockLoader<RefCountedMemorySegment> blockLoader,
        Worker worker
    )
        throws IOException {
        super(path, lockFactory);
        this.keyIvResolver = keyIvResolver;
        this.memorySegmentPool = memorySegmentPool;
        this.blockCache = blockCache;
        this.readAheadworker = worker;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        ensureCanRead(name);

        Path file = getDirectory().resolve(name);
        long size = Files.size(file);
        if (size == 0) {
            throw new IOException("Cannot open empty file with DirectIO: " + file);
        }

        ReadaheadManager readAheadManager = new ReadaheadManagerImpl(readAheadworker);
        ReadaheadContext readAheadContext = readAheadManager.register(file, size);
        BlockSlotTinyCache pinRegistry = new BlockSlotTinyCache(blockCache, file, size);

        return CachedMemorySegmentIndexInput
            .newInstance(
                "CachedMemorySegmentIndexInput(path=\"" + file + "\")",
                file,
                size,
                blockCache,
                readAheadManager,
                readAheadContext,
                pinRegistry
            );
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        if (name.contains("segments_") || name.endsWith(".si")) {
            return super.createOutput(name, context);
        }

        ensureOpen();
        Path path = directory.resolve(name);
        OutputStream fos = Files.newOutputStream(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);

        return new BufferIOWithCaching(
            name,
            path,
            fos,
            this.keyIvResolver.getDataKey().getEncoded(),
            keyIvResolver.getIvBytes(),
            this.memorySegmentPool,
            this.blockCache
        );

    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        if (prefix.contains("segments_") || prefix.endsWith(".si")) {
            return super.createTempOutput(prefix, suffix, context);
        }

        ensureOpen();
        String name = getTempFileName(prefix, suffix, nextTempFileCounter.getAndIncrement());
        Path path = directory.resolve(name);
        OutputStream fos = Files.newOutputStream(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);

        return new BufferIOWithCaching(
            name,
            path,
            fos,
            this.keyIvResolver.getDataKey().getEncoded(),
            keyIvResolver.getIvBytes(),
            this.memorySegmentPool,
            this.blockCache
        );
    }

    // only close resources owned by this directory type.
    // the actual directory is closed only once (see HybridCryptoDirectory.java)
    @Override
    @SuppressWarnings("ConvertToTryWithResources")
    public synchronized void close() throws IOException {
        readAheadworker.close();
    }

    @Override
    public void deleteFile(String name) throws IOException {
        Path file = getDirectory().resolve(name);

        if (blockCache != null) {
            try {
                long fileSize = Files.size(file);
                if (fileSize > 0) {
                    final int totalBlocks = (int) ((fileSize + CACHE_BLOCK_SIZE - 1) >>> CACHE_BLOCK_SIZE_POWER);
                    for (int i = 0; i < totalBlocks; i++) {
                        final long blockOffset = (long) i << CACHE_BLOCK_SIZE_POWER;
                        FileBlockCacheKey key = new FileBlockCacheKey(file, blockOffset);
                        blockCache.invalidate(key);
                    }
                }
            } catch (IOException e) {
                // Fall back to path-based invalidation if file size unavailable
                LOGGER.warn("Failed to get file size", e);
            }
        }

        super.deleteFile(name);
    }
}

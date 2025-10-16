/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE;
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
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
import org.opensearch.index.store.cipher.EncryptionCache;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.pool.MemorySegmentPool;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.index.store.read_ahead.impl.ReadaheadManagerImpl;
import org.opensearch.index.store.footer.EncryptionFooter;
import org.opensearch.index.store.footer.EncryptionMetadataTrailer;

@SuppressForbidden(reason = "uses custom DirectIO")
public final class CryptoDirectIODirectory extends FSDirectory {
    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectIODirectory.class);
    private final AtomicLong nextTempFileCounter = new AtomicLong();

    private final Pool<MemorySegmentPool.SegmentHandle> memorySegmentPool;
    private final BlockCache<RefCountedMemorySegment> blockCache;
    private final Worker readAheadworker;
    private final KeyResolver keyResolver;
    private final Provider provider;
    private final Path dirPath;
    private final String dirPathString;
    private final byte[] dataKeyBytes;
    private final byte[] ivBytes;

    public CryptoDirectIODirectory(
            Path path,
            LockFactory lockFactory,
            Provider provider,
            KeyResolver keyResolver,
            Pool<MemorySegmentPool.SegmentHandle> memorySegmentPool,
            BlockCache<RefCountedMemorySegment> blockCache,
            BlockLoader<MemorySegmentPool.SegmentHandle> blockLoader,
            Worker worker
    )
            throws IOException {
        super(path, lockFactory);
        this.keyResolver = keyResolver;
        this.memorySegmentPool = memorySegmentPool;
        this.blockCache = blockCache;
        this.readAheadworker = worker;
        this.provider = provider;
        this.dirPath = getDirectory();
        this.dirPathString = dirPath.toAbsolutePath().toString();
        this.dataKeyBytes = keyResolver.getDataKey().getEncoded();
        this.ivBytes = keyResolver.getIvBytes();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        ensureCanRead(name);

        Path file = dirPath.resolve(name);
        long rawFileSize = Files.size(file);
        if (rawFileSize == 0) {
            throw new IOException("Cannot open empty file with DirectIO: " + file);
        }

        // Calculate content length with OSEF validation
        long contentLength = calculateContentLengthWithValidation(file, rawFileSize);

        ReadaheadManager readAheadManager = new ReadaheadManagerImpl(readAheadworker);
        ReadaheadContext readAheadContext = readAheadManager.register(file, contentLength);
        BlockSlotTinyCache pinRegistry = new BlockSlotTinyCache(blockCache, file, contentLength);

        return CachedMemorySegmentIndexInput
                .newInstance(
                        "CachedMemorySegmentIndexInput(path=\"" + file + "\")",
                        file,
                        contentLength,
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
                dataKeyBytes,
                ivBytes,
                this.memorySegmentPool,
                this.blockCache,
                this.provider
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
                dataKeyBytes,
                ivBytes,
                this.memorySegmentPool,
                this.blockCache,
                this.provider
        );
    }

    // only close resources owned by this directory type.
    // the actual directory is closed only once (see HybridCryptoDirectory.java)
    @Override
    @SuppressWarnings("ConvertToTryWithResources")
    public synchronized void close() throws IOException {
        readAheadworker.close();
        EncryptionCache.getInstance().invalidateDirectory(dirPathString);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        Path file = dirPath.resolve(name);
        String filePath = dirPathString + "/" + name;

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
        EncryptionCache.getInstance().invalidateFile(filePath);
        super.deleteFile(name);
    }

    /**
     * Calculate content length with OSEF validation.
     * Fast path: check cache first to avoid FileChannel open.
     */
    private long calculateContentLengthWithValidation(Path file, long rawFileSize) throws IOException {
        if (rawFileSize < EncryptionMetadataTrailer.MIN_FOOTER_SIZE) {
            return rawFileSize;
        }

        // Fast path: check cache first - avoids FileChannel open
        String filePath = file.toAbsolutePath().toString();
        EncryptionFooter cachedFooter = EncryptionCache.getInstance().getFooter(filePath);
        if (cachedFooter != null) {
            return rawFileSize - cachedFooter.getFooterLength();
        }

        // Slow path: read footer from disk
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            try {
                EncryptionFooter footer = EncryptionFooter.readFromChannel(file, channel, dataKeyBytes);
                return rawFileSize - footer.getFooterLength();
            } catch (EncryptionFooter.NotOSEFFileException e) {
                LOGGER.debug("Not an OSEF file: {}", file);
                return rawFileSize;
            }
        }
    }
}

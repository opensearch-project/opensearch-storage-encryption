/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.Provider;
import java.time.Duration;
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
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.footer.EncryptionFooter;
import org.opensearch.index.store.footer.EncryptionMetadataTrailer;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.metrics.ErrorType;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.index.store.read_ahead.impl.ReadaheadManagerImpl;

/**
 * A high-performance FSDirectory implementation that combines Direct I/O operations with encryption.
 *
 * <p>This directory provides:
 * <ul>
 * <li>Direct I/O operations bypassing the OS page cache for better memory control</li>
 * <li>Block-level caching with memory segment pools for efficient memory management</li>
 * <li>Transparent encryption/decryption using OpenSSL native implementations</li>
 * <li>Read-ahead optimizations for sequential access patterns</li>
 * <li>Automatic cache invalidation on file deletion</li>
 * </ul>
 *
 * <p>The directory uses {@link BufferIOWithCaching} for output operations which encrypts
 * data before writing to disk and caches plaintext blocks for read operations. Input
 * operations use {@link CachedMemorySegmentIndexInput} with a multi-level cache hierarchy
 * including {@link BlockSlotTinyCache} for L1 caching.
 *
 * <p>Note: Some file types (segments files and .si files) fall back to the parent
 * directory implementation to avoid compatibility issues.
 *
 * @opensearch.internal
 */
@SuppressForbidden(reason = "uses custom DirectIO")
public class BufferPoolDirectory extends FSDirectory {
    private static final Logger LOGGER = LogManager.getLogger(BufferPoolDirectory.class);
    private final AtomicLong nextTempFileCounter = new AtomicLong();

    private final Pool<RefCountedMemorySegment> memorySegmentPool;
    private final BlockCache<RefCountedMemorySegment> blockCache;
    private final Worker readAheadworker;
    private final Provider provider;
    private final Path dirPath;
    private final byte[] masterKeyBytes;
    private final EncryptionMetadataCache encryptionMetadataCache;

    /**
     * Creates a new CryptoDirectIODirectory with the specified components.
     *
     * @param path the directory path
     * @param lockFactory the lock factory for coordinating access
     * @param provider the security provider for cryptographic operations
     * @param keyResolver resolver for encryption keys and initialization vectors
     * @param memorySegmentPool pool for managing off-heap memory segments
     * @param blockCache cache for storing decrypted blocks
     * @param blockLoader loader for reading blocks from storage
     * @param worker background worker for read-ahead operations
     * @throws IOException if the directory cannot be created or accessed
     */
    public BufferPoolDirectory(
        Path path,
        LockFactory lockFactory,
        Provider provider,
        KeyResolver keyResolver,
        Pool<RefCountedMemorySegment> memorySegmentPool,
        BlockCache<RefCountedMemorySegment> blockCache,
        BlockLoader<RefCountedMemorySegment> blockLoader,
        Worker worker,
        EncryptionMetadataCache encryptionMetadataCache
    )
        throws IOException {
        super(path, lockFactory);
        this.memorySegmentPool = memorySegmentPool;
        this.blockCache = blockCache;
        this.readAheadworker = worker;
        this.provider = provider;
        this.dirPath = getDirectory();
        this.masterKeyBytes = keyResolver.getDataKey().getEncoded();
        this.encryptionMetadataCache = encryptionMetadataCache;

        // startCacheStatsTelemetry(); // uncomment for local testing
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        try {
            ensureOpen();
            ensureCanRead(name);

            Path file = dirPath.resolve(name);
            long rawFileSize = Files.size(file);
            if (rawFileSize == 0) {
                throw new IOException("Cannot open empty file with DirectIO: " + file);
            }

            // Calculate content length with OSEF validation
            long contentLength = calculateContentLengthWithValidation(file, rawFileSize);

            ReadaheadManager readAheadManager = new ReadaheadManagerImpl(readAheadworker, blockCache);
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
        } catch (Exception e) {
            CryptoMetricsService.getInstance().recordError(ErrorType.INDEX_INPUT_ERROR);
            throw e;
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        try {
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
                masterKeyBytes,
                this.memorySegmentPool,
                this.blockCache,
                this.provider,
                this.encryptionMetadataCache
            );
        } catch (Exception e) {
            CryptoMetricsService.getInstance().recordError(ErrorType.INDEX_OUTPUT_ERROR);
            throw e;
        }
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
            masterKeyBytes,
            this.memorySegmentPool,
            this.blockCache,
            this.provider,
            this.encryptionMetadataCache
        );
    }

    // only close resources owned by this directory type.
    // the actual directory is closed only once (see HybridCryptoDirectory.java)
    @Override
    @SuppressWarnings("ConvertToTryWithResources")
    public synchronized void close() throws IOException {
        readAheadworker.close();
        encryptionMetadataCache.invalidateDirectory();

        // Invalidate all cache entries for this directory to prevent memory leaks
        // when the shard/index is closed or deleted
        if (blockCache != null) {
            blockCache.invalidateByPathPrefix(dirPath);
        }
    }

    @Override
    public void deleteFile(String name) throws IOException {
        Path file = dirPath.resolve(name);

        // Cancel any pending async read-ahead operations for this file FIRST
        // to prevent race where read-ahead tries to load blocks from deleted/replaced file
        readAheadworker.cancel(file);

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
                LOGGER.warn("Failed to get file size for clearing cache for deleting shard", e);
            }
        }
        super.deleteFile(name);
        encryptionMetadataCache.invalidateFile(EncryptionMetadataCache.normalizePath(file));
    }

    /**
     * Calculate content length by reading footer if file is an OSEF file.
     * Returns raw file size for non-OSEF files (< MIN_FOOTER_SIZE).
     */
    private long calculateContentLengthWithValidation(Path file, long rawFileSize) throws IOException {
        if (rawFileSize < EncryptionMetadataTrailer.MIN_FOOTER_SIZE) {
            return rawFileSize;
        }

        String normalizedPath = EncryptionMetadataCache.normalizePath(file);

        // Check cache first for fast path
        EncryptionFooter cachedFooter = encryptionMetadataCache.getFooter(normalizedPath);
        if (cachedFooter != null) {
            return rawFileSize - cachedFooter.getFooterLength();
        }

        // Cache miss - read footer from disk (happens during file open before cache populated)
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            EncryptionFooter footer = EncryptionFooter.readViaFileChannel(normalizedPath, channel, masterKeyBytes, encryptionMetadataCache);

            // Metadata is already cached by readViaFileChannel

            return rawFileSize - footer.getFooterLength();
        } catch (EncryptionFooter.NotOSEFFileException e) {
            // Not an encrypted file - return raw size
            return rawFileSize;
        }
    }

    private void logCacheAndPoolStats() {
        try {

            if (blockCache instanceof CaffeineBlockCache) {
                String cacheStats = ((CaffeineBlockCache<?, ?>) blockCache).cacheStats();
                LOGGER.info("{}", cacheStats);
            }

        } catch (Exception e) {
            LOGGER.warn("Failed to log cache/pool stats", e);
        }
    }

    @SuppressWarnings("unused")
    // only used during local testing.
    private void startCacheStatsTelemetry() {
        Thread loggerThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(Duration.ofSeconds(10));
                    logCacheAndPoolStats();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Throwable t) {
                    LOGGER.warn("Error in collecting cache stats", t);
                }
            }
        });

        loggerThread.setDaemon(true);
        loggerThread.setName("DirectIOBufferPoolStatsLogger");
        loggerThread.start();
    }
}

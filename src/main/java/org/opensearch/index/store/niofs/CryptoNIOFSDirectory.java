/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.niofs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.Provider;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.footer.EncryptionFooter;
import org.opensearch.index.store.footer.EncryptionMetadataTrailer;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.metrics.ErrorType;

/**
 * A NioFS directory implementation that encrypts files to be stored based on a
 * user supplied key
 *
 * @opensearch.internal
 */
public class CryptoNIOFSDirectory extends NIOFSDirectory {
    private final Provider provider;

    /** The resolver for encryption keys and initialization vectors. */
    public final KeyResolver keyResolver;

    private final AtomicLong nextTempFileCounter = new AtomicLong();
    private final int algorithmId = 1; // Default to AES_256_GCM_CTR
    private final Path dirPath;
    private final EncryptionMetadataCache encryptionMetadataCache;

    /**
     * Creates a new CryptoNIOFSDirectory with encryption support.
     *
     * @param lockFactory the lock factory for coordinating access
     * @param location the directory path
     * @param provider the security provider for cryptographic operations
     * @param keyResolver resolver for encryption keys and initialization vectors
     * @throws IOException if the directory cannot be created or accessed
     */
    public CryptoNIOFSDirectory(
        LockFactory lockFactory,
        Path location,
        Provider provider,
        KeyResolver keyResolver,
        EncryptionMetadataCache encryptionMetadataCache
    )
        throws IOException {
        super(location, lockFactory);
        this.provider = provider;
        this.keyResolver = keyResolver;
        this.dirPath = getDirectory();
        this.encryptionMetadataCache = encryptionMetadataCache;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        try {
            ensureOpen();
            ensureCanRead(name);
            Path path = getDirectory().resolve(name);
            FileChannel fc = FileChannel.open(path, StandardOpenOption.READ);
            boolean success = false;

            try {
                final IndexInput indexInput = new CryptoBufferedIndexInput(
                    "CryptoBufferedIndexInput(path=\"" + path + "\")",
                    fc,
                    context,
                    this.keyResolver,
                    path,
                    this.encryptionMetadataCache
                );
                success = true;
                return indexInput;
            } finally {
                if (!success) {
                    IOUtils.closeWhileHandlingException(fc);
                }
            }
        } catch (Exception e) {
            CryptoMetricsService.getInstance().recordError(ErrorType.INDEX_INPUT_ERROR);
            throw e;
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        try {
            ensureOpen();
            Path path = directory.resolve(name);

            OutputStream fos = Files.newOutputStream(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);

            return new CryptoOutputStreamIndexOutput(
                name,
                path,
                fos,
                this.keyResolver,
                provider,
                algorithmId,
                path,
                this.encryptionMetadataCache
            );
        } catch (Exception e) {
            CryptoMetricsService.getInstance().recordError(ErrorType.INDEX_OUTPUT_ERROR);
            throw e;
        }
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        ensureOpen();
        String name = getTempFileName(prefix, suffix, nextTempFileCounter.getAndIncrement());
        Path path = directory.resolve(name);
        OutputStream fos = Files.newOutputStream(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);

        return new CryptoOutputStreamIndexOutput(
            name,
            path,
            fos,
            this.keyResolver,
            provider,
            algorithmId,
            path,
            this.encryptionMetadataCache
        );
    }

    @Override
    public long fileLength(String name) throws IOException {
        Path path = dirPath.resolve(name);
        long fileSize = super.fileLength(name);

        if (fileSize < EncryptionMetadataTrailer.MIN_FOOTER_SIZE) {
            return fileSize;
        }

        String normalizedPath = EncryptionMetadataCache.normalizePath(path);

        // check cache first
        EncryptionFooter cachedFooter = encryptionMetadataCache.getFooter(normalizedPath);
        if (cachedFooter != null) {
            return fileSize - cachedFooter.getFooterLength();
        }

        // read footer from disk with OSEF validation
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            try {
                EncryptionFooter footer = EncryptionFooter
                    .readViaFileChannel(normalizedPath, channel, keyResolver.getDataKey().getEncoded(), encryptionMetadataCache);
                return fileSize - footer.getFooterLength();
            } catch (EncryptionFooter.NotOSEFFileException e) {
                return fileSize;
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        isOpen = false;
        deletePendingFiles();
        encryptionMetadataCache.invalidateDirectory();
    }

    @Override
    public void deleteFile(String name) throws IOException {
        super.deleteFile(name);
        encryptionMetadataCache.invalidateFile(EncryptionMetadataCache.normalizePath(dirPath.resolve(name)));
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.niofs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
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
import org.opensearch.index.store.cipher.EncryptionCache;
import org.opensearch.index.store.footer.EncryptionFooter;
import org.opensearch.index.store.footer.EncryptionMetadataTrailer;
import org.opensearch.index.store.key.KeyResolver;

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
    private final String dirPathString;

    /**
     * Creates a new CryptoNIOFSDirectory with encryption support.
     *
     * @param lockFactory the lock factory for coordinating access
     * @param location the directory path
     * @param provider the security provider for cryptographic operations
     * @param keyResolver resolver for encryption keys and initialization vectors
     * @throws IOException if the directory cannot be created or accessed
     */
    public CryptoNIOFSDirectory(LockFactory lockFactory, Path location, Provider provider, KeyResolver keyResolver) throws IOException {
        super(location, lockFactory);
        this.provider = provider;
        this.keyResolver = keyResolver;
        this.dirPath = getDirectory();
        this.dirPathString = dirPath.toAbsolutePath().toString();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        if (name.contains("segments_") || name.endsWith(".si")) {
            return super.openInput(name, context);
        }

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
                    path
            );
            success = true;
            return indexInput;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(fc);
            }
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        if (name.contains("segments_") || name.endsWith(".si")) {
            return super.createOutput(name, context);
        }

        ensureOpen();
        Path path = directory.resolve(name);

        OutputStream fos = Files.newOutputStream(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);

        return new CryptoOutputStreamIndexOutput(name, path, fos, this.keyResolver, provider, algorithmId, path);
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

        return new CryptoOutputStreamIndexOutput(name, path, fos, this.keyResolver, provider, algorithmId, path);
    }

    @Override
    public long fileLength(String name) throws IOException {
        if (name.contains("segments_") || name.endsWith(".si")) {
            return super.fileLength(name);
        }

        Path path = dirPath.resolve(name);
        long fileSize = super.fileLength(name);

        if (fileSize < EncryptionMetadataTrailer.MIN_FOOTER_SIZE) {
            return fileSize;
        }

        // check cache first
        String filePath = dirPathString + "/" + name;
        EncryptionFooter cachedFooter = EncryptionCache.getInstance().getFooter(filePath);
        if (cachedFooter != null) {
            return fileSize - cachedFooter.getFooterLength();
        }

        // read footer from disk with OSEF validation
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            try {
                EncryptionFooter footer = EncryptionFooter.readFromChannel(path, channel, keyResolver.getDataKey().getEncoded());
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
        EncryptionCache.getInstance().invalidateDirectory(dirPathString);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        super.deleteFile(name);
        EncryptionCache.getInstance().invalidateFile(dirPathString + "/" + name);
    }
}

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
import java.security.Key;
import java.security.Provider;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.store.key.KeyResolver;

/**
 * A NioFS directory implementation that encrypts files to be stored based on a
 * user supplied key
 *
 * @opensearch.internal
 */
public class CryptoNIOFSDirectory extends NIOFSDirectory {
    private static final Logger LOGGER = LogManager.getLogger(CryptoNIOFSDirectory.class);

    public final KeyResolver keyResolver;
    private final AtomicLong nextTempFileCounter = new AtomicLong();

    public CryptoNIOFSDirectory(LockFactory lockFactory, Path location, Provider provider, KeyResolver keyIvResolver) throws IOException {
        super(location, lockFactory);
        this.keyResolver = keyIvResolver;
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
            Key key = keyResolver.getFileEncryptionKey(path, name);
            final IndexInput indexInput = new CryptoBufferedIndexInput("CryptoBufferedIndexInput(path=\"" + path + "\")", fc, context, key);
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

        return new CryptoOutputStreamIndexOutput(name, path, fos, keyResolver);
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

        return new CryptoOutputStreamIndexOutput(name, path, fos, keyResolver);
    }

    @Override
    public synchronized void close() throws IOException {
        isOpen = false;
        deletePendingFiles();
    }

    @Override
    public void deleteFile(String name) throws IOException {
        this.keyResolver.invalidateFileEncrytionKeyStore(name);
        super.deleteFile(name);
    }
}

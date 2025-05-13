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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.crypto.Cipher;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.store.cipher.CipherFactory;
import org.opensearch.index.store.iv.DefaultKeyIvResolver;
import org.opensearch.index.store.iv.KeyIvResolver;
import org.opensearch.index.store.mmapfs.MMapCryptoIndexInput;

/**
 * A NioFS directory implementation that encrypts files to be stored based on a
 * user supplied key
 *
 * @opensearch.internal
 */
public final class CryptoNIOFSDirectory extends NIOFSDirectory {
    private final Provider provider;
    private final KeyIvResolver keyResolver;
    private final AtomicLong nextTempFileCounter = new AtomicLong();
    private Set<String> extensions;
    private final Path location;

    public CryptoNIOFSDirectory(LockFactory lockFactory, Path location, Provider provider, MasterKeyProvider keyProvider)
        throws IOException {
        super(location, lockFactory);
        this.location = location;
        this.provider = provider;

        Directory baseDir = new NIOFSDirectory(location, lockFactory);
        this.keyResolver = new DefaultKeyIvResolver(baseDir, provider, keyProvider);

        // list of file extensions to use with CrytoBufferedIndexInput. other will use MMapCryptoIndexInput
        List<String> list = new ArrayList<>();
        list.add("segments_N");
        list.add("write.lock");
        list.add("si");
        list.add("cfe");
        list.add("fnm");
        list.add("fdx");
        list.add("fdt");
        list.add("pos");
        list.add("pay");
        list.add("nvm");
        list.add("dvm");
        list.add("tvx");
        list.add("tvd");
        list.add("liv");
        list.add("dii");
        list.add("vem");
        list.add("cfs"); // temporary
        /*list.add("tim");
        list.add("tip");
        list.add("fdt");
        list.add("fdx");
        list.add("nvd");
        list.add("nvm");
        list.add("dvd");
        list.add("dvm");
        list.add("dii");
        list.add("dim");*/
        extensions = new HashSet<>(list);
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
        final String extension = FileSwitchDirectory.getExtension(name);

        try {
            Cipher cipher = CipherFactory.getCipher(provider);
            CipherFactory.initCipher(cipher, keyResolver.getDataKey(), keyResolver.getIvBytes(), Cipher.DECRYPT_MODE, 0);
            final IndexInput indexInput;
            // Use MMapCryptoIndexInput based on file extension
            if (!extensions.contains(extension)) {
                // System.out.println("OpenInput:: FileName: "+name+" length "+fc.size()+ " "+context+" MMapCryptoIndexInput(path=\"" + path
                // + "\")");
                MMapDirectory mmapDirectory = new MMapDirectory(location, lockFactory);
                indexInput = new MMapCryptoIndexInput(
                    "MMapCryptoIndexInput(path=\"" + path + "\")",
                    mmapDirectory.openInput(name, context),
                    cipher,
                    keyResolver
                );
                fc.close();
            } else {
                indexInput = new CryptoBufferedIndexInput(
                    "CryptoBufferedIndexInput(path=\"" + path + "\")",
                    fc,
                    context,
                    cipher,
                    this.keyResolver
                );
            }
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

        Cipher cipher = CipherFactory.getCipher(provider);
        CipherFactory.initCipher(cipher, this.keyResolver.getDataKey(), keyResolver.getIvBytes(), Cipher.ENCRYPT_MODE, 0);

        return new CryptoOutputStreamIndexOutput(name, path, fos, cipher);
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

        Cipher cipher = CipherFactory.getCipher(provider);
        CipherFactory.initCipher(cipher, keyResolver.getDataKey(), keyResolver.getIvBytes(), Cipher.ENCRYPT_MODE, 0);

        return new CryptoOutputStreamIndexOutput(name, path, fos, cipher);
    }

    @Override
    public synchronized void close() throws IOException {
        isOpen = false;
        deletePendingFiles();
    }
}

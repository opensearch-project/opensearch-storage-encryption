/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.hybrid;

import java.io.IOException;
import java.security.Provider;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.opensearch.index.store.directio.CryptoDirectIODirectory;
import org.opensearch.index.store.iv.KeyIvResolver;
import org.opensearch.index.store.mmap.EagerDecryptedCryptoMMapDirectory;
import org.opensearch.index.store.mmap.LazyDecryptedCryptoMMapDirectory;
import org.opensearch.index.store.niofs.CryptoNIOFSDirectory;

public class HybridCryptoDirectory extends CryptoNIOFSDirectory {
    private static final Logger LOGGER = LogManager.getLogger(CryptoNIOFSDirectory.class);

    private final LazyDecryptedCryptoMMapDirectory lazyDecryptedCryptoMMapDirectoryDelegate;
    private final EagerDecryptedCryptoMMapDirectory eagerDecryptedCryptoMMapDirectory;
    private final CryptoDirectIODirectory cryptoDirectIODirectory;

    // Only these extensions get special routing - everything else goes to NIOFS
    private final Set<String> specialExtensions;
    private final Set<String> stillMmaped;

    public HybridCryptoDirectory(
        LockFactory lockFactory,
        LazyDecryptedCryptoMMapDirectory delegate,
        EagerDecryptedCryptoMMapDirectory eagerDecryptedCryptoMMapDirectory,
        CryptoDirectIODirectory cryptoDirectIODirectory,
        Provider provider,
        KeyIvResolver keyIvResolver,
        Set<String> nioExtensions
    )
        throws IOException {
        super(lockFactory, delegate.getDirectory(), provider, keyIvResolver);
        this.lazyDecryptedCryptoMMapDirectoryDelegate = delegate;
        this.eagerDecryptedCryptoMMapDirectory = eagerDecryptedCryptoMMapDirectory;
        this.cryptoDirectIODirectory = cryptoDirectIODirectory;
        this.stillMmaped = Set.of("kdm", "tip", "tmd", "psm", "fdm");
        this.specialExtensions = Set.of("kdd", "cfs", "doc", "dvd");
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        String extension = FileSwitchDirectory.getExtension(name);

        ensureOpen();
        ensureCanRead(name);

        if (specialExtensions.contains(extension)) {
            return cryptoDirectIODirectory.openInput(name, context);
        }

        if (!stillMmaped.contains(extension)) {
            return eagerDecryptedCryptoMMapDirectory.openInput(name, context);
        }

        return super.openInput(name, context);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        String extension = FileSwitchDirectory.getExtension(name);

        ensureOpen();
        if (specialExtensions.contains(extension)) {
            return cryptoDirectIODirectory.createOutput(name, context);
        }

        return super.createOutput(name, context);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        String ext = FileSwitchDirectory.getExtension(name);

        if (specialExtensions.contains(ext)) {
            cryptoDirectIODirectory.deleteFile(name);
        } else {
            super.deleteFile(name); // goes to CryptoNIOFSDirectory
        }
    }

    @Override
    public void close() throws IOException {
        cryptoDirectIODirectory.close(); // only closes its resources.
        super.close(); // actually closes pending files.
    }
}

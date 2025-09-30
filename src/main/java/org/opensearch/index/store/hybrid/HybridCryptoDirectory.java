/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.hybrid;

import java.io.IOException;
import java.security.Provider;
import java.util.Set;

import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.opensearch.index.store.directio.CryptoDirectIODirectory;
import org.opensearch.index.store.iv.KeyIvResolver;
import org.opensearch.index.store.niofs.CryptoNIOFSDirectory;

public class HybridCryptoDirectory extends CryptoNIOFSDirectory {

    private final CryptoDirectIODirectory cryptoDirectIODirectory;

    // Only these extensions get special routing - everything else goes to NIOFS
    private final Set<String> specialExtensions;

    public HybridCryptoDirectory(LockFactory lockFactory, CryptoDirectIODirectory delegate, Provider provider, KeyIvResolver keyIvResolver)
        throws IOException {
        super(lockFactory, delegate.getDirectory(), provider, keyIvResolver);
        this.cryptoDirectIODirectory = delegate;
        // todo can be moved to buffer-io with caching
        // "kdm", "tip", "tmd", "psm", "fdm", "kdi");
        this.specialExtensions = Set.of("kdd", "cfs", "doc", "dvd", "nvd", "tim");
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        String extension = FileSwitchDirectory.getExtension(name);

        ensureOpen();
        ensureCanRead(name);

        if (specialExtensions.contains(extension)) {
            return cryptoDirectIODirectory.openInput(name, context);
        }

        return super.openInput(name, context);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        String extension = FileSwitchDirectory.getExtension(name);

        ensureOpen();
        ensureCanRead(name);

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

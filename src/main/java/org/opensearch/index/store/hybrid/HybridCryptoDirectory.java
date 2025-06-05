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
import org.apache.lucene.store.LockFactory;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.store.iv.KeyIvResolver;
import org.opensearch.index.store.mmap.CryptoMMapDirectory;
import org.opensearch.index.store.niofs.CryptoNIOFSDirectory;

public class HybridCryptoDirectory extends CryptoNIOFSDirectory {

    private final CryptoMMapDirectory delegate;
    private final Set<String> nioExtensions;

    public HybridCryptoDirectory(
        LockFactory lockFactory,
        CryptoMMapDirectory delegate,
        Provider provider,
        KeyIvResolver keyIvResolver,
        Set<String> nioExtensions
    )
        throws IOException {
        super(lockFactory, delegate.getDirectory(), provider, keyIvResolver);
        this.delegate = delegate;
        this.nioExtensions = nioExtensions;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        if (useDelegate(name)) {
            ensureOpen();
            ensureCanRead(name);
            return delegate.openInput(name, context);
        } else {
            return super.openInput(name, context);
        }
    }

    private boolean useDelegate(String name) {
        String extension = FileSwitchDirectory.getExtension(name);

        if (name.endsWith(".tmp") || name.contains("segments_")) {
            return false;
        }

        // [cfe, tvd, fnm, nvm, write.lock, dii, pay, segments_N, pos, si, fdt, tvx, liv, dvm, fdx, vem]

        return extension == null || !nioExtensions.contains(extension);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(super::close, delegate);
    }

    public CryptoMMapDirectory getDelegate() {
        return delegate;
    }
}

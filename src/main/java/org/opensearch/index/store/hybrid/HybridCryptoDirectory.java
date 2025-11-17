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
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.directio.CryptoDirectIODirectory;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.niofs.CryptoNIOFSDirectory;

/**
 * Hybrid encrypted directory that routes file operations based on OpenSearch's
 * {@code INDEX_STORE_HYBRID_NIO_EXTENSIONS} setting.
 *
 * <p>Routing logic:
 * <ul>
 * <li><strong>Direct I/O</strong> ({@link CryptoDirectIODirectory}): Files NOT in nioExtensions
 *     (e.g., tim, doc, dvd, nvd, cfs) - large data files with block caching</li>
 * <li><strong>NIO</strong> ({@link CryptoNIOFSDirectory}): Files in nioExtensions
 *     (e.g., si, cfe, fnm, fdx, segments_N) - metadata and small files</li>
 * </ul>
 *
 * <p>Both directories share the same encryption keys and IV resolution.
 *
 * @opensearch.internal
 */
public class HybridCryptoDirectory extends CryptoNIOFSDirectory {
    private final CryptoDirectIODirectory cryptoDirectIODirectory;
    private final Set<String> nioExtensions;

    /**
     * Creates a new HybridCryptoDirectory that routes operations between NIO and Direct I/O.
     *
     * <p>The hybrid directory uses the provided CryptoDirectIODirectory for performance-critical
     * large files and falls back to NIO operations for smaller files and metadata.
     *
     * @param lockFactory the lock factory for coordinating access across both directories
     * @param delegate the CryptoDirectIODirectory to use for Direct I/O operations
     * @param provider the security provider for cryptographic operations
     * @param keyResolver resolver for encryption keys and initialization vectors (shared across both directories)
     * @throws IOException if either directory cannot be initialized
     */
    public HybridCryptoDirectory(
        LockFactory lockFactory,
        CryptoDirectIODirectory delegate,
        Provider provider,
        KeyResolver keyResolver,
        EncryptionMetadataCache encryptionMetadataCache,
        Set<String> nioExtensions
    )
        throws IOException {
        super(lockFactory, delegate.getDirectory(), provider, keyResolver, encryptionMetadataCache);
        this.cryptoDirectIODirectory = delegate;
        this.nioExtensions = nioExtensions;
    }

    /**
     * Determines if a file should be routed to Direct I/O + caching directory
     * based on its extension.
     *
     * @param extension the file extension
     * @return true if the file should use Direct I/O, false for NIO
     */
    private boolean shouldUseDirectIO(String extension) {
        return !extension.isEmpty() && !nioExtensions.contains(extension);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        String extension = FileSwitchDirectory.getExtension(name);

        ensureOpen();
        ensureCanRead(name);

        if (shouldUseDirectIO(extension)) {
            return cryptoDirectIODirectory.openInput(name, context);
        }

        return super.openInput(name, context);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        String extension = FileSwitchDirectory.getExtension(name);

        ensureOpen();
        ensureCanRead(name);

        if (shouldUseDirectIO(extension)) {
            return cryptoDirectIODirectory.createOutput(name, context);
        }

        return super.createOutput(name, context);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        String extension = FileSwitchDirectory.getExtension(name);

        if (shouldUseDirectIO(extension)) {
            cryptoDirectIODirectory.deleteFile(name);
        } else {
            super.deleteFile(name);
        }
    }

    @Override
    public void close() throws IOException {
        cryptoDirectIODirectory.close(); // only closes its resources.
        super.close(); // actually closes pending files.
    }
}

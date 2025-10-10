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

/**
 * A hybrid directory implementation that intelligently routes file operations to different
 * underlying directory implementations based on file extensions.
 * 
 * <p>This directory combines the benefits of different I/O strategies:
 * <ul>
 * <li><strong>Direct I/O</strong> for large, performance-critical files (via {@link CryptoDirectIODirectory})</li>
 * <li><strong>NIO</strong> for smaller files and metadata (via {@link CryptoNIOFSDirectory})</li>
 * </ul>
 * 
 * <p>File routing is determined by extension:
 * <ul>
 * <li><strong>Direct I/O extensions:</strong> kdd, cfs, doc, dvd, nvd, tim</li>
 * <li><strong>NIO extensions:</strong> all other extensions (segments files, .si files, etc.)</li>
 * </ul>
 * 
 * <p>The routing strategy is designed to:
 * <ul>
 * <li>Use Direct I/O for large data files that benefit from bypassing OS cache</li>
 * <li>Use NIO for small metadata files where Direct I/O overhead isn't justified</li>
 * <li>Maintain compatibility with standard Lucene file operations</li>
 * <li>Provide transparent encryption for both file types</li>
 * </ul>
 * 
 * <p>Both underlying directories share the same encryption keys and IV resolution,
 * ensuring consistent encryption across all file types.
 * 
 * @opensearch.internal
 */
public class HybridCryptoDirectory extends CryptoNIOFSDirectory {

    private final CryptoDirectIODirectory cryptoDirectIODirectory;

    // Only these extensions get special routing - everything else goes to NIOFS
    private final Set<String> specialExtensions;

    /**
     * Creates a new HybridCryptoDirectory that routes operations between NIO and Direct I/O.
     * 
     * <p>The hybrid directory uses the provided CryptoDirectIODirectory for performance-critical
     * large files and falls back to NIO operations for smaller files and metadata.
     * 
     * @param lockFactory the lock factory for coordinating access across both directories
     * @param delegate the CryptoDirectIODirectory to use for Direct I/O operations
     * @param provider the security provider for cryptographic operations
     * @param keyIvResolver resolver for encryption keys and initialization vectors (shared across both directories)
     * @throws IOException if either directory cannot be initialized
     */
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

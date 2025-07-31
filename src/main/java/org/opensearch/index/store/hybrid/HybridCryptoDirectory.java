/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.hybrid;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IOContext.Context;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.opensearch.common.util.io.IOUtils;
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

    // File size thresholds for special files only
    private static final long MEDIUM_FILE_THRESHOLD = 10 * 1024 * 1024; // 10MB

    // Only these extensions get special routing - everything else goes to NIOFS
    private final Set<String> specialExtensions;

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
        this.specialExtensions = Set.of("kdd", "kdi", "kdm", "tip", "tim", "tmd", "doc", "dvd", "nvd", "psm", "fdm");
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        String extension = FileSwitchDirectory.getExtension(name);

        // TODO use the use-delegate method.
        if (!specialExtensions.contains(extension)) {
            return super.openInput(name, context);
        }

        ensureOpen();
        ensureCanRead(name);

        // MERGE context: Always use NIOFS for sequential, one-time access
        if (context.context() == Context.MERGE) {
            LOGGER.info("Routing {} to NIOFS for merge operation", name);
            return super.openInput(name, context);
        }

        // FLUSH context: New segment creation - consider future access patterns
        if (context.context() == Context.FLUSH) {
            LOGGER.info("Routing for flush operation", name);

            Path file = getDirectory().resolve(name);
            long fileSize = Files.size(file);

            // For files that will be accessed randomly after flush, prepare them for MMap
            // Exception: large files should avoid memory pressure during flush
            if (("kdd".equals(extension) || "cfs".equals(extension)) && fileSize > MEDIUM_FILE_THRESHOLD) {
                LOGGER.debug("Routing large {} to NIOFS during flush to avoid memory pressure", name);
                return super.openInput(name, context);
            }
        }

        // Special routing for key file types
        // return routeSpecialFile(name, extension, context);

        return cryptoDirectIODirectory.openInput(name, context);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        String extension = FileSwitchDirectory.getExtension(name);

        if (!specialExtensions.contains(extension)) {
            return super.createOutput(name, context);
        }

        ensureOpen();
        ensureCanRead(name);

        return cryptoDirectIODirectory.createOutput(name, context);
    }

    private IndexInput routeSpecialFile(String name, String extension, IOContext context) throws IOException {
        // MERGE context: Always use NIOFS for sequential, one-time access
        if (context.context() == Context.MERGE) {
            LOGGER.info("Routing {} to NIOFS for merge operation", name);
            return super.openInput(name, context);
        }

        // FLUSH context: New segment creation - consider future access patterns
        if (context.context() == Context.FLUSH) {
            LOGGER.info("Routing for flush operation", name);

            Path file = getDirectory().resolve(name);
            long fileSize = Files.size(file);

            // For files that will be accessed randomly after flush, prepare them for MMap
            // Exception: large files should avoid memory pressure during flush
            if (("kdd".equals(extension) || "cfs".equals(extension)) && fileSize > MEDIUM_FILE_THRESHOLD) {
                LOGGER.debug("Routing large {} to NIOFS during flush to avoid memory pressure", name);
                return super.openInput(name, context);
            }
            // Term files and tree files benefit from MMap even during flush
            if ("tim".equals(extension)
                || "tip".equals(extension)
                || "tmd".equals(extension)
                || "kdi".equals(extension)
                || "kdm".equals(extension)) {
                LOGGER.debug("Routing {} to MMap during flush for future random access", name);
                return lazyDecryptedCryptoMMapDirectoryDelegate.openInput(name, context);
            }
            // Small KDD and CFS files can use MMap during flush
            LOGGER.debug("Routing small {} to MMap during flush", name);
            return lazyDecryptedCryptoMMapDirectoryDelegate.openInput(name, context);
        }

        // Route based on file type and access patterns
        switch (extension) {
            case "tip", "tmd", "kdm", "kdi", "nvd", "psm" -> {
                return eagerDecryptedCryptoMMapDirectory.openInput(name, context);
            }

            case "tim", "doc", "fdm" -> {
                Path file = getDirectory().resolve(name);
                long fileSize = Files.size(file);

                if ((fileSize >= (2L << 20)) && (fileSize <= (8L << 20))) {
                    return eagerDecryptedCryptoMMapDirectory.openInput(name, context);
                }
                return lazyDecryptedCryptoMMapDirectoryDelegate.openInput(name, context);
            }

            case "cfs" -> {
                Path file = getDirectory().resolve(name);
                long fileSize = Files.size(file);

                if (fileSize <= (16L << 20)) {
                    return eagerDecryptedCryptoMMapDirectory.openInput(name, context);
                }
                return lazyDecryptedCryptoMMapDirectoryDelegate.openInput(name, context);
            }

            case "dvd", "kdd" -> {
                return lazyDecryptedCryptoMMapDirectoryDelegate.openInput(name, context);
            }

            default -> {
                return super.openInput(name, context);
            }
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(super::close, lazyDecryptedCryptoMMapDirectoryDelegate);
    }
}

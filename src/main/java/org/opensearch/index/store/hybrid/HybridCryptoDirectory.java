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
import org.apache.lucene.store.LockFactory;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.store.iv.KeyIvResolver;
import org.opensearch.index.store.mmap.EagerDecryptedCryptoMMapDirectory;
import org.opensearch.index.store.mmap.LazyDecryptedCryptoMMapDirectory;
import org.opensearch.index.store.niofs.CryptoNIOFSDirectory;

public class HybridCryptoDirectory extends CryptoNIOFSDirectory {
    private static final Logger LOGGER = LogManager.getLogger(CryptoNIOFSDirectory.class);

    private final LazyDecryptedCryptoMMapDirectory lazyDecryptedCryptoMMapDirectoryDelegate;
    private final EagerDecryptedCryptoMMapDirectory eagerDecryptedCryptoMMapDirectory;
    private final Set<String> nioExtensions;

    // File size thresholds for special files only
    private static final long MEDIUM_FILE_THRESHOLD = 10 * 1024 * 1024; // 10MB

    // Only these extensions get special routing - everything else goes to NIOFS
    private final Set<String> specialExtensions;

    public HybridCryptoDirectory(
        LockFactory lockFactory,
        LazyDecryptedCryptoMMapDirectory delegate,
        EagerDecryptedCryptoMMapDirectory eagerDecryptedCryptoMMapDirectory1,
        Provider provider,
        KeyIvResolver keyIvResolver,
        Set<String> nioExtensions
    )
        throws IOException {
        super(lockFactory, delegate.getDirectory(), provider, keyIvResolver);
        this.lazyDecryptedCryptoMMapDirectoryDelegate = delegate;
        this.eagerDecryptedCryptoMMapDirectory = eagerDecryptedCryptoMMapDirectory1;
        this.nioExtensions = nioExtensions;
        // Only these files get special treatment
        this.specialExtensions = Set.of("kdd", "kdi", "kdm", "tip", "tim", "tmd", "cfs");
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        String extension = FileSwitchDirectory.getExtension(name);

        // If not a special extension, always use NIOFS
        if (!specialExtensions.contains(extension)) {
            return super.openInput(name, context);
        }

        ensureOpen();
        ensureCanRead(name);

        // Special routing for key file types
        return routeSpecialFile(name, extension, context);
    }

    private IndexInput routeSpecialFile(String name, String extension, IOContext context) throws IOException {
        Path file = getDirectory().resolve(name);
        long fileSize = Files.size(file);

        // MERGE context: Always use NIOFS for sequential, one-time access
        if (context.context() == Context.MERGE) {
            LOGGER.info("Routing {} to NIOFS for merge operation", name);
            return super.openInput(name, context);
        }

        // FLUSH context: New segment creation - consider future access patterns
        if (context.context() == Context.FLUSH) {
            LOGGER.info("Routing for flush operation", name);

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

        if ((fileSize >= (2L << 20)) && (fileSize <= (8L << 20))) {
            return eagerDecryptedCryptoMMapDirectory.openInput(name, context);
        }

        // Route based on file type and access patterns
        switch (extension) {
            case "tip", "tmd" -> {
                // Term dictionary files: Random access to small blocks (~2KB)
                // Always use MMap for optimal performance regardless of size
                LOGGER.debug("Routing term file {} to MMap for random small block access", name);
                return lazyDecryptedCryptoMMapDirectoryDelegate.openInput(name, context);
            }
            case "kdm", "kdi" -> {
                // BKD tree metadata: Small file, infrequent access
                // Use MMap for simplicity
                LOGGER.debug("Routing KDM {} to MMap", name);
                return lazyDecryptedCryptoMMapDirectoryDelegate.openInput(name, context);
            }

            case "tim" -> {
                if (fileSize >= (32L << 20)) {
                    LOGGER.debug("Routing LARGE files {} for egar {}", name, fileSize / 1048576.0);
                    return eagerDecryptedCryptoMMapDirectory.openInput(name, context);
                }

                return lazyDecryptedCryptoMMapDirectoryDelegate.openInput(name, context);
            }

            case "kdd" -> {
                if (fileSize >= (32L << 20)) {
                    LOGGER.debug("Routing LARGE files {} for eagar {}", name, fileSize / 1048576.0);
                    return eagerDecryptedCryptoMMapDirectory.openInput(name, context);
                }

                return lazyDecryptedCryptoMMapDirectoryDelegate.openInput(name, context);
            }

            case "cfs" -> {
                if ((fileSize >= (2L << 20)) && (fileSize <= (16L << 20))) {
                    return eagerDecryptedCryptoMMapDirectory.openInput(name, context);
                }

                return lazyDecryptedCryptoMMapDirectoryDelegate.openInput(name, context);
            }

            default -> {
                if (useDelegate(name)) {
                    return lazyDecryptedCryptoMMapDirectoryDelegate.openInput(name, context);
                } else {
                    return super.openInput(name, context);
                }
            }
        }
    }

    private boolean useDelegate(String name) {
        String extension = FileSwitchDirectory.getExtension(name);

        if (name.endsWith(".tmp") || name.contains("segments_")) {
            return false;
        }

        // [cfe, tvd, fnm, nvm, write.lock, dii, pay, segments_N, pos, si, fdt, tvx, liv, dvm, fdx, vem]
        boolean result = extension == null || !nioExtensions.contains(extension);
        return result;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(super::close, lazyDecryptedCryptoMMapDirectoryDelegate);
    }
}

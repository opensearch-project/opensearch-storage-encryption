/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/
package org.opensearch.index.store.mmap;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.cipher.MemorySegmentDecryptor;
import org.opensearch.index.store.cipher.OpenSslNativeCipher;
import org.opensearch.index.store.iv.KeyIvResolver;

@SuppressWarnings("preview")
@SuppressForbidden(reason = "temporary bypass")
public final class EagerDecryptedCryptoMMapDirectory extends MMapDirectory {

    private static final Logger LOGGER = LogManager.getLogger(LazyDecryptedCryptoMMapDirectory.class);

    private final KeyIvResolver keyIvResolver;

    public EagerDecryptedCryptoMMapDirectory(Path path, Provider provider, KeyIvResolver keyIvResolver) throws IOException {
        super(path);
        this.keyIvResolver = keyIvResolver;
    }

    /**
     * Sets the preload predicate based on file extension list.
     *
     * @param preLoadExtensions extensions to preload (e.g., ["dvd", "tim",
     * "*"])
     * @throws IOException if preload configuration fails
     */
    public void setPreloadExtensions(Set<String> preLoadExtensions) throws IOException {
        if (!preLoadExtensions.isEmpty()) {
            this.setPreload(createPreloadPredicate(preLoadExtensions));
        }
    }

    private static BiPredicate<String, IOContext> createPreloadPredicate(Set<String> preLoadExtensions) {
        if (preLoadExtensions.contains("*")) {
            return MMapDirectory.ALL_FILES;
        }
        return (fileName, context) -> {
            int dotIndex = fileName.lastIndexOf('.');
            if (dotIndex > 0) {
                String ext = fileName.substring(dotIndex + 1);
                return preLoadExtensions.contains(ext);
            }
            return false;
        };
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        ensureCanRead(name);

        Path file = getDirectory().resolve(name);
        long size = Files.size(file);

        boolean confined = context == IOContext.READONCE;
        Arena arena = confined ? Arena.ofConfined() : Arena.ofShared();

        int chunkSizePower = 34;

        try {
            // Open the file using native open() call
            int fd = PanamaNativeAccess.openFile(file.toString());
            if (fd == -1) {
                throw new IOException("Failed to open file: " + file);
            }

            try {
                MemorySegment[] segments = mmapAndDecrypt(fd, size, arena, chunkSizePower, name, context);
                return MemorySegmentIndexInput
                    .newInstance("CryptoMemorySegmentIndexInput(path=\"" + file + "\")", arena, segments, size, chunkSizePower);
            } finally {
                PanamaNativeAccess.closeFile(fd);
            }

        } catch (Throwable t) {
            arena.close();
            throw new IOException("Failed to mmap/decrypt " + file, t);
        }
    }

    private MemorySegment[] mmapAndDecrypt(int fd, long size, Arena arena, int chunkSizePower, String name, IOContext context)
        throws Throwable {
        final long chunkSize = 1L << chunkSizePower;
        final int numSegments = (int) ((size + chunkSize - 1) >>> chunkSizePower);
        MemorySegment[] segments = new MemorySegment[numSegments];

        // Get madvise flags once - they don't change per segment
        int madviseFlags = LuceneIOContextMAdvise.getMAdviseFlags(context, name);

        long offset = 0;
        for (int i = 0; i < numSegments; i++) {
            long remaining = size - offset;
            long segmentSize = Math.min(chunkSize, remaining);

            MemorySegment mmapSegment = (MemorySegment) PanamaNativeAccess.MMAP
                .invoke(
                    MemorySegment.NULL,
                    segmentSize,
                    PanamaNativeAccess.PROT_READ | PanamaNativeAccess.PROT_WRITE,
                    PanamaNativeAccess.MAP_PRIVATE,
                    fd,
                    offset
                );
            if (mmapSegment.address() == 0 || mmapSegment.address() == -1) {
                throw new IOException("mmap failed at offset: " + offset);
            }

            try {
                if (mmapSegment.address() % PanamaNativeAccess.getPageSize() == 0) {
                    PanamaNativeAccess.madvise(mmapSegment.address(), segmentSize, PanamaNativeAccess.MADV_WILLNEED);
                }
            } catch (Throwable t) {
                LOGGER.warn("madvise failed for {} at context {} advise: {}", name, context, madviseFlags, t);
            }

            MemorySegment segment = MemorySegment.ofAddress(mmapSegment.address()).reinterpret(segmentSize, arena, null);

            decryptSegmentInPlaceParallel(arena, segment, offset);

            segments[i] = segment;
            offset += segmentSize;
        }

        return segments;
    }

    public void decryptSegmentInPlaceParallel(Arena arena, MemorySegment segment, long segmentOffsetInFile) throws Throwable {
        final long size = segment.byteSize();

        final int twoMB = 1 << 21; // 2 MiB
        final int fourMB = 1 << 22; // 4 MiB
        final int eightMB = 1 << 23; // 8 MiB
        final int sixteenMB = 1 << 24; // 16 MiB

        final byte[] key = this.keyIvResolver.getDataKey().getEncoded();
        final byte[] iv = this.keyIvResolver.getIvBytes();

        // Fast-path: no parallelism for ≤ 2 MiB
        if (size <= (4L << 20)) {
            MemorySegmentDecryptor.decryptSegment(segment, segmentOffsetInFile, key, iv, (int) size); // downcast is safe.
            return;
        }

        // Use Openssl for large block decrytion.
        final int chunkSize;
        if (size <= (8L << 20)) {
            chunkSize = twoMB;
        } else if (size <= (16L << 20)) {
            chunkSize = fourMB;
        } else if (size <= (32L << 20)) {
            chunkSize = fourMB;
        } else if (size <= (64L << 20)) {
            chunkSize = eightMB;
        } else {
            chunkSize = sixteenMB;
        }

        final int numChunks = (int) ((size + chunkSize - 1) / chunkSize);

        IntStream.range(0, numChunks).parallel().forEach(i -> {
            long offset = (long) i * chunkSize;
            long length = Math.min(chunkSize, size - offset);
            long fileOffset = segmentOffsetInFile + offset;
            long addr = segment.address() + offset;

            try {
                OpenSslNativeCipher.decryptInPlace(addr, length, key, iv, fileOffset);
            } catch (Throwable t) {
                throw new RuntimeException("Decryption failed at offset: " + fileOffset, t);
            }
        });
    }
}

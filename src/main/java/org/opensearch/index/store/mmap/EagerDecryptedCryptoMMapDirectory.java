/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.mmap;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
import org.opensearch.index.store.cipher.OpenSslNativeCipher;
import org.opensearch.index.store.key.HkdfKeyDerivation;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.footer.EncryptionFooter;
import org.opensearch.index.store.footer.EncryptionMetadataTrailer;
import org.opensearch.index.store.cipher.EncryptionAlgorithm;
import java.nio.ByteBuffer;

@SuppressWarnings("preview")
@SuppressForbidden(reason = "temporary bypass")
public final class EagerDecryptedCryptoMMapDirectory extends MMapDirectory {
    private static final Logger LOGGER = LogManager.getLogger(EagerDecryptedCryptoMMapDirectory.class);

    private final KeyResolver keyResolver;

    public EagerDecryptedCryptoMMapDirectory(Path path, Provider provider, KeyResolver keyResolver) throws IOException {
        super(path);
        this.keyResolver = keyResolver;
    }
    
    /**
     * Footer information extracted from encrypted file
     */
    private static class FooterInfo {
        final byte[] messageId;
        final long frameSize;
        final EncryptionAlgorithm algorithm;
        final long contentLength; // File size excluding footer
        
        FooterInfo(byte[] messageId, long frameSize, EncryptionAlgorithm algorithm, long contentLength) {
            this.messageId = messageId;
            this.frameSize = frameSize;
            this.algorithm = algorithm;
            this.contentLength = contentLength;
        }
    }
    
    /**
     * Read footer from FileChannel and return footer information
     */
    private FooterInfo readFooterInfo(FileChannel channel, byte[] directoryKey) throws IOException {
        long fileSize = channel.size();
        if (fileSize < EncryptionMetadataTrailer.MIN_FOOTER_SIZE) {
            throw new IOException("File too small to contain encryption footer");
        }
        
        // Read minimum footer to get actual length
        ByteBuffer minBuffer = ByteBuffer.allocate(EncryptionMetadataTrailer.MIN_FOOTER_SIZE);
        channel.read(minBuffer, fileSize - EncryptionMetadataTrailer.MIN_FOOTER_SIZE);
        
        int footerLength = EncryptionFooter.calculateFooterLength(minBuffer.array());
        
        // Read complete footer
        ByteBuffer footerBuffer = ByteBuffer.allocate(footerLength);
        int bytesRead = channel.read(footerBuffer, fileSize - footerLength);
        
        if (bytesRead != footerLength) {
            throw new IOException("Failed to read complete footer");
        }
        
        // Deserialize footer using directory key
        EncryptionFooter footer = EncryptionFooter.deserialize(footerBuffer.array(), directoryKey);
        
        return new FooterInfo(
            footer.getMessageId(),
            footer.getFrameSize(),
            EncryptionAlgorithm.fromId(footer.getAlgorithmId()),
            fileSize - footerLength
        );
    }
    
//    /**
//     * Check if file should be encrypted (excludes segments, si files, etc.)
//     */
//    private boolean isNonEncryptedFile(String fileName) {
//        return fileName.contains("segments_") ||
//               fileName.endsWith(".si") ||
//               fileName.equals("keyfile") ||
//               fileName.endsWith(".lock");
//    }

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
        
        // Skip footer processing for non-encrypted files
        if (name.contains("segments_") || name.endsWith(".si")) {
            return super.openInput(name, context);
        }
        
        boolean confined = context == IOContext.READONCE;
        Arena arena = confined ? Arena.ofConfined() : Arena.ofShared();

        // TODO: Make it a constant.
        int chunkSizePower = 34;
        boolean success = false;

        try (var fc = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            // Read footer information
            byte[] directoryKey = keyResolver.getDataKey().getEncoded();
            FooterInfo footerInfo = readFooterInfo(fc, directoryKey);
            
            // Map and decrypt only the content (excluding footer)
            MemorySegment[] segments = mmapAndDecrypt(fc, footerInfo.contentLength, arena, chunkSizePower, name, context, footerInfo);

            final IndexInput in = MemorySegmentIndexInput
                .newInstance("CryptoMemorySegmentIndexInput(path=\"" + file + "\")", arena, segments, footerInfo.contentLength, chunkSizePower);
            success = true;
            return in;
        } catch (Throwable ex) {
            throw new IOException("Failed to mmap/decrypt " + file, ex);
        } finally {
            if (success == false) {
                arena.close();
            }
        }
    }

    private MemorySegment[] mmapAndDecrypt(FileChannel fc, long size, Arena arena, int chunkSizePower, String name, IOContext context, FooterInfo footerInfo)
        throws Throwable {
        final long chunkSize = 1L << chunkSizePower;
        final int numSegments = (int) ((size + chunkSize - 1) >>> chunkSizePower);

        final MemorySegment[] segments = new MemorySegment[numSegments];

        long offset = 0;
        for (int i = 0; i < numSegments; i++) {
            long remaining = size - offset;
            long segmentSize = Math.min(chunkSize, remaining);

            MemorySegment mmapSegment = fc.map(MapMode.PRIVATE, offset, segmentSize, arena);

            if (mmapSegment.address() == 0 || mmapSegment.address() == -1) {
                throw new IOException("mmap failed at offset: " + offset);
            }

            try {
                if (mmapSegment.address() % PanamaNativeAccess.getPageSize() == 0) {
                    PanamaNativeAccess.madvise(mmapSegment.address(), segmentSize, PanamaNativeAccess.MADV_WILLNEED);
                }
            } catch (Throwable t) {
                LOGGER.warn("madvise MADV_WILLNEED failed for file {} with IOcontext {}", name, context, t);
            }

            decryptSegment(arena, mmapSegment, offset, footerInfo);

            segments[i] = mmapSegment;
            offset += segmentSize;
        }

        return segments;
    }

    public void decryptSegment(Arena arena, MemorySegment segment, long segmentOffsetInFile, FooterInfo footerInfo) throws Throwable {
        final long size = segment.byteSize();

        final int twoMB = 1 << 21; // 2 MiB
        final int fourMB = 1 << 22; // 4 MiB
        final int eightMB = 1 << 23; // 8 MiB
        final int sixteenMB = 1 << 24; // 16 MiB

        // Use frame-based decryption
        final byte[] directoryKey = this.keyResolver.getDataKey().getEncoded();
        final byte[] fileKey = HkdfKeyDerivation.deriveAesKey(directoryKey, footerInfo.messageId, "file-encryption");

        // Fast-path: no parallelism for ≤ 4 MiB
        if (size <= (4L << 20)) {
            long start = System.nanoTime();

            OpenSslNativeCipher.decryptInPlaceFrameBased(arena, segment.address(), size, fileKey, directoryKey, footerInfo.messageId, footerInfo.frameSize, segmentOffsetInFile);

            long end = System.nanoTime();
            long durationMs = (end - start) / 1_000_000;
            LOGGER.debug("Eager frame-based decryption of {} MiB at offset {} took {} ms", size / 1048576.0, segmentOffsetInFile, durationMs);
            return;
        }

        // Use Openssl for large block decryption with frame support
        final int chunkSize;
        if (size <= (8L << 20)) {
            chunkSize = twoMB;
        } else if (size <= (32L << 20)) {
            chunkSize = fourMB;
        } else if (size <= (64L << 20)) {
            chunkSize = eightMB;
        } else {
            chunkSize = sixteenMB;
        }

        final int numChunks = (int) ((size + chunkSize - 1) / chunkSize);

        // parallel decryptions with frame-based approach
        IntStream.range(0, numChunks).parallel().forEach(i -> {
            long offset = (long) i * chunkSize;
            long length = Math.min(chunkSize, size - offset);
            long fileOffset = segmentOffsetInFile + offset;
            long addr = segment.address() + offset;

            try {
                OpenSslNativeCipher.decryptInPlaceFrameBased(addr, length, fileKey, directoryKey, footerInfo.messageId, footerInfo.frameSize, fileOffset);
            } catch (Throwable t) {
                throw new RuntimeException("Frame-based decryption failed at offset: " + fileOffset, t);
            }
        });
    }
}

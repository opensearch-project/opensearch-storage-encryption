/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_loader;

import static org.opensearch.index.store.block_loader.DirectIOReaderUtil.directIOReadAligned;
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_MASK;
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE;
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.cipher.MemorySegmentDecryptor;
import org.opensearch.index.store.footer.EncryptionFooter;
import org.opensearch.index.store.footer.EncryptionMetadataTrailer;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.pool.MemorySegmentPool;
import org.opensearch.index.store.pool.Pool;

@SuppressWarnings("preview")
public class CryptoDirectIOBlockLoader implements BlockLoader<MemorySegmentPool.SegmentHandle> {
    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectIOBlockLoader.class);

    private final Pool<MemorySegmentPool.SegmentHandle> segmentPool;
    private final KeyResolver keyResolver;

    public CryptoDirectIOBlockLoader(Pool<MemorySegmentPool.SegmentHandle> segmentPool, KeyResolver keyResolver) {
        this.segmentPool = segmentPool;
        this.keyResolver = keyResolver;
    }

    @Override
    public MemorySegmentPool.SegmentHandle[] load(Path filePath, long startOffset, long blockCount) throws Exception {
        if (!Files.exists(filePath)) {
            throw new NoSuchFileException(filePath.toString());
        }

        if ((startOffset & CACHE_BLOCK_MASK) != 0) {
            throw new IllegalArgumentException("startOffset must be block-aligned: " + startOffset);
        }

        if (blockCount <= 0) {
            throw new IllegalArgumentException("blockCount must be positive: " + blockCount);
        }

        MemorySegmentPool.SegmentHandle[] result = new MemorySegmentPool.SegmentHandle[(int) blockCount];
        long readLength = blockCount << CACHE_BLOCK_SIZE_POWER;

        try (
            Arena arena = Arena.ofConfined();
            FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ, DirectIOReaderUtil.getDirectOpenOption())
        ) {
            MemorySegment readBytes = directIOReadAligned(channel, startOffset, readLength, arena);
            long bytesRead = readBytes.byteSize();


            byte[] messageId = readMessageIdFromFooter(filePath);
            byte[] directoryKey = keyResolver.getDataKey().getEncoded();
            byte[] fileKey = org.opensearch.index.store.key.HkdfKeyDerivation.deriveAesKey(
                    directoryKey, messageId, "file-encryption");

            // Use frame-based decryption with derived file key
            MemorySegmentDecryptor.decryptInPlaceFrameBased(
                    readBytes.address(),
                    readBytes.byteSize(),
                    fileKey,                                    // Derived file key (matches write path)
                    directoryKey,                               // Directory key for IV computation
                    messageId,                                  // Message ID from footer
                    org.opensearch.index.store.footer.EncryptionMetadataTrailer.DEFAULT_FRAME_SIZE, // Frame size
                    startOffset,                                 // File offset
                    filePath
            );

            if (bytesRead == 0) {
                throw new RuntimeException("EOF or empty read at offset " + startOffset);
            }

            int blockIndex = 0;
            long bytesCopied = 0;

            try {
                while (blockIndex < blockCount && bytesCopied < bytesRead) {
                    MemorySegmentPool.SegmentHandle handle = segmentPool.tryAcquire(10, TimeUnit.MILLISECONDS);
                    if (handle == null) {
                        throw new RuntimeException("Failed to acquire a block");
                    }

                    MemorySegment pooled = handle.segment();

                    int remaining = (int) (bytesRead - bytesCopied);
                    int toCopy = Math.min(CACHE_BLOCK_SIZE, remaining);

                    if (toCopy > 0) {
                        MemorySegment.copy(readBytes, bytesCopied, pooled, 0, toCopy);
                    }

                    result[blockIndex++] = handle;  // Store the handle, not the segment
                    bytesCopied += toCopy;
                }

            } catch (InterruptedException e) {
                releaseHandles(result, blockIndex);
                throw new RuntimeException("Failed to load blocks", e);
            }

            return result;

        } catch (NoSuchFileException e) {
            throw e;
        } catch (Exception e) {
            LOGGER.error("Bulk read failed: path={} offset={} length={} err={}", filePath, startOffset, readLength, e.toString());
            throw e;
        }
    }

    private void releaseHandles(MemorySegmentPool.SegmentHandle[] handles, int upTo) {
        for (int i = 0; i < upTo; i++) {
            if (handles[i] != null) {
                handles[i].release();  // Release back to correct tier
            }
        }
    }

    private byte[] readMessageIdFromFooter(Path filePath) throws IOException {
        // Use separate random access FileChannel to avoid position conflicts
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            if (fileSize < EncryptionMetadataTrailer.MIN_FOOTER_SIZE) {
                throw new IOException("File too small to contain footer: " + filePath);
            }

            // Read minimum footer to check OSEF magic bytes
            ByteBuffer minBuffer = ByteBuffer.allocate(EncryptionMetadataTrailer.MIN_FOOTER_SIZE);
            channel.read(minBuffer, fileSize - EncryptionMetadataTrailer.MIN_FOOTER_SIZE);
            byte[] minFooterBytes = minBuffer.array();

            // Check if this is an OSEF file
            if (!isValidOSEFFile(minFooterBytes)) {
                // Not an OSEF file - fall back to legacy decryption
                throw new IOException("Not an OSEF file, using legacy IV");
            }

            int footerLength = EncryptionFooter.calculateFooterLength(minFooterBytes);

            // Read complete footer
            ByteBuffer footerBuffer = ByteBuffer.allocate(footerLength);
            channel.read(footerBuffer, fileSize - footerLength);

            EncryptionFooter footer = EncryptionFooter.readFromChannel(filePath, channel, keyResolver.getDataKey().getEncoded());
            return footer.getMessageId();
        }
    }

    /**
     * Check if file has valid OSEF magic bytes
     */
    private boolean isValidOSEFFile(byte[] minFooterBytes) {
        int magicOffset = minFooterBytes.length - EncryptionMetadataTrailer.MAGIC.length;
        for (int i = 0; i < EncryptionMetadataTrailer.MAGIC.length; i++) {
            if (minFooterBytes[magicOffset + i] != EncryptionMetadataTrailer.MAGIC[i]) {
                return false;
            }
        }
        return true;
    }
}

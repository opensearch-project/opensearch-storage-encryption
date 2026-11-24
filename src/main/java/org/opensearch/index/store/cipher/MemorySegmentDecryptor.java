/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.cipher;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.security.Provider;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Hybrid cipher implementation for in-place memory segment decryption using AES-CTR mode.
 *
 * <p>This utility class provides efficient decryption capabilities for memory segments using the Java Cipher API
 * with ByteBuffer integration. It supports chunked processing to handle large memory segments efficiently while
 * maintaining optimal JIT performance characteristics.
 *
 * <p>Key features include:
 * <ul>
 * <li><strong>In-place decryption:</strong> Modifies memory segments directly without requiring additional allocation</li>
 * <li><strong>Chunked processing:</strong> Processes large segments in configurable chunks to optimize memory usage</li>
 * <li><strong>Offset-aware IV computation:</strong> Handles file offset-based initialization vectors for AES-CTR mode</li>
 * <li><strong>Thread-safe operation:</strong> Uses thread-local cipher pools for concurrent access</li>
 * <li><strong>Multiple interfaces:</strong> Supports both raw memory addresses and MemorySegment objects</li>
 * </ul>
 *
 * <p>The class uses AES-CTR mode decryption with support for non-block-aligned file offsets by applying
 * appropriate cipher state advancement using zero padding.
 *
 * @opensearch.internal
 */
@SuppressWarnings("preview")
public class MemorySegmentDecryptor {

    private static final int DEFAULT_MAX_CHUNK_SIZE = 16_384;

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private MemorySegmentDecryptor() {
        // Utility class - no instantiation
    }

    /**
     * Performs in-place AES-CTR decryption on a memory region specified by address within an arena scope.
     *
     * <p>This method decrypts the specified memory region directly, modifying the contents in place.
     * The decryption uses AES-CTR mode with proper IV computation based on the file offset to maintain
     * cryptographic correctness for position-dependent encryption schemes.
     *
     * <p>The method handles non-block-aligned file offsets by advancing the cipher state appropriately
     * using zero padding, ensuring correct decryption regardless of the starting position within the file.
     *
     * @param arena the memory arena that manages the lifecycle of the memory segment
     * @param addr the starting memory address of the encrypted data
     * @param length the number of bytes to decrypt
     * @param key the AES decryption key bytes
     * @param iv the initialization vector for AES-CTR mode
     * @param fileOffset the offset within the file where this data starts, used for IV computation
     * @param provider the JCE provider used to obtain the pooled CTR cipher
     * @throws Exception if decryption fails due to cryptographic errors or memory access issues
     */
    public static void decryptInPlace(Arena arena, long addr, long length, byte[] key, byte[] iv, long fileOffset, Provider provider)
        throws Exception {
        Cipher cipher = AesCipherFactory.getPooledCipher(provider);
        SecretKeySpec keySpec = new SecretKeySpec(key, AesCipherFactory.ALGORITHM);
        byte[] ivCopy = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(iv, fileOffset);

        cipher.init(Cipher.DECRYPT_MODE, keySpec, new IvParameterSpec(ivCopy));

        MemorySegment segment = MemorySegment.ofAddress(addr).reinterpret(length, arena, null);
        decryptByteBuffer(cipher, segment.asByteBuffer(), fileOffset);
    }

    /**
     * Performs in-place AES-CTR decryption on a memory region specified by address using global scope.
     *
     * <p>This method decrypts the specified memory region directly, modifying the contents in place.
     * Unlike the arena-scoped version, this method uses global memory segment scope and should be used
     * with caution to ensure proper memory management.
     *
     * <p>The decryption process handles file offset alignment and uses chunked processing for large
     * segments to optimize performance and memory usage patterns.
     *
     * @param addr the starting memory address of the encrypted data
     * @param length the number of bytes to decrypt
     * @param key the AES decryption key bytes
     * @param iv the initialization vector for AES-CTR mode
     * @param fileOffset the offset within the file where this data starts, used for IV computation
     * @param provider the JCE provider used to obtain the pooled CTR cipher
     * @throws Exception if decryption fails due to cryptographic errors or memory access issues
     */
    public static void decryptInPlace(long addr, long length, byte[] key, byte[] iv, long fileOffset, Provider provider) throws Exception {
        Cipher cipher = AesCipherFactory.getPooledCipher(provider);
        SecretKeySpec keySpec = new SecretKeySpec(key, AesCipherFactory.ALGORITHM);
        byte[] ivCopy = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(iv, fileOffset);

        cipher.init(Cipher.DECRYPT_MODE, keySpec, new IvParameterSpec(ivCopy));

        MemorySegment segment = MemorySegment.ofAddress(addr).reinterpret(length);
        decryptByteBuffer(cipher, segment.asByteBuffer(), fileOffset);
    }

    /**
     * Performs in-place AES-CTR decryption on a provided MemorySegment.
     *
     * <p>This method provides a high-level interface for decrypting MemorySegment objects directly.
     * The segment's contents are modified in place using efficient ByteBuffer operations with
     * chunked processing for optimal performance.
     *
     * <p>The method properly handles the cipher initialization with offset-aware IV computation
     * and applies necessary cipher state advancement for non-aligned file positions.
     *
     * @param segment the memory segment containing encrypted data to decrypt in place
     * @param fileOffset the offset within the file where this segment's data starts, used for IV computation
     * @param key the AES decryption key bytes
     * @param iv the initialization vector for AES-CTR mode
     * @param segmentSize the size of the segment to decrypt in bytes
     * @param provider the JCE provider used to obtain the pooled CTR cipher
     * @throws Exception if decryption fails due to cryptographic errors or segment access issues
     */
    public static void decryptSegment(MemorySegment segment, long fileOffset, byte[] key, byte[] iv, int segmentSize, Provider provider)
        throws Exception {
        Cipher cipher = AesCipherFactory.getPooledCipher(provider);
        SecretKeySpec keySpec = new SecretKeySpec(key, AesCipherFactory.ALGORITHM);
        byte[] ivCopy = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(iv, fileOffset);

        cipher.init(Cipher.DECRYPT_MODE, keySpec, new IvParameterSpec(ivCopy));
        decryptByteBuffer(cipher, segment.asByteBuffer(), fileOffset);
    }

    /**
     * Decrypts {@code buffer} in place. Discards a CTR skip prefix that a buffering
     * provider (Bouncy Castle) may emit with later ciphertext instead of during skip.
     */
    private static void decryptByteBuffer(Cipher cipher, ByteBuffer buffer, long fileOffset) throws Exception {
        int pendingSkip = AesCipherFactory.advanceCtrSkip(cipher, AesCipherFactory.skipBytesForOffset(fileOffset));
        final int chunkSize = Math.min(DEFAULT_MAX_CHUNK_SIZE, Math.max(buffer.capacity(), 1));
        byte[] chunk = new byte[chunkSize];
        byte[] decryptedChunk = new byte[chunkSize + AesCipherFactory.AES_BLOCK_SIZE_BYTES];

        int readPos = 0;
        int writePos = 0;
        while (readPos < buffer.capacity()) {
            int size = Math.min(chunkSize, buffer.capacity() - readPos);
            buffer.position(readPos);
            buffer.get(chunk, 0, size);

            int decryptedLength = cipher.update(chunk, 0, size, decryptedChunk, 0);
            writePos += copyDiscardingSkip(buffer, decryptedChunk, decryptedLength, writePos, pendingSkip);
            pendingSkip = remainingSkipAfterCopy(pendingSkip, decryptedLength);
            readPos += size;
        }

        int finalLength = cipher.doFinal(new byte[0], 0, 0, decryptedChunk, 0);
        copyDiscardingSkip(buffer, decryptedChunk, finalLength, writePos, pendingSkip);
    }

    private static int remainingSkipAfterCopy(int pendingSkip, int decryptedLength) {
        if (pendingSkip <= 0 || decryptedLength <= 0) {
            return pendingSkip;
        }
        return pendingSkip - Math.min(pendingSkip, decryptedLength);
    }

    private static int copyDiscardingSkip(ByteBuffer buffer, byte[] decrypted, int decryptedLength, int writePos, int pendingSkip) {
        if (decryptedLength <= 0) {
            return 0;
        }
        int from = 0;
        if (pendingSkip > 0) {
            int discard = Math.min(pendingSkip, decryptedLength);
            from += discard;
            decryptedLength -= discard;
        }
        if (decryptedLength <= 0) {
            return 0;
        }
        buffer.position(writePos);
        buffer.put(decrypted, from, decryptedLength);
        return decryptedLength;
    }

    /**
     * Frame-based decryption for large files with frame boundary handling
     */
    public static void decryptInPlaceFrameBased(
        long addr,
        long length,
        byte[] fileKey,
        byte[] directoryKey,
        byte[] messageId,
        long frameSize,
        long fileOffset,
        String filePath,
        EncryptionMetadataCache cache,
        Provider provider
    ) throws Exception {

        // Fast path: single frame
        long frameNumber = fileOffset / frameSize;
        long offsetWithinFrame = fileOffset % frameSize;

        if (offsetWithinFrame + length <= frameSize) {
            byte[] frameIV = AesCipherFactory
                .computeFrameIV(directoryKey, messageId, frameNumber, offsetWithinFrame, filePath, cache, provider);
            decryptInPlace(addr, length, fileKey, frameIV, offsetWithinFrame, provider);
            return;
        }

        // Slow path: multi-frame
        long remaining = length;
        long currentOffset = fileOffset;
        long bufferOffset = 0;

        while (remaining > 0) {
            frameNumber = currentOffset / frameSize;
            long frameStart = frameNumber * frameSize;
            long frameEnd = frameStart + frameSize;
            long bytesInFrame = Math.min(remaining, frameEnd - currentOffset);

            byte[] frameIV = AesCipherFactory
                .computeFrameIV(directoryKey, messageId, frameNumber, currentOffset - frameStart, filePath, cache, provider);

            decryptInPlace(addr + bufferOffset, bytesInFrame, fileKey, frameIV, currentOffset, provider);

            currentOffset += bytesInFrame;
            bufferOffset += bytesInFrame;
            remaining -= bytesInFrame;
        }
    }
}

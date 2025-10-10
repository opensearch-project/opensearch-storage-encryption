/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.cipher;

import static org.opensearch.index.store.cipher.AesCipherFactory.CIPHER_POOL;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

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

    private static final byte[] ZERO_SKIP = new byte[AesCipherFactory.AES_BLOCK_SIZE_BYTES];
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
     * @throws Exception if decryption fails due to cryptographic errors or memory access issues
     */
    public static void decryptInPlace(Arena arena, long addr, long length, byte[] key, byte[] iv, long fileOffset) throws Exception {
        // Get thread-local cipher
        Cipher cipher = CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(key, AesCipherFactory.ALGORITHM);
        byte[] ivCopy = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(iv, fileOffset);

        cipher.init(Cipher.DECRYPT_MODE, keySpec, new IvParameterSpec(ivCopy));

        if (fileOffset % AesCipherFactory.AES_BLOCK_SIZE_BYTES > 0) {
            cipher.update(ZERO_SKIP, 0, (int) (fileOffset % AesCipherFactory.AES_BLOCK_SIZE_BYTES));
        }

        MemorySegment segment = MemorySegment.ofAddress(addr).reinterpret(length, arena, null);
        ByteBuffer buffer = segment.asByteBuffer();

        final int CHUNK_SIZE = Math.min(DEFAULT_MAX_CHUNK_SIZE, (int) length); // typecast is safe.
        byte[] chunk = new byte[CHUNK_SIZE];
        byte[] decryptedChunk = new byte[CHUNK_SIZE];

        int position = 0;
        while (position < buffer.capacity()) {
            int size = Math.min(CHUNK_SIZE, buffer.capacity() - position);

            buffer.position(position);
            buffer.get(chunk, 0, size);

            int decryptedLength = cipher.update(chunk, 0, size, decryptedChunk, 0);
            if (decryptedLength > 0) {
                buffer.position(position);
                buffer.put(decryptedChunk, 0, decryptedLength);
            }

            position += size;
        }

        int finalLength = cipher.doFinal(new byte[0], 0, 0, decryptedChunk, 0);
        if (finalLength > 0) {
            buffer.position(position - finalLength);
            buffer.put(decryptedChunk, 0, finalLength);
        }
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
     * @throws Exception if decryption fails due to cryptographic errors or memory access issues
     */
    public static void decryptInPlace(long addr, long length, byte[] key, byte[] iv, long fileOffset) throws Exception {
        Cipher cipher = CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(key, AesCipherFactory.ALGORITHM);
        byte[] ivCopy = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(iv, fileOffset);

        cipher.init(Cipher.DECRYPT_MODE, keySpec, new IvParameterSpec(ivCopy));

        if (fileOffset % AesCipherFactory.AES_BLOCK_SIZE_BYTES > 0) {
            cipher.update(ZERO_SKIP, 0, (int) (fileOffset % AesCipherFactory.AES_BLOCK_SIZE_BYTES));
        }

        MemorySegment segment = MemorySegment.ofAddress(addr).reinterpret(length);
        ByteBuffer buffer = segment.asByteBuffer();

        final int CHUNK_SIZE = Math.min(DEFAULT_MAX_CHUNK_SIZE, (int) length); // typecast is safe.
        byte[] chunk = new byte[CHUNK_SIZE];
        byte[] decryptedChunk = new byte[CHUNK_SIZE];

        int position = 0;
        while (position < buffer.capacity()) {
            int size = Math.min(CHUNK_SIZE, buffer.capacity() - position);

            buffer.position(position);
            buffer.get(chunk, 0, size);

            int decryptedLength = cipher.update(chunk, 0, size, decryptedChunk, 0);
            if (decryptedLength > 0) {
                buffer.position(position);
                buffer.put(decryptedChunk, 0, decryptedLength);
            }

            position += size;
        }

        int finalLength = cipher.doFinal(new byte[0], 0, 0, decryptedChunk, 0);
        if (finalLength > 0) {
            buffer.position(position - finalLength);
            buffer.put(decryptedChunk, 0, finalLength);
        }
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
     * @throws Exception if decryption fails due to cryptographic errors or segment access issues
     */
    public static void decryptSegment(MemorySegment segment, long fileOffset, byte[] key, byte[] iv, int segmentSize) throws Exception {
        Cipher cipher = CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(key, AesCipherFactory.ALGORITHM);
        byte[] ivCopy = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(iv, fileOffset);

        cipher.init(Cipher.DECRYPT_MODE, keySpec, new IvParameterSpec(ivCopy));

        if (fileOffset % AesCipherFactory.AES_BLOCK_SIZE_BYTES > 0) {
            cipher.update(ZERO_SKIP, 0, (int) (fileOffset % AesCipherFactory.AES_BLOCK_SIZE_BYTES));
        }

        ByteBuffer buffer = segment.asByteBuffer();
        final int CHUNK_SIZE = Math.min(DEFAULT_MAX_CHUNK_SIZE, segmentSize);
        byte[] chunk = new byte[CHUNK_SIZE];
        byte[] decryptedChunk = new byte[CHUNK_SIZE];

        int position = 0;
        while (position < buffer.capacity()) {
            int size = Math.min(CHUNK_SIZE, buffer.capacity() - position);

            buffer.position(position);
            buffer.get(chunk, 0, size);

            int decryptedLength = cipher.update(chunk, 0, size, decryptedChunk, 0);
            if (decryptedLength > 0) {
                buffer.position(position);
                buffer.put(decryptedChunk, 0, decryptedLength);
            }

            position += size;
        }

    }
}

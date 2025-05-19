/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.cipher;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Hybrid cipher implementation that can use either:
 * 1. Native OpenSSL via Panama (for large operations)
 * 2. Java Cipher API via ByteBuffer (for small operations, better JIT optimization)
 */

@SuppressWarnings("preview")
public class MemorySegmentDecryptor {

    private static final ThreadLocal<Cipher> CIPHER_POOL = ThreadLocal.withInitial(() -> {
        try {
            return Cipher.getInstance("AES/CTR/NoPadding", "SunJCE");
        } catch (NoSuchAlgorithmException | NoSuchProviderException | NoSuchPaddingException e) {
            throw new RuntimeException(e);
        }
    });

    private MemorySegmentDecryptor() {

    }

    public static void decryptInPlace(long addr, long length, byte[] key, byte[] iv, long fileOffset) throws Exception {
        // Get thread-local cipher
        Cipher cipher = CIPHER_POOL.get();

        // Initialize cipher for this position (your proven approach)
        SecretKeySpec keySpec = new SecretKeySpec(key, "AES");
        byte[] ivCopy = Arrays.copyOf(iv, iv.length);

        int blockOffset = (int) (fileOffset / AesCipherFactory.AES_BLOCK_SIZE_BYTES);
        for (int i = AesCipherFactory.IV_ARRAY_LENGTH - 1; i >= AesCipherFactory.IV_ARRAY_LENGTH
            - AesCipherFactory.COUNTER_SIZE_BYTES; i--) {
            ivCopy[i] = (byte) blockOffset;
            blockOffset >>>= Byte.SIZE;
        }

        cipher.init(Cipher.DECRYPT_MODE, keySpec, new IvParameterSpec(ivCopy));

        // Skip partial block offset if needed
        int bytesToSkip = (int) (fileOffset % AesCipherFactory.AES_BLOCK_SIZE_BYTES);
        if (bytesToSkip > 0) {
            cipher.update(new byte[bytesToSkip]);
        }

        // Create memory segment and ByteBuffer
        MemorySegment segment = MemorySegment.ofAddress(addr).reinterpret(length);
        ByteBuffer buffer = segment.asByteBuffer();

        // Use your chunked decryption approach
        final int CHUNK_SIZE = Math.min(8192, (int) length); // typecast is safe.
        byte[] chunk = new byte[CHUNK_SIZE];

        int position = 0;
        while (position < buffer.capacity()) {
            int size = Math.min(CHUNK_SIZE, buffer.capacity() - position);
            buffer.position(position);
            buffer.get(chunk, 0, size);

            byte[] decrypted;
            if (position + size >= buffer.capacity()) {
                // Last chunk
                decrypted = cipher.doFinal(chunk, 0, size);
            } else {
                decrypted = cipher.update(chunk, 0, size);
            }

            if (decrypted != null) {
                buffer.position(position);
                buffer.put(decrypted);
            }

            position += size;
        }
    }

    public static void decryptSegment(MemorySegment segment, long offset, byte[] key, byte[] iv, int segmentSize) throws Exception {
        Cipher cipher = CIPHER_POOL.get();

        // Initialize cipher for this position (your proven approach)
        SecretKeySpec keySpec = new SecretKeySpec(key, "AES");
        byte[] ivCopy = Arrays.copyOf(iv, iv.length);

        int blockOffset = (int) (offset / AesCipherFactory.AES_BLOCK_SIZE_BYTES);
        for (int i = AesCipherFactory.IV_ARRAY_LENGTH - 1; i >= AesCipherFactory.IV_ARRAY_LENGTH
            - AesCipherFactory.COUNTER_SIZE_BYTES; i--) {
            ivCopy[i] = (byte) blockOffset;
            blockOffset >>>= Byte.SIZE;
        }

        cipher.init(Cipher.DECRYPT_MODE, keySpec, new IvParameterSpec(ivCopy));

        // Process the data in smaller chunks to avoid OOM
        ByteBuffer buffer = segment.asByteBuffer();

        final int CHUNK_SIZE = Math.min(8192, segmentSize);

        byte[] chunk = new byte[CHUNK_SIZE];

        int position = 0;
        while (position < buffer.capacity()) {
            int size = Math.min(CHUNK_SIZE, buffer.capacity() - position);
            buffer.position(position);
            buffer.get(chunk, 0, size);

            byte[] decrypted;
            if (position + size >= buffer.capacity()) {
                // Last chunk
                decrypted = cipher.doFinal(chunk, 0, size);
            } else {
                decrypted = cipher.update(chunk, 0, size);
            }

            if (decrypted != null) {
                buffer.position(position);
                buffer.put(decrypted);
            }

            position += size;
        }
    }
}

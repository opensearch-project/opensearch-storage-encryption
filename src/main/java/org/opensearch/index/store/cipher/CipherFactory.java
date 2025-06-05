/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.cipher;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;

/**
 * Factory utility for creating and initializing Cipher instances
 *
 * This class is tailored for symmetric encryption modes like AES-CTR,
 * where a block counter is appended to the IV.
 *
 * @opensearch.internal
 */
public class CipherFactory {

    /** AES block size in bytes. Required for counter calculations. */
    public static final int AES_BLOCK_SIZE_BYTES = 16;

    /** Number of bytes used for the counter in the IV (last 4 bytes). */
    public static final int COUNTER_SIZE_BYTES = 4;

    /** Total IV array length (typically 16 bytes for AES). */
    public static final int IV_ARRAY_LENGTH = 16;

    /** Static buffer to avoid allocating dummy padding buffer on every call. */
    private static final byte[] SKIP_BUFFER = new byte[AES_BLOCK_SIZE_BYTES];

    /**
     * Returns a new Cipher instance configured for AES/CTR/NoPadding using the given provider.
     *
     * @param provider The JCE provider to use (e.g., SunJCE, BouncyCastle)
     * @return A configured {@link Cipher} instance
     */
    public static Cipher getCipher(Provider provider) {
        try {
            return Cipher.getInstance("AES/CTR/NoPadding", provider);
        } catch (NoSuchPaddingException | NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to get cipher instance", e);
        }
    }

    /**
     * Initializes a cipher for encryption or decryption, using an IV adjusted for the given position.
     * The last 4 bytes of the IV are treated as a counter, and are adjusted to reflect the block offset.
     * This allows for seeking into an encrypted stream without re-processing prior blocks.
     *
     * @param cipher The cipher instance to initialize
     * @param key The symmetric key (e.g., AES key)
     * @param iv The base IV, typically 16 bytes long
     * @param opmode Cipher.ENCRYPT_MODE or Cipher.DECRYPT_MODE
     * @param newPosition The position in the stream to begin processing from
     */
    public static void initCipher(Cipher cipher, Key key, byte[] iv, int opmode, long newPosition) {
        try {
            // Fast-copy IV (faster than Arrays.copyOf)
            byte[] ivCopy = new byte[IV_ARRAY_LENGTH];
            System.arraycopy(iv, 0, ivCopy, 0, IV_ARRAY_LENGTH);

            // Compute block-aligned counter
            int counter = (int) (newPosition / AES_BLOCK_SIZE_BYTES);
            int pos = IV_ARRAY_LENGTH - COUNTER_SIZE_BYTES;
            ivCopy[pos] = (byte) (counter >>> 24);
            ivCopy[pos + 1] = (byte) (counter >>> 16);
            ivCopy[pos + 2] = (byte) (counter >>> 8);
            ivCopy[pos + 3] = (byte) counter;

            cipher.init(opmode, key, new IvParameterSpec(ivCopy));

            // Skip partial block if needed
            int bytesToSkip = (int) (newPosition % AES_BLOCK_SIZE_BYTES);
            if (bytesToSkip > 0) {
                cipher.update(SKIP_BUFFER, 0, bytesToSkip);
            }
        } catch (InvalidAlgorithmParameterException | InvalidKeyException e) {
            throw new RuntimeException("Failed to initialize cipher", e);
        }
    }
}

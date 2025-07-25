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
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.GCMParameterSpec;

/**
 * Factory utility for creating and initializing Cipher instances
 *
 * This class is tailored for symmetric encryption modes like AES-CTR,
 * where a block counter is appended to the IV.
 *
 * @opensearch.internal
 */
public class AesCipherFactory {

    /**
     * Enum representing supported cipher types
     */
    public enum CipherType {
        CTR("AES/CTR/NoPadding"),
        GCM("AES/GCM/NoPadding");

        private final String transformation;

        CipherType(String transformation) {
            this.transformation = transformation;
        }

        public String getTransformation() {
            return transformation;
        }
    }

    /** AES block size in bytes. Required for counter calculations. */
    public static final int AES_BLOCK_SIZE_BYTES = 16;

    /** Number of bytes used for the counter in the IV (last 4 bytes). */
    public static final int COUNTER_SIZE_BYTES = 4;

    /** Total IV array length (typically 16 bytes for AES). */
    public static final int IV_ARRAY_LENGTH = 16;

    private static final byte[] ZERO_SKIP = new byte[AesCipherFactory.AES_BLOCK_SIZE_BYTES];

    /**
     * Returns a new Cipher instance configured based on the cipher type using the given provider.
     *
     * @param cipherType The cipher type to create
     * @param provider   The JCE provider to use (e.g., SunJCE, BouncyCastle)
     * @return A configured {@link Cipher} instance
     * @throws RuntimeException If the algorithm or padding is not supported
     */
    public static Cipher getCipher(CipherType cipherType, Provider provider) {
        try {
            return Cipher.getInstance(cipherType.getTransformation(), provider);
        } catch (NoSuchPaddingException | NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to get cipher instance", e);
        }
    }

    /**
     * Initializes a cipher for encryption or decryption based on cipher type.
     *
     * @param cipherType  The cipher type to initialize
     * @param cipher      The cipher instance to initialize
     * @param key         The symmetric key (e.g., AES key)
     * @param iv          The base IV
     * @param opmode      Cipher.ENCRYPT_MODE or Cipher.DECRYPT_MODE
     * @param newPosition The position in the stream to begin processing from
     * @throws RuntimeException If cipher initialization fails
     */
    public static void initCipher(CipherType cipherType, Cipher cipher, Key key, byte[] iv, int opmode, long newPosition) {
        switch (cipherType) {
            case CTR:
                initCTRCipher(cipher, key, iv, opmode, newPosition);
                break;
            case GCM:
                initGCMCipher(cipher, key, iv, opmode, newPosition);
                break;
            default:
                throw new RuntimeException("Unsupported cipher type: " + cipherType);
        }
    }

    /**
     * Initializes a CTR cipher for encryption or decryption, using an IV adjusted for the given position.
     * The last 4 bytes of the IV are treated as a counter, and are adjusted to reflect the block offset.
     * This allows for seeking into an encrypted stream without re-processing prior blocks.
     *
     * @param cipher      The cipher instance to initialize
     * @param key         The symmetric key (e.g., AES key)
     * @param iv          The base IV, typically 16 bytes long
     * @param opmode      Cipher.ENCRYPT_MODE or Cipher.DECRYPT_MODE
     * @param newPosition The position in the stream to begin processing from
     * @throws RuntimeException If cipher initialization fails
     */
    private static void initCTRCipher(Cipher cipher, Key key, byte[] iv, int opmode, long newPosition) {
        try {
            byte[] ivCopy = new byte[16];
            System.arraycopy(iv, 0, ivCopy, 0, 12);
            
            // Calculate which AES block this byte offset corresponds to
            long blockNumber = newPosition / AES_BLOCK_SIZE_BYTES;
            
            // GCM starts data blocks at counter value 2 (0: reserved, 1: auth, 2+: data)
            long gcmCounter = blockNumber + 2;

            // Set the 4-byte counter in big-endian format to match GCM
            ivCopy[12] = (byte) (gcmCounter >>> 24);
            ivCopy[13] = (byte) (gcmCounter >>> 16);
            ivCopy[14] = (byte) (gcmCounter >>> 8);
            ivCopy[15] = (byte) (gcmCounter);

            IvParameterSpec spec = new IvParameterSpec(ivCopy);
            cipher.init(opmode, key, spec);

            // Skip over any partial block offset using dummy update
            if (newPosition % AES_BLOCK_SIZE_BYTES > 0) {
                cipher.update(ZERO_SKIP, 0, (int) (newPosition % AES_BLOCK_SIZE_BYTES));
            }
        } catch (InvalidAlgorithmParameterException | InvalidKeyException e) {
            throw new RuntimeException("Failed to initialize cipher", e);
        }
    }

    /**
     * Initializes a GCM cipher for encryption or decryption, using a 12-byte IV.
     * GCM mode uses a different IV format than CTR mode.
     *
     * @param cipher      The cipher instance to initialize
     * @param key         The symmetric key (e.g., AES key)
     * @param iv          The base IV, first 12 bytes used for GCM
     * @param opmode      Cipher.ENCRYPT_MODE or Cipher.DECRYPT_MODE
     * @param newPosition The position in the stream (not used for GCM IV calculation)
     * @throws RuntimeException If cipher initialization fails
     */
    private static void initGCMCipher(Cipher cipher, Key key, byte[] iv, int opmode, long newPosition) {
        try {
            byte[] gcmIv = new byte[12];
            System.arraycopy(iv, 0, gcmIv, 0, 12);
            GCMParameterSpec spec = new GCMParameterSpec(128, gcmIv);
            cipher.init(opmode, key, spec);
        } catch (InvalidAlgorithmParameterException | InvalidKeyException e) {
            throw new RuntimeException("Failed to initialize GCM cipher", e);
        }
    }
}

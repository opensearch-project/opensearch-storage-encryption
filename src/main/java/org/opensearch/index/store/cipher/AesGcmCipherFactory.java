/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.cipher;

import org.opensearch.index.store.footer.EncryptionFooter;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;

/**
 * Factory utility for creating and initializing AES-GCM cipher instances.
 *
 * <p>This class provides methods for GCM (Galois Counter Mode) encryption and decryption
 * operations, including support for authentication tags and streaming encryption without
 * premature finalization.
 *
 * @opensearch.internal
 */
public class AesGcmCipherFactory {

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private AesGcmCipherFactory() {}

    /**
     * Standard GCM authentication tag length in bytes (128 bits).
     */
    public static final int GCM_TAG_LENGTH = 16;

    /**
     * Returns a new Cipher instance configured for AES/GCM/NoPadding using the given provider.
     *
     * @param provider The JCE provider to use (e.g., SunJCE, BouncyCastle)
     * @return A configured {@link Cipher} instance
     * @throws RuntimeException If the algorithm or padding is not supported
     */
    public static Cipher getCipher(Provider provider) {
        try {
            return Cipher.getInstance("AES/GCM/NoPadding", provider);
        } catch (NoSuchPaddingException | NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to get cipher instance", e);
        }
    }

    /**
     * Returns a new Cipher instance configured for AES/GCM/NoPadding using the given provider.
     *
     * @return A configured {@link Cipher} instance
     * @throws RuntimeException If the algorithm or padding is not supported
     */
    public static Cipher getCipher() {
        try {
            return Cipher.getInstance("AES/GCM/NoPadding");
        } catch (NoSuchPaddingException | NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to get cipher instance", e);
        }
    }

    /**
     * Initializes a GCM cipher for encryption or decryption, using a 12-byte IV.
     *
     * @param cipher      The cipher instance to initialize
     * @param key         The symmetric key (e.g., AES key)
     * @param iv          The base IV, first 12 bytes used for GCM
     * @param opmode      Cipher.ENCRYPT_MODE or Cipher.DECRYPT_MODE
     * @param newPosition The position in the stream (not used for GCM IV calculation)
     * @throws RuntimeException If cipher initialization fails
     */
    public static void initCipher(Cipher cipher, Key key, byte[] iv, int opmode, long newPosition) {
        try {
            // Verify we're using AES-256 (32-byte key)
            if (key.getEncoded().length != 32) {
                throw new RuntimeException("Expected AES-256 key (32 bytes), got " + key.getEncoded().length + " bytes");
            }

            byte[] gcmIv = new byte[12];
            System.arraycopy(iv, 0, gcmIv, 0, 12);
            GCMParameterSpec spec = new GCMParameterSpec(128, gcmIv);
            cipher.init(opmode, key, spec);
        } catch (InvalidAlgorithmParameterException | InvalidKeyException e) {
            throw new RuntimeException("Failed to initialize GCM cipher", e);
        }
    }

    /**
     * Encrypts data using pre-initialized GCM cipher without creating tags or calling doFinal
     *
     * @param filePosition The file position offset
     * @param cipher       Pre-initialized GCM cipher
     * @param input        Input data to encrypt
     * @param length       Length of data to encrypt
     * @return Encrypted data without authentication tag
     */
    public static byte[] encryptWithoutTag(long filePosition, javax.crypto.Cipher cipher, byte[] input, int length) {
        if (input == null || length <= 0) {
            throw new IllegalArgumentException("Input cannot be null and length must be positive");
        }
        if (length > input.length) {
            throw new IllegalArgumentException("Length cannot exceed input array size");
        }

        try {
            // Use cipher.update() without pre-allocated buffer to let cipher determine size
            return cipher.update(input, 0, length);
        } catch (Exception e) {
            throw new JavaCryptoException("GCM encryption failed at position " + filePosition, e);
        }
    }

    /**
     * Finalizes GCM encryption and returns the authentication tag.
     *
     * @param cipher Pre-initialized GCM cipher
     * @return Authentication tag
     * @throws JavaCryptoException If finalization fails
     */
    public static byte[] finalizeAndGetTag(javax.crypto.Cipher cipher) throws JavaCryptoException {
        try {
            return cipher.doFinal();
        } catch (Exception e) {
            throw new JavaCryptoException("GCM finalization failed", e);
        }
    }

    /**
     * Encrypts data with GCM and returns ciphertext with authentication tag appended.
     *
     * @param key    The AES key
     * @param iv     The initialization vector (first 12 bytes used)
     * @param input  Input data to encrypt
     * @param length Length of data to encrypt
     * @return Encrypted data with 16-byte authentication tag appended
     * @throws JavaCryptoException If encryption fails
     */
    public static byte[] encryptWithTag(Key key, byte[] iv, byte[] input, int length) throws JavaCryptoException {
        if (input == null || length <= 0) {
            throw new IllegalArgumentException("Input cannot be null and length must be positive");
        }
        if (length > input.length) {
            throw new IllegalArgumentException("Length cannot exceed input array size");
        }

        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            byte[] gcmIv = new byte[12];
            System.arraycopy(iv, 0, gcmIv, 0, 12);
            GCMParameterSpec spec = new GCMParameterSpec(128, gcmIv);
            cipher.init(Cipher.ENCRYPT_MODE, key, spec);

            return cipher.doFinal(input, 0, length);
        } catch (Exception e) {
            throw new JavaCryptoException("GCM encryption with tag failed", e);
        }
    }

    /**
     * Decrypts GCM data and verifies authentication tag.
     *
     * @param key        The AES key
     * @param iv         The initialization vector (first 12 bytes used)
     * @param ciphertext Encrypted data with authentication tag appended
     * @return Decrypted plaintext data
     * @throws JavaCryptoException If decryption or authentication fails
     */
    public static byte[] decryptWithTag(Key key, byte[] iv, byte[] ciphertext) throws JavaCryptoException {
        if (ciphertext == null || ciphertext.length < GCM_TAG_LENGTH) {
            throw new IllegalArgumentException("Ciphertext must be at least " + GCM_TAG_LENGTH + " bytes");
        }

        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            byte[] gcmIv = new byte[12];
            System.arraycopy(iv, 0, gcmIv, 0, 12);
            GCMParameterSpec spec = new GCMParameterSpec(128, gcmIv);
            cipher.init(Cipher.DECRYPT_MODE, key, spec);

            return cipher.doFinal(ciphertext);
        } catch (Exception e) {
            throw new JavaCryptoException("GCM decryption with tag verification failed", e);
        }
    }

    /**
     * Initializes a frame cipher for encryption with frame-specific IV.
     *
     * @param algorithm The encryption algorithm
     * @param provider The JCE provider
     * @param fileKey The file-specific encryption key
     * @param directoryKey The directory key for IV derivation
     * @param messageId The message ID from the footer
     * @param frameNumber The frame number
     * @param offsetWithinFrame The offset within the frame
     * @param filePathString The absolute file path as string
     * @return Initialized cipher ready for encryption
     */
    public static Cipher initializeFrameCipher(
        EncryptionAlgorithm algorithm,
        Provider provider,
        Key fileKey,
        byte[] directoryKey,
        byte[] messageId,
        int frameNumber,
        long offsetWithinFrame,
        String filePathString
    ) {
        byte[] frameIV = AesCipherFactory.computeFrameIV(
            directoryKey,
            messageId,
            frameNumber,
            offsetWithinFrame,
            filePathString
        );

        Cipher cipher = algorithm.getEncryptionCipher(provider);
        initCipher(cipher, fileKey, frameIV, Cipher.ENCRYPT_MODE, offsetWithinFrame);
        return cipher;
    }

    /**
     * Finalizes the current frame and extracts the GCM tag.
     *
     * @param cipher The cipher to finalize
     * @param footer The encryption footer to store the tag
     * @param outputStream The output stream to write remaining encrypted data
     * @param frameNumber The current frame number (for error messages)
     * @throws java.io.IOException If finalization or writing fails
     */
    public static void finalizeFrameAndWriteTag(
        Cipher cipher,
        EncryptionFooter footer,
        java.io.OutputStream outputStream,
        int frameNumber
    ) throws java.io.IOException {
        if (cipher == null) return;

        try {
            byte[] finalData = finalizeAndGetTag(cipher);

            if (finalData.length >= GCM_TAG_LENGTH) {
                int encryptedLength = finalData.length - GCM_TAG_LENGTH;
                if (encryptedLength > 0) {
                    outputStream.write(finalData, 0, encryptedLength);
                }

                byte[] gcmTag = java.util.Arrays.copyOfRange(finalData, encryptedLength, finalData.length);
                footer.addGcmTag(gcmTag);
            }
        } catch (Throwable t) {
            throw new java.io.IOException("Failed to finalize frame " + frameNumber, t);
        }
    }

    /**
     * Initialize GCM cipher for translog block encryption at given offset.
     * Generates unique IV based on offset and initializes cipher for encryption.
     *
     * @param key The encryption key
     * @param baseIV The base IV from KeyResolver
     * @param offset The byte offset (e.g., blockNumber * BLOCK_SIZE)
FHeader should contain translog UUID     * @return Initialized GCM cipher ready for streaming encryption
     */
    public static Cipher initializeGCMCipher(Key key, byte[] baseIV, long offset) {
        // Generate unique IV for this offset (16 bytes)
        byte[] uniqueIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(baseIV, offset);

        // Extract first 12 bytes for GCM
        byte[] gcmIV = new byte[12];
        System.arraycopy(uniqueIV, 0, gcmIV, 0, 12);

        // Get cipher instance
        Cipher cipher = getCipher();

        // Initialize for encryption
        initCipher(cipher, key, gcmIV, Cipher.ENCRYPT_MODE, offset);

        return cipher;
    }

    /**
     * Finalize GCM cipher and write authentication tag to FileChannel.
     * For translog blocks where tags are written inline after encrypted data.
     *
     * @param cipher The GCM cipher to finalize
     * @param channel The FileChannel to write the tag
     * @throws java.io.IOException If finalization or writing fails
     */
    public static void finalizeCipherAndWriteTag(
        Cipher cipher,
        java.nio.channels.FileChannel channel
    ) throws java.io.IOException {
        if (cipher == null) {
            throw new IllegalArgumentException("Cipher cannot be null");
        }

        try {
            // Finalize cipher - returns any remaining encrypted data + 16-byte tag
            byte[] finalData = finalizeAndGetTag(cipher);

            // Write to channel
            if (finalData.length > 0) {
                channel.write(java.nio.ByteBuffer.wrap(finalData));
            }
        } catch (JavaCryptoException e) {
            throw new java.io.IOException("Failed to finalize cipher and write tag", e);
        }
    }

    /**
     * Custom exception for Java crypto-related errors in GCM operations.
     */
    public static class JavaCryptoException extends RuntimeException {
        /**
         * Constructs a new JavaCryptoException with the specified detail message.
         *
         * @param message the detail message explaining the exception
         */
        public JavaCryptoException(String message) {
            super(message);
        }

        /**
         * Constructs a new JavaCryptoException with the specified detail message and cause.
         *
         * @param message the detail message explaining the exception
         * @param cause the underlying cause of the exception
         */
        public JavaCryptoException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

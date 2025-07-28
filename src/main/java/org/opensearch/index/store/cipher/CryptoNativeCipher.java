/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.cipher;

import javax.crypto.Cipher;

public class CryptoNativeCipher {

    /**
     * Custom exception for Java crypto-related errors
     */
    public static class JavaCryptoException extends RuntimeException {
        public JavaCryptoException(String message) {
            super(message);
        }

        public JavaCryptoException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Encrypts data using pre-initialized GCM cipher without creating tags or calling doFinal
     *
     * @param filePosition The file position offset
     * @param cipher Pre-initialized GCM cipher
     * @param input Input data to encrypt
     * @param length Length of data to encrypt
     * @return Encrypted data without authentication tag
     */
    public static byte[] encryptGCMJava(long filePosition, Cipher cipher, byte[] input, int length) {
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
    public static byte[] finalizeGCMJava(Cipher cipher) throws JavaCryptoException {
        try {
            return cipher.doFinal();
        } catch (Exception e) {
            throw new JavaCryptoException("GCM finalization failed", e);
        }
    }
}
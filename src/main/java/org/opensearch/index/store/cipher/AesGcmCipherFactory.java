/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.cipher;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;

public class AesGcmCipherFactory {

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
     * Initializes a GCM cipher for encryption or decryption, using a 12-byte IV.
     *
     * @param cipher      The cipher instance to initialize
     * @param key         The symmetric key (e.g., AES key)
     * @param iv          The base IV, first 12 bytes used for GCM
     * @param opmode      Cipher.ENCRYPT_MODE or Cipher.DECRYPT_MODE
     * @param newPosition The position in the stream (not used for GCM IV calculation)
     * @throws RuntimeException If cipher initialization fails
     */
    public static void initGCMCipher(Cipher cipher, Key key, byte[] iv, int opmode, long newPosition) {
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
}

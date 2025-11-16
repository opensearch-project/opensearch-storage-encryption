/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.cipher;

import static org.junit.Assert.assertArrayEquals;

import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for cipher encryption and decryption operations
 */
public class CipherEncryptionDecryptionTests extends OpenSearchTestCase {

    private static final byte[] TEST_KEY = new byte[32]; // 256-bit AES key
    private static final byte[] TEST_IV = new byte[16];  // 128-bit IV
    private static final byte[] TEST_DATA = "Hello World Test Data".getBytes(StandardCharsets.UTF_8);

    static {
        Arrays.fill(TEST_KEY, (byte) 0x42);
        Arrays.fill(TEST_IV, (byte) 0x24);
    }

    public void testEncryptDecryptWithCTR() throws Exception {
        // Get cipher from pool
        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");

        // Encrypt
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(TEST_IV));
        byte[] encrypted = cipher.update(TEST_DATA);

        // Decrypt
        cipher.init(Cipher.DECRYPT_MODE, keySpec, new IvParameterSpec(TEST_IV));
        byte[] decrypted = cipher.update(encrypted);

        assertArrayEquals(TEST_DATA, decrypted);
    }

    public void testEncryptDecryptWithGCM() throws Exception {
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");

        // Encrypt with tag
        byte[] encryptedWithTag = AesGcmCipherFactory.encryptWithTag(keySpec, TEST_IV, TEST_DATA, TEST_DATA.length);

        // Decrypt with tag verification
        byte[] decrypted = AesGcmCipherFactory.decryptWithTag(keySpec, TEST_IV, encryptedWithTag);

        assertArrayEquals(TEST_DATA, decrypted);
    }

    public void testEncryptWithGcmAndDecryptWithCTR() throws Exception {
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");

        // Encrypt with GCM without tag
        Cipher gcmCipher = AesGcmCipherFactory.getCipher(Security.getProvider("SunJCE"));
        AesGcmCipherFactory.initCipher(gcmCipher, keySpec, TEST_IV, Cipher.ENCRYPT_MODE, 0);
        byte[] gcmEncrypted = AesGcmCipherFactory.encryptWithoutTag(0, gcmCipher, TEST_DATA, TEST_DATA.length);

        // Get remaining bytes from GCM cipher
        byte[] remaining = AesGcmCipherFactory.finalizeAndGetTag(gcmCipher);

        // Combine encrypted data with remaining bytes (excluding tag)
        byte[] fullEncrypted = new byte[gcmEncrypted.length + remaining.length - AesGcmCipherFactory.GCM_TAG_LENGTH];
        System.arraycopy(gcmEncrypted, 0, fullEncrypted, 0, gcmEncrypted.length);
        System.arraycopy(remaining, 0, fullEncrypted, gcmEncrypted.length, remaining.length - AesGcmCipherFactory.GCM_TAG_LENGTH);

        // Decrypt with CTR using offset IV
        Cipher ctrCipher = AesCipherFactory.CIPHER_POOL.get();
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, 0);
        ctrCipher.init(Cipher.DECRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));
        byte[] decrypted = ctrCipher.update(fullEncrypted);

        assertArrayEquals(TEST_DATA, decrypted);
    }
}

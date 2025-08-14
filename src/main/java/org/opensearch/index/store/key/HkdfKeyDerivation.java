/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.store.key;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.nio.charset.StandardCharsets;

/**
 * HKDF (HMAC-based Key Derivation Function) implementation for deriving keys from MessageId.
 * Based on RFC 5869: https://tools.ietf.org/html/rfc5869
 * 
 * @opensearch.internal
 */
public class HkdfKeyDerivation {
    
    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private static final int HASH_LENGTH = 32; // SHA-256 output length
    
    /**
     * Derive a key from Directory Key + MessageId using HKDF
     * 
     * @param directoryKey the master key from KeyIvResolver (32 bytes)
     * @param messageId the unique file identifier (16 bytes from footer)
     * @param context the context string for key derivation
     * @param keyLength the desired output key length in bytes
     * @return derived key bytes
     */
    public static byte[] deriveKey(byte[] directoryKey, byte[] messageId, String context, int keyLength) {
        if (directoryKey == null || directoryKey.length != 32) {
            throw new IllegalArgumentException("Directory key must be 32 bytes");
        }
        if (messageId == null || messageId.length != 16) {
            throw new IllegalArgumentException("MessageId must be 16 bytes");
        }
        if (keyLength <= 0 || keyLength > 255 * HASH_LENGTH) {
            throw new IllegalArgumentException("Invalid key length: " + keyLength);
        }
        
        try {
            // HKDF-Extract: PRK = HMAC-Hash(directoryKey, messageId)
            // Use directory key as salt, messageId as input key material
            byte[] prk = hmac(directoryKey, messageId);
            
            // HKDF-Expand: OKM = HMAC-Hash(PRK, info || counter)
            byte[] info = context.getBytes(StandardCharsets.UTF_8);
            return hkdfExpand(prk, info, keyLength);
            
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException("HKDF key derivation failed", e);
        }
    }
    
    /**
     * Convenience method for deriving AES-256 keys (32 bytes)
     */
    public static byte[] deriveAesKey(byte[] directoryKey, byte[] messageId, String context) {
        return deriveKey(directoryKey, messageId, context, 32);
    }
    
    private static byte[] hmac(byte[] key, byte[] data) throws NoSuchAlgorithmException, InvalidKeyException {
        Mac mac = Mac.getInstance(HMAC_ALGORITHM);
        mac.init(new SecretKeySpec(key, HMAC_ALGORITHM));
        return mac.doFinal(data);
    }
    
    private static byte[] hkdfExpand(byte[] prk, byte[] info, int length) throws NoSuchAlgorithmException, InvalidKeyException {
        int n = (length + HASH_LENGTH - 1) / HASH_LENGTH; // Ceiling division
        byte[] okm = new byte[length];
        byte[] t = new byte[0];
        
        for (int i = 1; i <= n; i++) {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(new SecretKeySpec(prk, HMAC_ALGORITHM));
            mac.update(t);
            mac.update(info);
            mac.update((byte) i);
            t = mac.doFinal();
            
            int copyLength = Math.min(HASH_LENGTH, length - (i - 1) * HASH_LENGTH);
            System.arraycopy(t, 0, okm, (i - 1) * HASH_LENGTH, copyLength);
        }
        
        return okm;
    }
}
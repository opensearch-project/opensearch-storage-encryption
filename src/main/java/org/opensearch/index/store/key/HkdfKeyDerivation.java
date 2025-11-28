/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.key;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * HKDF (HMAC-based Key Derivation Function) implementation for deriving keys from MessageId.
 * Based on RFC 5869: https://tools.ietf.org/html/rfc5869
 *
 * @opensearch.internal
 */
public class HkdfKeyDerivation {

    private static final String HMAC_ALGORITHM = "HmacSHA384";
    private static final int HASH_LENGTH = 48; // SHA-384 output length

    /**
     * Derive a file key from Master Key + MessageId using HKDF
     *
     * @param masterKey the master key (32 bytes)
     * @param messageId the unique file identifier (16 bytes from footer)
     * @param context the context string for key derivation
     * @param keyLength the desired output key length in bytes
     * @return derived key bytes
     */
    public static byte[] deriveKey(byte[] masterKey, byte[] messageId, String context, int keyLength) {
        if (masterKey == null || masterKey.length != 32) {
            throw new IllegalArgumentException("Master key must be 32 bytes");
        }
        if (messageId == null || messageId.length != 16) {
            throw new IllegalArgumentException("MessageId must be 16 bytes");
        }
        if (keyLength <= 0 || keyLength > 255 * HASH_LENGTH) {
            throw new IllegalArgumentException("Invalid key length: " + keyLength);
        }

        try {
            // HKDF-Extract: PRK = HMAC-Hash(masterKey, messageId)
            // Use master key as salt, messageId as input key material
            byte[] prk = hmac(masterKey, messageId);

            // HKDF-Expand: OKM = HMAC-Hash(PRK, info || counter)
            byte[] info = context.getBytes(StandardCharsets.UTF_8);
            return hkdfExpand(prk, info, keyLength);

        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException("HKDF key derivation failed", e);
        }
    }

    /**
     * Convenience method for deriving AES-256 file keys (32 bytes)
     */
    public static byte[] deriveAesKey(byte[] masterKey, byte[] messageId, String context) {
        return deriveKey(masterKey, messageId, context, 32);
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

    /**
     * Derive file key directly from master key and messageId.
     *
     * @param masterKey the master key (32 bytes)
     * @param messageId the file's unique message ID (16 bytes)
     * @return derived 32-byte file key
     */
    public static byte[] deriveFileKey(byte[] masterKey, byte[] messageId) {
        return deriveKey(masterKey, messageId, "file-encryption", 32);
    }

    /**
     * Derive base IV for translog encryption from master key and translog UUID.
     * This ensures deterministic IV generation for translog files.
     *
     * @param masterKey the master key (32 bytes)
     * @param translogUUID the translog UUID string
     * @return derived 16-byte base IV for translog
     */
    public static byte[] deriveTranslogBaseIV(byte[] masterKey, String translogUUID) {
        byte[] uuidBytes = translogUUID.getBytes(StandardCharsets.UTF_8);
        byte[] paddedUuid = new byte[16];
        System.arraycopy(uuidBytes, 0, paddedUuid, 0, Math.min(uuidBytes.length, 16));
        return deriveKey(masterKey, paddedUuid, "translog-base-iv", 16);
    }
}

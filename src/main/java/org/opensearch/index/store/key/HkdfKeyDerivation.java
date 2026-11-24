/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.key;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.util.Objects;

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
     * Returns an HMAC-SHA384 {@link Mac} bound to {@code provider}.
     *
     * @param provider the JCE provider to use
     * @return a {@link Mac} instance from {@code provider}
     * @throws NoSuchAlgorithmException if {@code provider} does not support HMAC-SHA384
     */
    public static Mac getMac(Provider provider) throws NoSuchAlgorithmException {
        Objects.requireNonNull(provider, "provider");
        return Mac.getInstance(HMAC_ALGORITHM, provider);
    }

    /**
     * Derive a file key from Master Key + MessageId using HKDF
     *
     * @param masterKey the master key (32 bytes)
     * @param messageId the unique file identifier (16 bytes from footer)
     * @param context the context string for key derivation
     * @param keyLength the desired output key length in bytes
     * @param provider the JCE provider used for HMAC
     * @return derived key bytes
     */
    public static byte[] deriveKey(byte[] masterKey, byte[] messageId, String context, int keyLength, Provider provider) {
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
            byte[] prk = hmac(masterKey, messageId, provider);

            // HKDF-Expand: OKM = HMAC-Hash(PRK, info || counter)
            byte[] info = context.getBytes(StandardCharsets.UTF_8);
            return hkdfExpand(prk, info, keyLength, provider);

        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException("HKDF key derivation failed", e);
        }
    }

    /**
     * Convenience method for deriving AES-256 file keys (32 bytes)
     */
    public static byte[] deriveAesKey(byte[] masterKey, byte[] messageId, String context, Provider provider) {
        return deriveKey(masterKey, messageId, context, 32, provider);
    }

    private static byte[] hmac(byte[] key, byte[] data, Provider provider) throws NoSuchAlgorithmException, InvalidKeyException {
        Mac mac = getMac(provider);
        mac.init(new SecretKeySpec(key, HMAC_ALGORITHM));
        return mac.doFinal(data);
    }

    private static byte[] hkdfExpand(byte[] prk, byte[] info, int length, Provider provider) throws NoSuchAlgorithmException,
        InvalidKeyException {
        int n = (length + HASH_LENGTH - 1) / HASH_LENGTH; // Ceiling division
        byte[] okm = new byte[length];
        byte[] t = new byte[0];

        for (int i = 1; i <= n; i++) {
            Mac mac = getMac(provider);
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
     * @param provider the JCE provider used for HMAC
     * @return derived 32-byte file key
     */
    public static byte[] deriveFileKey(byte[] masterKey, byte[] messageId, Provider provider) {
        return deriveKey(masterKey, messageId, "file-encryption", 32, provider);
    }

    /**
     * Derive base IV for translog encryption from master key and translog UUID.
     * This ensures deterministic IV generation for translog files.
     *
     * @param masterKey the master key (32 bytes)
     * @param translogUUID the translog UUID string
     * @param provider the JCE provider used for HMAC
     * @return derived 16-byte base IV for translog
     */
    public static byte[] deriveTranslogBaseIV(byte[] masterKey, String translogUUID, Provider provider) {
        byte[] uuidBytes = translogUUID.getBytes(StandardCharsets.UTF_8);
        byte[] paddedUuid = new byte[16];
        System.arraycopy(uuidBytes, 0, paddedUuid, 0, Math.min(uuidBytes.length, 16));
        return deriveKey(masterKey, paddedUuid, "translog-base-iv", 16, provider);
    }
}

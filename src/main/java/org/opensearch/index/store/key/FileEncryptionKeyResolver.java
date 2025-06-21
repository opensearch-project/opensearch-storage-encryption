/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.key;

import static org.opensearch.index.store.cipher.EncryptionMetadataTrailer.ENCRYPTION_KEY_SIZE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.cipher.AesCtrCipherFactory;
import org.opensearch.index.store.cipher.EncryptionMetadataTrailer;

/**
 * File-level key resolver that derives a unique 256-bit HKDF key per Lucene
 * file.
 *
 * Design: - Derives a unique 256-bit HKDF key for each Lucene file using
 * HKDF-Extract/Expand - Uses directory data key as Input Key Material (IKM) -
 * Uses filename hash as salt for unique per-file derivation - Uses filename as
 * context info in HKDF-Expand - Stores raw HKDF key directly in Lucene file
 * header
 *
 * @opensearch.internal
 */
public class FileEncryptionKeyResolver {
    private static final Logger LOGGER = LogManager.getLogger(FileEncryptionKeyResolver.class);

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    /**
     * Derives a new 256-bit HKDF key using proper HKDF derivation and stores it
     * directly in the file header.
     */
    public static Key generateEncryptionKey(Key directoryDataKey) throws IOException {
        try {
            byte[] salt = generateRandomSalt();
            byte[] prk = hkdfExtract(salt, directoryDataKey.getEncoded());
            byte[] hkdfKeyBytes = hkdfExpand(prk, ENCRYPTION_KEY_SIZE);

            return new SecretKeySpec(hkdfKeyBytes, "AES");
        } catch (InvalidKeyException | NoSuchAlgorithmException e) {
            throw new IOException("Failed to derive and store HKDF key", e);
        }
    }

    private static byte[] hkdfExtract(byte[] salt, byte[] inputKeyMaterial) throws NoSuchAlgorithmException, InvalidKeyException {
        Mac hmac = Mac.getInstance("HmacSHA256");
        SecretKeySpec saltKey = new SecretKeySpec(salt, "HmacSHA256");
        hmac.init(saltKey);
        return hmac.doFinal(inputKeyMaterial);
    }

    private static byte[] hkdfExpand(byte[] prk, int length) throws NoSuchAlgorithmException, InvalidKeyException {
        Mac hmac = Mac.getInstance("HmacSHA256");
        SecretKeySpec prkKey = new SecretKeySpec(prk, "HmacSHA256");
        hmac.init(prkKey);

        int hashLen = hmac.getMacLength(); // 32 for SHA-256
        int n = (int) Math.ceil((double) length / hashLen);

        byte[] okm = new byte[length];
        byte[] t = new byte[0];

        for (int i = 1; i <= n; i++) {
            hmac.reset();
            hmac.update(t);
            hmac.update((byte) i);
            t = hmac.doFinal();

            int copyLen = Math.min(hashLen, length - (i - 1) * hashLen);
            System.arraycopy(t, 0, okm, (i - 1) * hashLen, copyLen);
        }

        return okm;
    }

    /**
     * Generates a 256-bit random salt for use in HKDF-Extract. This must be
     * stored alongside the ciphertext (e.g., in Lucene header).
     */
    private static byte[] generateRandomSalt() {
        byte[] salt = new byte[ENCRYPTION_KEY_SIZE];
        SECURE_RANDOM.nextBytes(salt);
        return salt;
    }

    @SuppressForbidden(reason = "temporary bypass")
    public static Key readEncryptionKey(Path filePath) throws IOException {
        long fileSize = Files.size(filePath);

        if (fileSize < EncryptionMetadataTrailer.ENCRYPTION_METADATA_TRAILER_SIZE) {
            throw new IOException("File too small to contain encryption footer: " + fileSize);
        }

        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            channel.position(fileSize - EncryptionMetadataTrailer.ENCRYPTION_METADATA_TRAILER_SIZE);

            ByteBuffer buffer = ByteBuffer.allocate(EncryptionMetadataTrailer.ENCRYPTION_METADATA_TRAILER_SIZE);
            int bytesRead = channel.read(buffer);
            if (bytesRead != EncryptionMetadataTrailer.ENCRYPTION_METADATA_TRAILER_SIZE) {
                throw new IOException("Failed to read full encryption trailer");
            }

            buffer.flip();

            byte[] magic = new byte[EncryptionMetadataTrailer.ENCRYPTION_MAGIC_BYTES.length];
            buffer.get(magic);
            if (!Arrays.equals(magic, EncryptionMetadataTrailer.ENCRYPTION_MAGIC_BYTES)) {
                throw new IOException(
                    "Invalid footer magic: got "
                        + new String(magic, StandardCharsets.UTF_8)
                        + ", expected "
                        + EncryptionMetadataTrailer.ENCRYPTION_MAGIC_STRING
                );
            }

            int version = buffer.getInt();
            if (version != EncryptionMetadataTrailer.ENCRYPTION_KEY_FORMAT_VERSION) {
                throw new IOException("Unsupported encryption key format version: " + version);
            }

            byte[] keyBytes = new byte[EncryptionMetadataTrailer.ENCRYPTION_KEY_SIZE];
            buffer.get(keyBytes);

            return new SecretKeySpec(keyBytes, AesCtrCipherFactory.ALGORITHM);
        }
    }
}

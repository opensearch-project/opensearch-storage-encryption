/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.cipher;

import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Provider;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;

/**
 * Factory utility for creating and initializing Cipher instances
 *
 * This class is tailored for symmetric encryption modes like AES-CTR,
 * where a block counter is appended to the IV.
 *
 * @opensearch.internal
 */
public class AesCtrCipherFactory {

    public static final String ALGORITHM = "AES";

    /** AES block size in bytes. Required for counter calculations. */
    public static final int AES_BLOCK_SIZE_BYTES = 16;

    /** Total IV array length (typically 16 bytes for AES). */
    public static final int IV_ARRAY_LENGTH = 16;

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
    * Computes a 16-byte AES-CTR IV by encoding the file position as a full 128-bit
    * big-endian counter value derived from the offset.
    * <p>
    * The offset is first converted to a block index (offset / 16),
    * and then mapped into two {@code long} values representing the high and low
    * parts of a 128-bit counter. This counter is encoded in big-endian order.
    *
    * @param offset The byte offset within the file.
    * @return A 16-byte IV where all 128 bits represent the counter.
    */
    public static byte[] computeOffsetIV(long offset) {
        byte[] iv = new byte[16];

        // Compute the AES block-aligned counter
        long blockIndex = offset / AesCtrCipherFactory.AES_BLOCK_SIZE_BYTES;

        long high = 0L;
        long low = blockIndex;

        // Encode 'high' (most significant 64 bits) into IV[0..7] in big-endian order
        iv[0] = (byte) (high >>> 56);
        iv[1] = (byte) (high >>> 48);
        iv[2] = (byte) (high >>> 40);
        iv[3] = (byte) (high >>> 32);
        iv[4] = (byte) (high >>> 24);
        iv[5] = (byte) (high >>> 16);
        iv[6] = (byte) (high >>> 8);
        iv[7] = (byte) high;

        // Encode 'low' (least significant 64 bits) into IV[8..15] in big-endian order
        iv[8] = (byte) (low >>> 56);
        iv[9] = (byte) (low >>> 48);
        iv[10] = (byte) (low >>> 40);
        iv[11] = (byte) (low >>> 32);
        iv[12] = (byte) (low >>> 24);
        iv[13] = (byte) (low >>> 16);
        iv[14] = (byte) (low >>> 8);
        iv[15] = (byte) low;

        return iv;
    }

    public static final ThreadLocal<Cipher> CIPHER_POOL = ThreadLocal.withInitial(() -> {
        try {
            return Cipher.getInstance("AES/CTR/NoPadding", "SunJCE");
        } catch (NoSuchAlgorithmException | NoSuchProviderException | NoSuchPaddingException e) {
            throw new RuntimeException(e);
        }
    });
}

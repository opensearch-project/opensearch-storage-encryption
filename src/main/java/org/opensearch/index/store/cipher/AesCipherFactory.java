/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.cipher;

import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Provider;
import java.util.Arrays;

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
public class AesCipherFactory {

    /** AES block size in bytes. Required for counter calculations. */
    public static final int AES_BLOCK_SIZE_BYTES = 16;

    /** Number of bytes used for the counter in the IV (last 4 bytes). */
    public static final int COUNTER_SIZE_BYTES = 4;

    /** Total IV array length (typically 16 bytes for AES). */
    public static final int IV_ARRAY_LENGTH = 16;

    /** The algorrithm. */
    public static final String ALGORITHM = "AES";

    /**
     * Returns a new Cipher instance configured for AES/CTR/NoPadding using the given provider.
     *
     * @param provider The JCE provider to use (e.g., SunJCE, BouncyCastle)
     * @return A configured {@link Cipher} instance
     * @throws RuntimeException If the algorithm or padding is not supported
     */
    public static Cipher getCipher(Provider provider) {
        try {
            return Cipher.getInstance("AES/CTR/NoPadding", provider);
        } catch (NoSuchPaddingException | NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to get cipher instance", e);
        }
    }

    public static final ThreadLocal<Cipher> CIPHER_POOL = ThreadLocal.withInitial(() -> {
        try {
            return Cipher.getInstance("AES/CTR/NoPadding", "SunJCE");
        } catch (NoSuchAlgorithmException | NoSuchProviderException | NoSuchPaddingException e) {
            throw new RuntimeException(e);
        }
    });

    public static byte[] computeOffsetIV(byte[] baseIV, long offset) {
        byte[] ivCopy = Arrays.copyOf(baseIV, baseIV.length);
        int blockOffset = (int) (offset / AesCipherFactory.AES_BLOCK_SIZE_BYTES);

        ivCopy[AesCipherFactory.IV_ARRAY_LENGTH - 1] = (byte) blockOffset;
        ivCopy[AesCipherFactory.IV_ARRAY_LENGTH - 2] = (byte) (blockOffset >>> 8);
        ivCopy[AesCipherFactory.IV_ARRAY_LENGTH - 3] = (byte) (blockOffset >>> 16);
        ivCopy[AesCipherFactory.IV_ARRAY_LENGTH - 4] = (byte) (blockOffset >>> 24);

        return ivCopy;
    }
}

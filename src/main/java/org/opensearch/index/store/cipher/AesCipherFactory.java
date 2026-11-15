/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.cipher;

import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.ShortBufferException;

import org.opensearch.index.store.footer.EncryptionMetadataTrailer;
import org.opensearch.index.store.key.HkdfKeyDerivation;

/**
 * Factory utility for creating and initializing Cipher instances
 *
 * This class is tailored for symmetric encryption modes like AES-CTR,
 * where a block counter is appended to the IV.
 *
 * @opensearch.internal
 */
public class AesCipherFactory {

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private AesCipherFactory() {}

    /** AES block size in terms of 2 power bytes (16 bytes). Required for counter calculations. */
    public static final int AES_BLOCK_SIZE_BYTES_IN_POWER = 4;

    /** AES block size in bytes (16). */
    public static final int AES_BLOCK_SIZE_BYTES = 1 << AES_BLOCK_SIZE_BYTES_IN_POWER;

    private static final byte[] ZERO_SKIP = new byte[AES_BLOCK_SIZE_BYTES];

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

    /**
     * Thread-local, provider-keyed cipher pool for AES/CTR/NoPadding operations.
     * Each thread gets its own cipher instance per provider so mixed-provider nodes
     * stay correct without synchronization on the hot path.
     */
    private static final ThreadLocal<Map<String, Cipher>> CIPHER_POOLS = ThreadLocal.withInitial(HashMap::new);

    /**
     * Returns a thread-local pooled AES/CTR/NoPadding cipher for the given provider.
     *
     * @param provider the JCE provider to use
     * @return a pooled {@link Cipher} instance bound to {@code provider}
     */
    public static Cipher getPooledCipher(Provider provider) {
        Objects.requireNonNull(provider, "provider");
        return CIPHER_POOLS.get().computeIfAbsent(provider.getName(), name -> getCipher(provider));
    }

    /**
     * Intra-block CTR skip for {@code offset}: {@code offset % 16}.
     *
     * @param offset byte offset in the stream
     * @return number of keystream bytes to skip within the current AES block
     */
    public static int skipBytesForOffset(long offset) {
        return (int) (offset & (AES_BLOCK_SIZE_BYTES - 1));
    }

    /**
     * Advances a CTR cipher by feeding {@code skipBytes} of zeros. SunJCE emits that
     * prefix immediately; Bouncy Castle buffers a partial AES block, so the prefix
     * appears at the start of a later {@code update}/{@code doFinal}. Returns how many
     * of those later output bytes must be discarded.
     *
     * @param cipher an initialized AES/CTR cipher
     * @param skipBytes keystream bytes to skip, in {@code [0, 16)}
     * @return pending skip-prefix length in subsequent output
     * @throws ShortBufferException if the provider needs more than one AES block of skip output
     */
    public static int advanceCtrSkip(Cipher cipher, int skipBytes) throws ShortBufferException {
        if (skipBytes <= 0) {
            return 0;
        }
        byte[] skipOut = new byte[AES_BLOCK_SIZE_BYTES];
        int emitted = cipher.update(ZERO_SKIP, 0, skipBytes, skipOut, 0);
        if (emitted < 0) {
            emitted = 0;
        }
        return skipBytes - emitted;
    }

    /**
     * Computes an offset-adjusted IV for CTR mode decryption at a specific file position.
     * This allows seeking into an encrypted stream without re-processing prior blocks.
     *
     * <p>The method calculates which AES block corresponds to the given byte offset and updates
     * the last 4 bytes of the IV with a block counter value. The counter is incremented by 2
     * to maintain compatibility with GCM mode counter semantics, where:
     * <ul>
     * <li>Counter 0: Reserved for GCM internal use</li>
     * <li>Counter 1: Used for authentication data processing</li>
     * <li>Counter 2+: Used for actual data block encryption</li>
     * </ul>
     *
     * <p>This ensures that CTR mode IVs generated by this method can be safely used in mixed
     * environments where both CTR and GCM modes might be employed, preventing counter collision
     * and maintaining cryptographic security.
     *
     * @param baseIV the base initialization vector (typically 16 bytes)
     * @param offset the byte offset in the file/stream to compute the IV for
     * @return a new IV with the counter field adjusted for the specified offset
     * @throws IllegalArgumentException if baseIV is null or has invalid length
     */
    public static byte[] computeOffsetIVForAesGcmEncrypted(byte[] baseIV, long offset) {
        byte[] ivCopy = Arrays.copyOf(baseIV, baseIV.length);
        int blockOffset = (int) (offset >> AES_BLOCK_SIZE_BYTES_IN_POWER);

        // Add 2 for GCM compatibility: counter 0 (reserved) + counter 1 (auth) + data counters start at 2
        blockOffset += 2;

        // Set the 4-byte counter in big-endian format (last 4 bytes of IV)
        ivCopy[AesCipherFactory.IV_ARRAY_LENGTH - 1] = (byte) blockOffset;
        ivCopy[AesCipherFactory.IV_ARRAY_LENGTH - 2] = (byte) (blockOffset >>> 8);
        ivCopy[AesCipherFactory.IV_ARRAY_LENGTH - 3] = (byte) (blockOffset >>> 16);
        ivCopy[AesCipherFactory.IV_ARRAY_LENGTH - 4] = (byte) (blockOffset >>> 24);

        return ivCopy;
    }

    /**
     * Computes an offset-adjusted IV for pure CTR mode encryption/decryption at a specific file position.
     * This method generates IVs for data that was originally encrypted using standard CTR mode without
     * GCM compatibility adjustments.
     *
     * <p>The method calculates which AES block corresponds to the given byte offset and updates
     * the last 4 bytes of the IV with the exact block counter value (starting from 0). This is used
     * for decrypting data that was encrypted with standard CTR mode where counters begin at 0.
     *
     * @param baseIV the base initialization vector (typically 16 bytes)
     * @param offset the byte offset in the file/stream to compute the IV for
     * @return a new IV with the counter field adjusted for the specified offset (starting from counter 0)
     * @throws IllegalArgumentException if baseIV is null or has invalid length
     */
    public static byte[] computeOffsetIVForAesCtrEncrypted(byte[] baseIV, long offset) {
        byte[] ivCopy = Arrays.copyOf(baseIV, baseIV.length);
        int blockOffset = (int) (offset >> AES_BLOCK_SIZE_BYTES_IN_POWER);

        // Set the 4-byte counter in big-endian format (last 4 bytes of IV)
        ivCopy[AesCipherFactory.IV_ARRAY_LENGTH - 1] = (byte) blockOffset;
        ivCopy[AesCipherFactory.IV_ARRAY_LENGTH - 2] = (byte) (blockOffset >>> 8);
        ivCopy[AesCipherFactory.IV_ARRAY_LENGTH - 3] = (byte) (blockOffset >>> 16);
        ivCopy[AesCipherFactory.IV_ARRAY_LENGTH - 4] = (byte) (blockOffset >>> 24);

        return ivCopy;
    }

    /**
     * Compute frame-specific IV for large file encryption
     *
     * @param directoryKey      the directory's master key (32 bytes)
     * @param messageId         the file's unique MessageId (16 bytes)
     * @param frameNumber       the frame number (0-based)
     * @param offsetWithinFrame the byte offset within the frame
     * @param normalizedFilePath the normalized file path string
     * @param encryptionMetadataCache the encryption metadata cache
     * @param provider the JCE provider used for HKDF HMAC
     * @return frame-specific IV for encryption/decryption
     */
    public static byte[] computeFrameIV(
        byte[] directoryKey,
        byte[] messageId,
        long frameNumber,
        long offsetWithinFrame,
        String normalizedFilePath,
        EncryptionMetadataCache encryptionMetadataCache,
        Provider provider
    ) {
        if (messageId.length != 16) {
            throw new IllegalArgumentException("MessageId must be 16 bytes");
        }
        if (frameNumber < 0 || frameNumber >= EncryptionMetadataTrailer.MAX_FRAMES_PER_FILE) {
            throw new IllegalArgumentException("Invalid frame number: " + frameNumber);
        }

        // Get or derive frame base IV from cache
        byte[] frameBaseIV = encryptionMetadataCache.getFrameIv(normalizedFilePath, frameNumber);
        if (frameBaseIV == null) {
            String frameContext = EncryptionMetadataTrailer.FRAME_CONTEXT_PREFIX + frameNumber;
            frameBaseIV = HkdfKeyDerivation.deriveKey(directoryKey, messageId, frameContext, 16, provider);
            encryptionMetadataCache.putFrameIv(normalizedFilePath, frameNumber, frameBaseIV);
        }

        // Modify last 4 bytes for block counter within frame
        byte[] frameIV = new byte[16];
        System.arraycopy(frameBaseIV, 0, frameIV, 0, 16);
        int blockOffset = (int) (offsetWithinFrame >> AES_BLOCK_SIZE_BYTES_IN_POWER);

        // Add 2 for GCM compatibility: counter 0 (reserved) + counter 1 (auth) + data counters start at 2
        blockOffset += 2;

        // Bytes 12-15: Block counter within frame (4 bytes, big-endian)
        frameIV[12] = (byte) (blockOffset >>> 24);
        frameIV[13] = (byte) (blockOffset >>> 16);
        frameIV[14] = (byte) (blockOffset >>> 8);
        frameIV[15] = (byte) blockOffset;

        return frameIV;
    }
}

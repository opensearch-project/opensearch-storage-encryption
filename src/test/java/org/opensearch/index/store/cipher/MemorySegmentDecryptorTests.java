/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.cipher;

import static org.junit.Assert.assertArrayEquals;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.junit.After;
import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;

@SuppressWarnings("preview")
public class MemorySegmentDecryptorTests extends OpenSearchTestCase {

    private static final byte[] TEST_KEY = new byte[32]; // 256-bit AES key
    private static final byte[] TEST_IV = new byte[16];  // 128-bit IV
    private static final byte[] TEST_DATA = "Hello World Test Data for Encryption!".getBytes(StandardCharsets.UTF_8);

    private Arena arena;

    static {
        Arrays.fill(TEST_KEY, (byte) 0x42);
        Arrays.fill(TEST_IV, (byte) 0x24);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        arena = Arena.ofConfined();
    }

    @After
    public void tearDown() throws Exception {
        if (arena != null) {
            arena.close();
        }
        super.tearDown();
    }

    public void testDecryptInPlaceWithArena() throws Exception {
        // Encrypt test data first
        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, 0);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));
        byte[] encrypted = cipher.update(TEST_DATA);

        // Allocate memory segment and copy encrypted data
        MemorySegment segment = arena.allocate(encrypted.length);
        for (int i = 0; i < encrypted.length; i++) {
            segment.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
        }

        // Decrypt in place
        MemorySegmentDecryptor.decryptInPlace(arena, segment.address(), encrypted.length, TEST_KEY, TEST_IV, 0);

        // Verify decrypted data
        byte[] decrypted = new byte[TEST_DATA.length];
        for (int i = 0; i < decrypted.length; i++) {
            decrypted[i] = segment.get(ValueLayout.JAVA_BYTE, i);
        }

        assertArrayEquals(TEST_DATA, decrypted);
    }

    public void testDecryptInPlaceWithGlobalScope() throws Exception {
        // Encrypt test data first
        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, 0);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));
        byte[] encrypted = cipher.update(TEST_DATA);

        // Allocate memory segment and copy encrypted data
        MemorySegment segment = arena.allocate(encrypted.length);
        for (int i = 0; i < encrypted.length; i++) {
            segment.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
        }

        // Decrypt in place using global scope
        MemorySegmentDecryptor.decryptInPlace(segment.address(), encrypted.length, TEST_KEY, TEST_IV, 0);

        // Verify decrypted data
        byte[] decrypted = new byte[TEST_DATA.length];
        for (int i = 0; i < decrypted.length; i++) {
            decrypted[i] = segment.get(ValueLayout.JAVA_BYTE, i);
        }

        assertArrayEquals(TEST_DATA, decrypted);
    }

    public void testDecryptSegment() throws Exception {
        // Encrypt test data first
        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, 0);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));
        byte[] encrypted = cipher.update(TEST_DATA);

        // Allocate memory segment and copy encrypted data
        MemorySegment segment = arena.allocate(encrypted.length);
        for (int i = 0; i < encrypted.length; i++) {
            segment.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
        }

        // Decrypt segment
        MemorySegmentDecryptor.decryptSegment(segment, 0, TEST_KEY, TEST_IV, encrypted.length);

        // Verify decrypted data
        byte[] decrypted = new byte[TEST_DATA.length];
        for (int i = 0; i < decrypted.length; i++) {
            decrypted[i] = segment.get(ValueLayout.JAVA_BYTE, i);
        }

        assertArrayEquals(TEST_DATA, decrypted);
    }

    public void testDecryptWithFileOffset() throws Exception {
        long fileOffset = 1024;

        // Encrypt test data with offset-aware IV
        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, fileOffset);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));

        // Advance cipher for non-aligned offset
        if (fileOffset % AesCipherFactory.AES_BLOCK_SIZE_BYTES > 0) {
            byte[] skip = new byte[(int) (fileOffset % AesCipherFactory.AES_BLOCK_SIZE_BYTES)];
            cipher.update(skip);
        }

        byte[] encrypted = cipher.update(TEST_DATA);

        // Allocate memory segment and copy encrypted data
        MemorySegment segment = arena.allocate(encrypted.length);
        for (int i = 0; i < encrypted.length; i++) {
            segment.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
        }

        // Decrypt with file offset
        MemorySegmentDecryptor.decryptInPlace(arena, segment.address(), encrypted.length, TEST_KEY, TEST_IV, fileOffset);

        // Verify decrypted data
        byte[] decrypted = new byte[TEST_DATA.length];
        for (int i = 0; i < decrypted.length; i++) {
            decrypted[i] = segment.get(ValueLayout.JAVA_BYTE, i);
        }

        assertArrayEquals(TEST_DATA, decrypted);
    }

    public void testDecryptLargeData() throws Exception {
        // Test with data larger than default chunk size
        byte[] largeData = new byte[32768]; // 32KB
        new SecureRandom().nextBytes(largeData);

        // Encrypt
        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, 0);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));
        byte[] encrypted = cipher.doFinal(largeData);

        // Allocate memory segment and copy encrypted data
        MemorySegment segment = arena.allocate(encrypted.length);
        for (int i = 0; i < encrypted.length; i++) {
            segment.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
        }

        // Decrypt
        MemorySegmentDecryptor.decryptInPlace(arena, segment.address(), encrypted.length, TEST_KEY, TEST_IV, 0);

        // Verify
        byte[] decrypted = new byte[largeData.length];
        for (int i = 0; i < decrypted.length; i++) {
            decrypted[i] = segment.get(ValueLayout.JAVA_BYTE, i);
        }

        assertArrayEquals(largeData, decrypted);
    }

    public void testDecryptWithNonAlignedOffset() throws Exception {
        long fileOffset = 17; // Non-aligned offset

        // Encrypt with offset
        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, fileOffset);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));

        // Advance cipher for non-aligned offset
        int skipBytes = (int) (fileOffset % AesCipherFactory.AES_BLOCK_SIZE_BYTES);
        if (skipBytes > 0) {
            cipher.update(new byte[skipBytes]);
        }

        byte[] encrypted = cipher.update(TEST_DATA);

        // Allocate and decrypt
        MemorySegment segment = arena.allocate(encrypted.length);
        for (int i = 0; i < encrypted.length; i++) {
            segment.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
        }

        MemorySegmentDecryptor.decryptInPlace(arena, segment.address(), encrypted.length, TEST_KEY, TEST_IV, fileOffset);

        // Verify
        byte[] decrypted = new byte[TEST_DATA.length];
        for (int i = 0; i < decrypted.length; i++) {
            decrypted[i] = segment.get(ValueLayout.JAVA_BYTE, i);
        }

        assertArrayEquals(TEST_DATA, decrypted);
    }

    public void testDecryptEmptyData() throws Exception {
        byte[] emptyData = new byte[0];

        // Encrypt empty data
        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, 0);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));
        byte[] encrypted = cipher.update(emptyData);

        if (encrypted == null || encrypted.length == 0) {
            // Empty encryption is valid
            return;
        }

        // Allocate and decrypt
        MemorySegment segment = arena.allocate(Math.max(1, encrypted.length));
        MemorySegmentDecryptor.decryptInPlace(arena, segment.address(), encrypted.length, TEST_KEY, TEST_IV, 0);

        // Should complete without error
    }

    public void testDecryptMultipleSegments() throws Exception {
        int segmentCount = 5;
        byte[][] originalData = new byte[segmentCount][];
        MemorySegment[] segments = new MemorySegment[segmentCount];

        for (int i = 0; i < segmentCount; i++) {
            originalData[i] = ("Segment " + i + " data content").getBytes(StandardCharsets.UTF_8);

            // Encrypt
            Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
            SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
            byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, i * 1024L);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));

            if ((i * 1024L) % AesCipherFactory.AES_BLOCK_SIZE_BYTES > 0) {
                cipher.update(new byte[(int) ((i * 1024L) % AesCipherFactory.AES_BLOCK_SIZE_BYTES)]);
            }

            byte[] encrypted = cipher.update(originalData[i]);

            // Allocate segment
            segments[i] = arena.allocate(encrypted.length);
            for (int j = 0; j < encrypted.length; j++) {
                segments[i].set(ValueLayout.JAVA_BYTE, j, encrypted[j]);
            }

            // Decrypt
            MemorySegmentDecryptor.decryptSegment(segments[i], i * 1024L, TEST_KEY, TEST_IV, encrypted.length);

            // Verify
            byte[] decrypted = new byte[originalData[i].length];
            for (int j = 0; j < decrypted.length; j++) {
                decrypted[j] = segments[i].get(ValueLayout.JAVA_BYTE, j);
            }

            assertArrayEquals("Segment " + i + " mismatch", originalData[i], decrypted);
        }
    }

    public void testChunkedDecryption() throws Exception {
        // Test that chunked processing works correctly
        int dataSize = 20000; // Larger than default chunk size
        byte[] testData = new byte[dataSize];
        new SecureRandom().nextBytes(testData);

        // Encrypt
        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, 0);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));
        byte[] encrypted = cipher.doFinal(testData);

        // Allocate segment
        MemorySegment segment = arena.allocate(encrypted.length);
        for (int i = 0; i < encrypted.length; i++) {
            segment.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
        }

        // Decrypt using chunked processing
        MemorySegmentDecryptor.decryptSegment(segment, 0, TEST_KEY, TEST_IV, encrypted.length);

        // Verify
        byte[] decrypted = new byte[testData.length];
        for (int i = 0; i < decrypted.length; i++) {
            decrypted[i] = segment.get(ValueLayout.JAVA_BYTE, i);
        }

        assertArrayEquals(testData, decrypted);
    }
}

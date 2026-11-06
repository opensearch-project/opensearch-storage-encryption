/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.cipher;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for the heap-array GCM update in {@link OpenSslNativeCipher}.
 */
public class OpenSslNativeCipherTests extends OpenSearchTestCase {

    /**
     * Ciphertext and tag must match the JDK's AES-GCM for the same key, IV and plaintext, whatever the
     * array offsets and update sizes, including updates larger than {@link OpenSslNativeCipher#MAX_HEAP_UPDATE_SIZE}.
     */
    public void testEncryptUpdateMatchesJdkGcm() throws Throwable {
        for (int iter = 0; iter < 20; iter++) {
            byte[] key = randomByteArrayOfLength(32);
            byte[] iv = randomByteArrayOfLength(16);
            byte[] plaintext = randomByteArrayOfLength(randomIntBetween(0, 3 * OpenSslNativeCipher.MAX_HEAP_UPDATE_SIZE + 17));

            MemorySegment ctx = OpenSslNativeCipher.initGCMCipher(key, iv, 0);
            ByteArrayOutputStream ciphertext = new ByteArrayOutputStream();
            int pos = 0;
            while (pos < plaintext.length) {
                int n = Math.min(plaintext.length - pos, randomIntBetween(1, 2 * OpenSslNativeCipher.MAX_HEAP_UPDATE_SIZE));
                int inOffset = randomIntBetween(0, 64);
                int outOffset = randomIntBetween(0, 64);
                byte[] in = new byte[inOffset + n + randomIntBetween(0, 64)];
                System.arraycopy(plaintext, pos, in, inOffset, n);
                byte[] inCopy = in.clone();
                byte[] out = new byte[outOffset + n + randomIntBetween(0, 64)];

                OpenSslNativeCipher.encryptUpdate(ctx, in, inOffset, out, outOffset, n);

                assertArrayEquals("input must not be modified", inCopy, in);
                ciphertext.write(out, outOffset, n);
                pos += n;
            }
            byte[] tag = OpenSslNativeCipher.finalizeAndGetTag(ctx);

            Cipher jdk = Cipher.getInstance("AES/GCM/NoPadding");
            jdk.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(128, Arrays.copyOf(iv, 12)));
            byte[] expected = jdk.doFinal(plaintext);

            assertArrayEquals(Arrays.copyOf(expected, plaintext.length), ciphertext.toByteArray());
            assertArrayEquals(Arrays.copyOfRange(expected, plaintext.length, expected.length), tag);
        }
    }

    public void testEncryptUpdateRejectsOutOfBoundsRanges() throws Throwable {
        MemorySegment ctx = OpenSslNativeCipher.initGCMCipher(randomByteArrayOfLength(32), randomByteArrayOfLength(16), 0);
        try {
            byte[] ten = new byte[10];
            expectThrows(IndexOutOfBoundsException.class, () -> OpenSslNativeCipher.encryptUpdate(ctx, ten, 5, ten.clone(), 0, 6));
            expectThrows(IndexOutOfBoundsException.class, () -> OpenSslNativeCipher.encryptUpdate(ctx, ten, 0, ten.clone(), 5, 6));
            expectThrows(IndexOutOfBoundsException.class, () -> OpenSslNativeCipher.encryptUpdate(ctx, ten, -1, ten.clone(), 0, 1));
        } finally {
            OpenSslNativeCipher.finalizeAndGetTag(ctx);
        }
    }

    public void testEncryptUpdateRejectsPartlyOverlappingRanges() throws Throwable {
        MemorySegment ctx = OpenSslNativeCipher.initGCMCipher(randomByteArrayOfLength(32), randomByteArrayOfLength(16), 0);
        try {
            byte[] buf = new byte[64];
            expectThrows(IllegalArgumentException.class, () -> OpenSslNativeCipher.encryptUpdate(ctx, buf, 0, buf, 8, 32));
            expectThrows(IllegalArgumentException.class, () -> OpenSslNativeCipher.encryptUpdate(ctx, buf, 8, buf, 0, 32));
        } finally {
            OpenSslNativeCipher.finalizeAndGetTag(ctx);
        }
    }

    /** In-place encryption (same array, same offset) is allowed and gives the same result as separate arrays. */
    public void testEncryptUpdateInPlace() throws Throwable {
        byte[] key = randomByteArrayOfLength(32);
        byte[] iv = randomByteArrayOfLength(16);
        byte[] plaintext = randomByteArrayOfLength(randomIntBetween(1, 2 * OpenSslNativeCipher.MAX_HEAP_UPDATE_SIZE));

        MemorySegment separate = OpenSslNativeCipher.initGCMCipher(key, iv, 0);
        byte[] expected = new byte[plaintext.length];
        OpenSslNativeCipher.encryptUpdate(separate, plaintext, 0, expected, 0, plaintext.length);
        byte[] expectedTag = OpenSslNativeCipher.finalizeAndGetTag(separate);

        MemorySegment inPlace = OpenSslNativeCipher.initGCMCipher(key, iv, 0);
        byte[] buf = plaintext.clone();
        OpenSslNativeCipher.encryptUpdate(inPlace, buf, 0, buf, 0, buf.length);
        byte[] tag = OpenSslNativeCipher.finalizeAndGetTag(inPlace);

        assertArrayEquals(expected, buf);
        assertArrayEquals(expectedTag, tag);
    }
}

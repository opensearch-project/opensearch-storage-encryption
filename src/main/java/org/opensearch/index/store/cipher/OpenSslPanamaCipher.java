/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.cipher;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.Locale;

import org.opensearch.common.SuppressForbidden;

/**
 * Provides native bindings to OpenSSL EVP_aes_256_ctr using the Java Panama FFI.
 * This class is thread-safe as it creates new cipher contexts for each encryption operation.
 *
 * @opensearch.internal
 */
@SuppressForbidden(reason = "temporary bypass")
@SuppressWarnings("preview")
public final class OpenSslPanamaCipher {

    private static final int AES_256_KEY_SIZE = 32;
    private static final int AES_BLOCK_SIZE = 16;

    public static final Linker LINKER;
    public static final SymbolLookup LIBCRYPTO;

    public static final MethodHandle EVP_CIPHER_CTX_new;
    public static final MethodHandle EVP_CIPHER_CTX_free;
    public static final MethodHandle EVP_EncryptInit_ex;
    public static final MethodHandle EVP_EncryptUpdate;
    public static final MethodHandle EVP_aes_256_ctr;

    /**
     * Custom exception for OpenSSL-related errors
     */
    public static class OpenSslException extends RuntimeException {
        public OpenSslException(String message) {
            super(message);
        }

        public OpenSslException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    static {
        try {
            LINKER = Linker.nativeLinker();
            LIBCRYPTO = SymbolLookup.libraryLookup("crypto", Arena.global());
        } catch (Exception e) {
            throw new UnsatisfiedLinkError("Failed to load OpenSSL crypto library: " + e.getMessage());
        }

        try {
            EVP_CIPHER_CTX_new = LINKER
                .downcallHandle(LIBCRYPTO.find("EVP_CIPHER_CTX_new").orElseThrow(), FunctionDescriptor.of(ValueLayout.ADDRESS));

            EVP_CIPHER_CTX_free = LINKER
                .downcallHandle(LIBCRYPTO.find("EVP_CIPHER_CTX_free").orElseThrow(), FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

            EVP_EncryptInit_ex = LINKER
                .downcallHandle(
                    LIBCRYPTO.find("EVP_EncryptInit_ex").orElseThrow(),
                    FunctionDescriptor
                        .of(
                            ValueLayout.JAVA_INT,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS
                        )
                );

            EVP_EncryptUpdate = LINKER
                .downcallHandle(
                    LIBCRYPTO.find("EVP_EncryptUpdate").orElseThrow(),
                    FunctionDescriptor
                        .of(
                            ValueLayout.JAVA_INT,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.JAVA_INT
                        )
                );

            EVP_aes_256_ctr = LINKER
                .downcallHandle(LIBCRYPTO.find("EVP_aes_256_ctr").orElseThrow(), FunctionDescriptor.of(ValueLayout.ADDRESS));

        } catch (Throwable t) {
            throw new OpenSslException("Failed to initialize OpenSSL method handles via Panama", t);
        }
    }

    /**
     * Encrypts the input data using AES-256-CTR mode.
     *
     * @param key   The 32-byte encryption key
     * @param iv    The 16-byte initialization vector
     * @param input The data to encrypt
     * @return The encrypted data
     * @throws IllegalArgumentException if the input parameters are invalid
     * @throws OpenSslException if encryption fails
     * @throws Throwable if there's an unexpected error
     */
    public static byte[] encrypt(byte[] key, byte[] iv, byte[] input) throws Throwable {
        // Input validation
        if (key == null || key.length != AES_256_KEY_SIZE) {
            throw new IllegalArgumentException("Invalid key length: expected " + AES_256_KEY_SIZE + " bytes");
        }
        if (iv == null || iv.length != AES_BLOCK_SIZE) {
            throw new IllegalArgumentException("Invalid IV length: expected " + AES_BLOCK_SIZE + " bytes");
        }
        if (input == null || input.length == 0) {
            throw new IllegalArgumentException("Input cannot be null or empty");
        }

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment ctx = (MemorySegment) EVP_CIPHER_CTX_new.invoke();
            if (ctx.address() == 0) {
                throw new OpenSslException("EVP_CIPHER_CTX_new failed");
            }

            try {
                MemorySegment cipher = (MemorySegment) EVP_aes_256_ctr.invoke();
                if (cipher.address() == 0) {
                    throw new OpenSslException("EVP_aes_256_ctr failed");
                }

                MemorySegment keySeg = arena.allocateArray(ValueLayout.JAVA_BYTE, key);
                MemorySegment ivSeg = arena.allocateArray(ValueLayout.JAVA_BYTE, iv);

                int rc = (int) EVP_EncryptInit_ex.invoke(ctx, cipher, MemorySegment.NULL, keySeg, ivSeg);
                if (rc != 1) {
                    throw new OpenSslException("EVP_EncryptInit_ex failed");
                }

                MemorySegment inputSeg = arena.allocateArray(ValueLayout.JAVA_BYTE, input);
                // Ensure output buffer is large enough for potential padding
                MemorySegment outputSeg = arena.allocate(input.length + AES_BLOCK_SIZE);
                MemorySegment outLen = arena.allocate(ValueLayout.JAVA_INT);

                rc = (int) EVP_EncryptUpdate.invoke(ctx, outputSeg, outLen, inputSeg, input.length);
                if (rc != 1) {
                    throw new OpenSslException("EVP_EncryptUpdate failed");
                }

                int bytesWritten = outLen.get(ValueLayout.JAVA_INT, 0);
                byte[] encrypted = new byte[bytesWritten];
                outputSeg.asByteBuffer().get(encrypted);

                return encrypted;
            } finally {
                EVP_CIPHER_CTX_free.invoke(ctx);
            }
        }
    }

    private OpenSslPanamaCipher() {
        // Utility class
    }
}

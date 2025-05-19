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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
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

    public static final int AES_256_KEY_SIZE = 16;
    public static final int COUNTER_SIZE_BYTES = 4;

    public static final MethodHandle EVP_CIPHER_CTX_new;
    public static final MethodHandle EVP_CIPHER_CTX_free;
    public static final MethodHandle EVP_EncryptInit_ex;
    public static final MethodHandle EVP_EncryptUpdate;
    public static final MethodHandle EVP_aes_256_ctr;

    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup LIBCRYPTO = loadLibcrypto();

    private static SymbolLookup loadLibcrypto() {
        String os = System.getProperty("os.name").toLowerCase(Locale.ROOT);

        if (os.contains("mac")) {
            // Common Homebrew path for OpenSSL on macOS (adjust for Intel vs M1)
            String macPath = "/opt/homebrew/opt/openssl@3/lib/libcrypto.dylib";
            if (!Files.exists(Path.of(macPath))) {
                macPath = "/usr/local/opt/openssl@3/lib/libcrypto.dylib"; // fallback for Intel
            }
            return SymbolLookup.libraryLookup(Path.of(macPath), Arena.global());
        } else if (os.contains("linux")) {
            try {
                // Prefer explicitly versioned libcrypto if available
                Path lib64 = Path.of("/lib64/libcrypto.so.10");
                if (Files.exists(lib64)) {
                    return SymbolLookup.libraryLookup(lib64, Arena.global());
                }

                Path generic = Path.of("/lib64/libcrypto.so");
                if (Files.exists(generic)) {
                    return SymbolLookup.libraryLookup(generic, Arena.global());
                }

                // Fallback to standard /lib location
                Path fallback = Path.of("/lib/libcrypto.so");
                if (Files.exists(fallback)) {
                    return SymbolLookup.libraryLookup(fallback, Arena.global());
                }

                throw new RuntimeException("Could not find libcrypto in common Linux library locations.");
            } catch (Exception e) {
                throw new RuntimeException("Failed to load libcrypto", e);
            }
        } else {
            throw new UnsupportedOperationException("Unsupported OS: " + os);
        }
    }

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
                            ValueLayout.ADDRESS, // ctx
                            ValueLayout.ADDRESS, // out
                            ValueLayout.ADDRESS, // outLen
                            ValueLayout.ADDRESS, // in
                            ValueLayout.JAVA_INT // inLen
                        )
                );

            EVP_aes_256_ctr = LINKER
                .downcallHandle(LIBCRYPTO.find("EVP_aes_256_ctr").orElseThrow(), FunctionDescriptor.of(ValueLayout.ADDRESS));

        } catch (Throwable t) {
            throw new OpenSslException("Failed to initialize OpenSSL method handles via Panama", t);
        }
    }

    private static byte[] computeOffsetIV(byte[] baseIV, long filePosition) {

        byte[] ivCopy = Arrays.copyOf(baseIV, AES_256_KEY_SIZE);

        int counter = (int) (filePosition / AES_256_KEY_SIZE);
        for (int i = AES_256_KEY_SIZE - 1; i >= AES_256_KEY_SIZE - COUNTER_SIZE_BYTES; i--) {
            ivCopy[i] = (byte) counter;
            counter >>>= 8;
        }

        return ivCopy;
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
        return encrypt(key, iv, input, 0L);
    }

    public static byte[] encrypt(byte[] key, byte[] iv, byte[] input, long filePosition) throws Throwable {
        if (key == null || key.length != AES_256_KEY_SIZE) {
            throw new IllegalArgumentException("Invalid key length: expected " + AES_256_KEY_SIZE + " bytes");
        }
        if (iv == null || iv.length != AES_256_KEY_SIZE) {
            throw new IllegalArgumentException("Invalid IV length: expected " + AES_256_KEY_SIZE + " bytes");
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

                byte[] adjustedIV = computeOffsetIV(iv, filePosition);
                MemorySegment keySeg = arena.allocateArray(ValueLayout.JAVA_BYTE, key);
                MemorySegment ivSeg = arena.allocateArray(ValueLayout.JAVA_BYTE, adjustedIV);

                int rc = (int) EVP_EncryptInit_ex.invoke(ctx, cipher, MemorySegment.NULL, keySeg, ivSeg);
                if (rc != 1) {
                    throw new OpenSslException("EVP_EncryptInit_ex failed");
                }

                int skipBytes = (int) (filePosition % AES_256_KEY_SIZE);
                if (skipBytes > 0) {
                    MemorySegment dummyIn = arena.allocateArray(ValueLayout.JAVA_BYTE, skipBytes);
                    MemorySegment dummyOut = arena.allocate(skipBytes + AES_256_KEY_SIZE);
                    MemorySegment dummyLen = arena.allocate(ValueLayout.JAVA_INT);
                    EVP_EncryptUpdate.invoke(ctx, dummyOut, dummyLen, dummyIn, skipBytes);
                }

                MemorySegment inSeg = arena.allocateArray(ValueLayout.JAVA_BYTE, input);
                MemorySegment outSeg = arena.allocate(input.length + AES_256_KEY_SIZE);
                MemorySegment outLen = arena.allocate(ValueLayout.JAVA_INT);

                rc = (int) EVP_EncryptUpdate.invoke(ctx, outSeg, outLen, inSeg, input.length);
                if (rc != 1) {
                    throw new OpenSslException("EVP_EncryptUpdate failed");
                }

                int bytesWritten = outLen.get(ValueLayout.JAVA_INT, 0);
                return outSeg.asSlice(0, bytesWritten).toArray(ValueLayout.JAVA_BYTE);
            } finally {
                EVP_CIPHER_CTX_free.invoke(ctx);
            }
        }
    }

    /**
    * Decrypts the input data using AES-256-CTR mode.
    * This method is symmetric with `encrypt(...)` because AES-CTR uses the same function for encryption and decryption.
    *
    * @param key   The 32-byte AES key
    * @param iv    The 16-byte initialization vector
    * @param input The encrypted data
    * @param filePosition The file offset (used to adjust IV counter)
    * @return The decrypted plaintext
    * @throws OpenSslException if decryption fails
    * @throws Throwable if a low-level Panama error occurs
    */
    public static byte[] decrypt(byte[] key, byte[] iv, byte[] input, long filePosition) throws Throwable {
        if (key == null || key.length != AES_256_KEY_SIZE) {
            throw new IllegalArgumentException("Invalid key length: expected " + AES_256_KEY_SIZE + " bytes");
        }
        if (iv == null || iv.length != AES_256_KEY_SIZE) {
            throw new IllegalArgumentException("Invalid IV length: expected " + AES_256_KEY_SIZE + " bytes");
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

                // Compute IV with offset counter
                byte[] adjustedIV = computeOffsetIV(iv, filePosition);
                MemorySegment keySeg = arena.allocateArray(ValueLayout.JAVA_BYTE, key);
                MemorySegment ivSeg = arena.allocateArray(ValueLayout.JAVA_BYTE, adjustedIV);

                int rc = (int) EVP_EncryptInit_ex.invoke(ctx, cipher, MemorySegment.NULL, keySeg, ivSeg);
                if (rc != 1) {
                    throw new OpenSslException("EVP_EncryptInit_ex failed");
                }

                // Skip any partial block
                int partialOffset = (int) (filePosition % AES_256_KEY_SIZE);
                if (partialOffset > 0) {
                    MemorySegment dummyIn = arena.allocateArray(ValueLayout.JAVA_BYTE, partialOffset);
                    MemorySegment dummyOut = arena.allocate(partialOffset + AES_256_KEY_SIZE);
                    MemorySegment dummyLen = arena.allocate(ValueLayout.JAVA_INT);
                    EVP_EncryptUpdate.invoke(ctx, dummyOut, dummyLen, dummyIn, partialOffset);
                }

                MemorySegment inSeg = arena.allocateArray(ValueLayout.JAVA_BYTE, input);
                MemorySegment outSeg = arena.allocate(input.length + AES_256_KEY_SIZE);
                MemorySegment outLen = arena.allocate(ValueLayout.JAVA_INT);

                rc = (int) EVP_EncryptUpdate.invoke(ctx, outSeg, outLen, inSeg, input.length);
                if (rc != 1) {
                    throw new OpenSslException("EVP_EncryptUpdate failed during decryption");
                }

                int bytesWritten = outLen.get(ValueLayout.JAVA_INT, 0);
                return outSeg.asSlice(0, bytesWritten).toArray(ValueLayout.JAVA_BYTE);
            } finally {
                EVP_CIPHER_CTX_free.invoke(ctx);
            }
        }
    }

    private OpenSslPanamaCipher() {
        // Utility class
    }
}

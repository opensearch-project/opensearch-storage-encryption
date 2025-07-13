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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;

/**
 * Provides native bindings to OpenSSL EVP_aes_256_ctr using the Java Panama FFI.
 * This class is thread-safe as it creates new cipher contexts for each encryption operation.
 *
 * @opensearch.internal
 */
@SuppressForbidden(reason = "temporary bypass")
@SuppressWarnings("preview")
public final class OpenSslNativeCipher {

    private static final Logger LOGGER = LogManager.getLogger(OpenSslNativeCipher.class);

    public static final int AES_BLOCK_SIZE = 16;
    public static final int AES_256_KEY_SIZE = 32;
    public static final int COUNTER_SIZE = 4;

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
            // Common Homebrew path for OpenSSL on macOS
            String[] macPaths = {
                "/opt/homebrew/opt/openssl@3/lib/libcrypto.dylib",   // Apple Silicon (M1/M2)
                "/usr/local/opt/openssl@3/lib/libcrypto.dylib"       // Intel Macs
            };

            for (String path : macPaths) {
                Path p = Path.of(path);
                if (Files.exists(p)) {
                    return SymbolLookup.libraryLookup(p, Arena.global());
                }
            }

            throw new RuntimeException("Could not find libcrypto.dylib in expected macOS locations.");
        } else if (os.contains("linux")) {
            try {
                // Try OpenSSL 3 first
                String[] linuxPaths = {
                    "/lib64/libcrypto.so.3",     // OpenSSL 3
                    "/lib64/libcrypto.so.1.1",   // OpenSSL 1.1 fallback
                    "/lib64/libcrypto.so.10",    // Legacy systems
                    "/lib64/libcrypto.so",       // Generic symlink
                    "/lib/libcrypto.so.3",
                    "/lib/libcrypto.so" };

                for (String path : linuxPaths) {
                    Path p = Path.of(path);
                    if (Files.exists(p)) {
                        return SymbolLookup.libraryLookup(p, Arena.global());
                    }
                }

                throw new RuntimeException("Could not find libcrypto in known Linux paths.");
            } catch (RuntimeException e) {
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

    public static byte[] computeOffsetIV(byte[] baseIV, long offset) {
        byte[] ivCopy = Arrays.copyOf(baseIV, baseIV.length);
        int blockOffset = (int) (offset / AesCipherFactory.AES_BLOCK_SIZE_BYTES);

        ivCopy[AesCipherFactory.IV_ARRAY_LENGTH - 1] = (byte) blockOffset;
        ivCopy[AesCipherFactory.IV_ARRAY_LENGTH - 2] = (byte) (blockOffset >>> 8);
        ivCopy[AesCipherFactory.IV_ARRAY_LENGTH - 3] = (byte) (blockOffset >>> 16);
        ivCopy[AesCipherFactory.IV_ARRAY_LENGTH - 4] = (byte) (blockOffset >>> 24);

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

                byte[] adjustedIV = computeOffsetIV(iv, filePosition);
                MemorySegment keySeg = arena.allocateArray(ValueLayout.JAVA_BYTE, key);
                MemorySegment ivSeg = arena.allocateArray(ValueLayout.JAVA_BYTE, adjustedIV);

                int rc = (int) EVP_EncryptInit_ex.invoke(ctx, cipher, MemorySegment.NULL, keySeg, ivSeg);
                if (rc != 1) {
                    throw new OpenSslException("EVP_EncryptInit_ex failed");
                }

                int skipBytes = (int) (filePosition % AES_BLOCK_SIZE);
                if (skipBytes > 0) {
                    MemorySegment dummyIn = arena.allocateArray(ValueLayout.JAVA_BYTE, skipBytes);
                    MemorySegment dummyOut = arena.allocate(skipBytes + AES_BLOCK_SIZE);
                    MemorySegment dummyLen = arena.allocate(ValueLayout.JAVA_INT);
                    EVP_EncryptUpdate.invoke(ctx, dummyOut, dummyLen, dummyIn, skipBytes);
                }

                MemorySegment inSeg = arena.allocateArray(ValueLayout.JAVA_BYTE, input);
                MemorySegment outSeg = arena.allocate(input.length + AES_BLOCK_SIZE);
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

    public static int encryptInto(Arena arena, byte[] key, byte[] iv, MemorySegment input, MemorySegment output, long filePosition)
        throws Throwable {
        if (key.length != AES_256_KEY_SIZE || iv.length != AES_BLOCK_SIZE) {
            throw new IllegalArgumentException("Invalid key or IV length");
        }

        MemorySegment ctx = (MemorySegment) EVP_CIPHER_CTX_new.invoke();
        if (ctx.address() == 0)
            throw new OpenSslException("EVP_CIPHER_CTX_new failed");

        try {
            MemorySegment cipher = (MemorySegment) EVP_aes_256_ctr.invoke();
            if (cipher.address() == 0)
                throw new OpenSslException("EVP_aes_256_ctr failed");

            byte[] adjustedIV = computeOffsetIV(iv, filePosition);
            MemorySegment keySeg = arena.allocateArray(ValueLayout.JAVA_BYTE, key);
            MemorySegment ivSeg = arena.allocateArray(ValueLayout.JAVA_BYTE, adjustedIV);

            int rc = (int) EVP_EncryptInit_ex.invoke(ctx, cipher, MemorySegment.NULL, keySeg, ivSeg);
            if (rc != 1)
                throw new OpenSslException("EVP_EncryptInit_ex failed");

            // handle partial block alignment (CTR skip)
            int skip = (int) (filePosition % AES_BLOCK_SIZE);
            if (skip > 0) {
                MemorySegment dummyIn = arena.allocateArray(ValueLayout.JAVA_BYTE, skip);
                MemorySegment dummyOut = arena.allocate(skip + AES_BLOCK_SIZE);
                MemorySegment dummyLen = arena.allocate(ValueLayout.JAVA_INT);
                EVP_EncryptUpdate.invoke(ctx, dummyOut, dummyLen, dummyIn, skip);
            }

            MemorySegment outLen = arena.allocate(ValueLayout.JAVA_INT);

            rc = (int) EVP_EncryptUpdate
                .invoke(
                    ctx,
                    output,              // write directly into the provided aligned MemorySegment
                    outLen,
                    input,
                    (int) input.byteSize()
                );
            if (rc != 1)
                throw new OpenSslException("EVP_EncryptUpdate failed");

            return outLen.get(ValueLayout.JAVA_INT, 0);
        } finally {
            EVP_CIPHER_CTX_free.invoke(ctx);
        }

    }

    public static MemorySegment decryptInto(long srcAddr, long length, byte[] key, byte[] iv, long fileOffset, Arena arena)
        throws Throwable {
        if (key == null || key.length != AES_256_KEY_SIZE)
            throw new IllegalArgumentException("Key must be 32 bytes for AES-256-CTR");
        if (iv == null || iv.length != AES_BLOCK_SIZE)
            throw new IllegalArgumentException("IV must be 16 bytes for AES-CTR");

        MemorySegment ctx = (MemorySegment) EVP_CIPHER_CTX_new.invoke();
        if (ctx.address() == 0)
            throw new OpenSslException("EVP_CIPHER_CTX_new failed");

        try {
            MemorySegment cipher = (MemorySegment) EVP_aes_256_ctr.invoke();
            if (cipher.address() == 0)
                throw new OpenSslException("EVP_aes_256_ctr failed");

            byte[] adjustedIV = computeOffsetIV(iv, fileOffset);
            MemorySegment keySeg = arena.allocateArray(ValueLayout.JAVA_BYTE, key);
            MemorySegment ivSeg = arena.allocateArray(ValueLayout.JAVA_BYTE, adjustedIV);

            int rc = (int) EVP_EncryptInit_ex.invoke(ctx, cipher, MemorySegment.NULL, keySeg, ivSeg);
            if (rc != 1)
                throw new OpenSslException("EVP_EncryptInit_ex failed");

            // Direct mapping of source
            MemorySegment src = MemorySegment.ofAddress(srcAddr).reinterpret(length);

            // Allocate native memory for decrypted data
            MemorySegment dst = arena.allocate(length);
            MemorySegment outLen = arena.allocate(ValueLayout.JAVA_INT);

            rc = (int) EVP_EncryptUpdate.invoke(ctx, dst, outLen, src, (int) length);
            if (rc != 1)
                throw new OpenSslException("EVP_EncryptUpdate failed");

            return dst;
        } finally {
            EVP_CIPHER_CTX_free.invoke(ctx);
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

                // Compute IV with offset counter
                byte[] adjustedIV = computeOffsetIV(iv, filePosition);
                MemorySegment keySeg = arena.allocateArray(ValueLayout.JAVA_BYTE, key);
                MemorySegment ivSeg = arena.allocateArray(ValueLayout.JAVA_BYTE, adjustedIV);

                int rc = (int) EVP_EncryptInit_ex.invoke(ctx, cipher, MemorySegment.NULL, keySeg, ivSeg);
                if (rc != 1) {
                    throw new OpenSslException("EVP_EncryptInit_ex failed");
                }

                // Skip any partial block
                int partialOffset = (int) (filePosition % AES_BLOCK_SIZE);
                if (partialOffset > 0) {
                    MemorySegment dummyIn = arena.allocateArray(ValueLayout.JAVA_BYTE, partialOffset);
                    MemorySegment dummyOut = arena.allocate(partialOffset + AES_BLOCK_SIZE);
                    MemorySegment dummyLen = arena.allocate(ValueLayout.JAVA_INT);
                    EVP_EncryptUpdate.invoke(ctx, dummyOut, dummyLen, dummyIn, partialOffset);
                }

                MemorySegment inSeg = arena.allocateArray(ValueLayout.JAVA_BYTE, input);
                MemorySegment outSeg = arena.allocate(input.length + AES_BLOCK_SIZE);
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

    public static void decryptInPlace(long addr, long length, byte[] key, byte[] iv, long fileOffset) throws Throwable {
        if (key == null || key.length != AES_256_KEY_SIZE)
            throw new IllegalArgumentException("Key must be 32 bytes for AES-256-CTR");
        if (iv == null || iv.length != AES_BLOCK_SIZE)
            throw new IllegalArgumentException("IV must be 16 bytes for AES-CTR");

        long tStart = System.nanoTime();

        try (Arena arena = Arena.ofConfined()) {
            long tCtxStart = System.nanoTime();
            MemorySegment ctx = (MemorySegment) EVP_CIPHER_CTX_new.invoke();
            long tCtxAlloc = System.nanoTime();
            if (ctx.address() == 0)
                throw new OpenSslException("EVP_CIPHER_CTX_new failed");

            try {
                long tCipherStart = System.nanoTime();
                MemorySegment cipher = (MemorySegment) EVP_aes_256_ctr.invoke();
                long tCipherLookup = System.nanoTime();
                if (cipher.address() == 0)
                    throw new OpenSslException("EVP_aes_256_ctr failed");

                byte[] adjustedIV = computeOffsetIV(iv, fileOffset);
                long tKeyIvStart = System.nanoTime();
                MemorySegment keySeg = arena.allocateArray(ValueLayout.JAVA_BYTE, key);
                MemorySegment ivSeg = arena.allocateArray(ValueLayout.JAVA_BYTE, adjustedIV);
                long tKeyIvAlloc = System.nanoTime();

                int rc = (int) EVP_EncryptInit_ex.invoke(ctx, cipher, MemorySegment.NULL, keySeg, ivSeg);
                long tInit = System.nanoTime();
                if (rc != 1)
                    throw new OpenSslException("EVP_EncryptInit_ex failed");

                int partialBlockOffset = (int) (fileOffset % AES_BLOCK_SIZE);
                if (partialBlockOffset > 0) {
                    MemorySegment dummyIn = arena.allocateArray(ValueLayout.JAVA_BYTE, partialBlockOffset);
                    MemorySegment dummyOut = arena.allocate(partialBlockOffset + AES_BLOCK_SIZE);
                    MemorySegment dummyLen = arena.allocate(ValueLayout.JAVA_INT);
                    EVP_EncryptUpdate.invoke(ctx, dummyOut, dummyLen, dummyIn, partialBlockOffset);
                }

                MemorySegment inOut = MemorySegment.ofAddress(addr).reinterpret(length);
                MemorySegment outLen = arena.allocate(ValueLayout.JAVA_INT);

                long tUpdateStart = System.nanoTime();
                rc = (int) EVP_EncryptUpdate.invoke(ctx, inOut, outLen, inOut, (int) length);
                long tUpdateEnd = System.nanoTime();

                if (rc != 1)
                    throw new OpenSslException("EVP_EncryptUpdate failed");

                // Final log
                long tEnd = System.nanoTime();
                LOGGER
                    .trace(
                        """
                            Decryption breakdown ({} MiB at offset {}):
                             > ctx alloc: {} \u00b5s
                             > cipher lookup: {} \u00b5s
                             > key/iv alloc: {} \u00b5s
                             > init cipher: {} \u00b5s
                             > update decrypt: {} \u00b5s
                             > total time: {} \u00b5s""",
                        String.format("%.2f", length / 1048576.0),
                        fileOffset,
                        (tCtxAlloc - tCtxStart) / 1_000,
                        (tCipherLookup - tCipherStart) / 1_000,
                        (tKeyIvAlloc - tKeyIvStart) / 1_000,
                        (tInit - tKeyIvAlloc) / 1_000,
                        (tUpdateEnd - tUpdateStart) / 1_000,
                        (tEnd - tStart) / 1_000
                    );

            } finally {
                EVP_CIPHER_CTX_free.invoke(ctx);
            }
        }
    }

    public static void decryptInPlace(Arena arena, long addr, long length, byte[] key, byte[] iv, long fileOffset) throws Throwable {
        if (key == null || key.length != AES_256_KEY_SIZE)
            throw new IllegalArgumentException("Key must be 32 bytes for AES-256-CTR");
        if (iv == null || iv.length != AES_BLOCK_SIZE)
            throw new IllegalArgumentException("IV must be 16 bytes for AES-CTR");

        MemorySegment ctx = (MemorySegment) EVP_CIPHER_CTX_new.invoke();
        if (ctx.address() == 0)
            throw new OpenSslException("EVP_CIPHER_CTX_new failed");

        try {
            MemorySegment cipher = (MemorySegment) EVP_aes_256_ctr.invoke();
            if (cipher.address() == 0)
                throw new OpenSslException("EVP_aes_256_ctr failed");

            byte[] adjustedIV = computeOffsetIV(iv, fileOffset);
            MemorySegment keySeg = arena.allocateArray(ValueLayout.JAVA_BYTE, key);
            MemorySegment ivSeg = arena.allocateArray(ValueLayout.JAVA_BYTE, adjustedIV);

            int rc = (int) EVP_EncryptInit_ex.invoke(ctx, cipher, MemorySegment.NULL, keySeg, ivSeg);
            if (rc != 1)
                throw new OpenSslException("EVP_EncryptInit_ex failed");

            int partialBlockOffset = (int) (fileOffset % AES_BLOCK_SIZE);
            if (partialBlockOffset > 0) {
                MemorySegment dummyIn = arena.allocateArray(ValueLayout.JAVA_BYTE, partialBlockOffset);
                MemorySegment dummyOut = arena.allocate(partialBlockOffset + AES_BLOCK_SIZE);
                MemorySegment dummyLen = arena.allocate(ValueLayout.JAVA_INT);
                EVP_EncryptUpdate.invoke(ctx, dummyOut, dummyLen, dummyIn, partialBlockOffset);
            }

            MemorySegment inOut = MemorySegment.ofAddress(addr).reinterpret(length, arena, null);
            MemorySegment outLen = arena.allocate(ValueLayout.JAVA_INT);

            rc = (int) EVP_EncryptUpdate.invoke(ctx, inOut, outLen, inOut, (int) length);

            if (rc != 1)
                throw new OpenSslException("EVP_EncryptUpdate failed");

        } finally {
            EVP_CIPHER_CTX_free.invoke(ctx);
        }
    }

    private OpenSslNativeCipher() {
        // Utility class
    }
}

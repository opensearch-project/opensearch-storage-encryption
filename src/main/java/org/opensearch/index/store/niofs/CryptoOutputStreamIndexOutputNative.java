/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.niofs;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import org.apache.lucene.store.OutputStreamIndexOutput;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.cipher.OpenSslPanamaCipher;
import org.opensearch.index.store.cipher.OpenSslPanamaCipher.OpenSslException;

/**
 * An IndexOutput implementation that encrypts data before writing using native OpenSSL AES-CTR.
 *
 * @opensearch.internal
 */
@SuppressWarnings("preview")
@SuppressForbidden(reason = "temporary bypass")
public final class CryptoOutputStreamIndexOutputNative extends OutputStreamIndexOutput {

    static final int CHUNK_SIZE = 8192;

    /**
     * Creates a new CryptoIndexOutput
     *
     * @param name The name of the output
     * @param path The path to write to
     * @param os The output stream
     * @param key The AES key (must be 32 bytes for AES-256)
     * @param iv The initialization vector (must be 16 bytes)
     * @throws IOException If there is an I/O error
     * @throws IllegalArgumentException If key or iv lengths are invalid
     */
    public CryptoOutputStreamIndexOutputNative(String name, Path path, OutputStream os, byte[] key, byte[] iv) throws IOException {
        super("FSIndexOutput(path=\"" + path + "\")", name, new EncryptedOutputStream(os, key, iv), CHUNK_SIZE);
    }

    @SuppressWarnings("preview")
    @SuppressForbidden(reason = "temporary bypass")
    private static class EncryptedOutputStream extends FilterOutputStream {
        private static final int BUFFER_SIZE = 65536;
        private static final int AES_BLOCK_SIZE = 16;

        private final Arena arena;
        private final MemorySegment ctx;
        private final byte[] buffer;
        private int bufferPosition = 0;
        private boolean isClosed = false;

        EncryptedOutputStream(OutputStream os, byte[] key, byte[] iv) throws IOException {
            super(os);
            try {
                this.arena = Arena.ofConfined();
                this.ctx = (MemorySegment) OpenSslPanamaCipher.EVP_CIPHER_CTX_new.invoke();
                if (ctx.address() == 0) {
                    throw new OpenSslException("Failed to create cipher context");
                }

                MemorySegment cipher = (MemorySegment) OpenSslPanamaCipher.EVP_aes_256_ctr.invoke();
                if (cipher.address() == 0) {
                    throw new OpenSslException("Failed to create cipher");
                }

                MemorySegment keySeg = arena.allocateArray(ValueLayout.JAVA_BYTE, key);
                MemorySegment ivSeg = arena.allocateArray(ValueLayout.JAVA_BYTE, iv);

                int rc = (int) OpenSslPanamaCipher.EVP_EncryptInit_ex.invoke(ctx, cipher, MemorySegment.NULL, keySeg, ivSeg);
                if (rc != 1) {
                    throw new OpenSslException("EVP_EncryptInit_ex failed");
                }

                this.buffer = new byte[BUFFER_SIZE];
            } catch (Throwable t) {
                throw new IOException("Failed to initialize encryption", t);
            }
        }

        @Override
        public void write(byte[] b, int offset, int length) throws IOException {
            checkClosed();
            if (b == null) {
                throw new NullPointerException("Input buffer cannot be null");
            }
            if (offset < 0 || length < 0 || offset + length > b.length) {
                throw new IndexOutOfBoundsException("Invalid offset or length");
            }
            if (length == 0) {
                return;
            }

            if (length >= BUFFER_SIZE) {
                flushBuffer();
                processAndWrite(b, offset, length);
            } else if (bufferPosition + length > BUFFER_SIZE) {
                flushBuffer();
                System.arraycopy(b, offset, buffer, bufferPosition, length);
                bufferPosition += length;
            } else {
                System.arraycopy(b, offset, buffer, bufferPosition, length);
                bufferPosition += length;
            }
        }

        private void processAndWrite(byte[] data, int offset, int length) throws IOException {
            try {
                // Create ByteBuffer from the input data
                ByteBuffer input = ByteBuffer.wrap(data, offset, length);
                ByteBuffer output = ByteBuffer.allocate(length + AES_BLOCK_SIZE);

                // Convert ByteBuffers to MemorySegments
                MemorySegment inSeg = MemorySegment.ofBuffer(input);
                MemorySegment outSeg = MemorySegment.ofBuffer(output);
                MemorySegment outLen = arena.allocate(ValueLayout.JAVA_INT);

                int rc = (int) OpenSslPanamaCipher.EVP_EncryptUpdate.invoke(ctx, outSeg, outLen, inSeg, length);
                if (rc != 1) {
                    throw new OpenSslException("EVP_EncryptUpdate failed");
                }

                int written = outLen.get(ValueLayout.JAVA_INT, 0);
                out.write(output.array(), 0, written);
            } catch (Throwable t) {
                throw new IOException("Encryption failed", t);
            }
        }

        private void flushBuffer() throws IOException {
            if (bufferPosition > 0) {
                processAndWrite(buffer, 0, bufferPosition);
                bufferPosition = 0;
            }
        }

        @Override
        public void write(int b) throws IOException {
            checkClosed();
            if (bufferPosition >= BUFFER_SIZE) {
                flushBuffer();
            }
            buffer[bufferPosition++] = (byte) b;
        }

        private void checkClosed() throws IOException {
            if (isClosed) {
                throw new IOException("Stream is closed");
            }
        }

        @Override
        public void close() throws IOException {
            if (isClosed) {
                return;
            }

            IOException exception = null;
            try {
                flushBuffer();
            } catch (IOException e) {
                exception = e;
            }

            try {
                OpenSslPanamaCipher.EVP_CIPHER_CTX_free.invoke(ctx);
            } catch (Throwable t) {
                if (exception == null) {
                    exception = new IOException("Failed to free OpenSSL context", t);
                }
            }

            try {
                arena.close();
            } catch (Exception e) {
                if (exception == null) {
                    exception = new IOException("Failed to close arena", e);
                }
            }

            try {
                super.close();
            } catch (IOException e) {
                if (exception == null) {
                    exception = e;
                }
            }

            isClosed = true;
            if (exception != null) {
                throw exception;
            }
        }

        @Override
        public void flush() throws IOException {
            checkClosed();
            flushBuffer();
            out.flush();
        }
    }
}

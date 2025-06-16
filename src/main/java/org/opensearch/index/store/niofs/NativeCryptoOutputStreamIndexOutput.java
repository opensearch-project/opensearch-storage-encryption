/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.niofs;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

import org.apache.lucene.store.OutputStreamIndexOutput;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.cipher.OpenSslNativeCipher;

/**
 * An IndexOutput implementation that encrypts data before writing using native OpenSSL AES-CTR.
 *
 * @opensearch.internal
 */
@SuppressForbidden(reason = "temporary bypass")
public final class NativeCryptoOutputStreamIndexOutput extends OutputStreamIndexOutput {

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
    public NativeCryptoOutputStreamIndexOutput(String name, Path path, OutputStream os, byte[] key, byte[] iv) throws IOException {
        super("FSIndexOutput(path=\"" + path + "\")", name, new EncryptedOutputStream(os, key, iv), CHUNK_SIZE);
    }

    private static class EncryptedOutputStream extends FilterOutputStream {
        private static final int BUFFER_SIZE = 65536;

        private final byte[] key;
        private final byte[] iv;
        private final byte[] buffer;
        private int bufferPosition = 0;
        private long streamOffset = 0;
        private boolean isClosed = false;

        EncryptedOutputStream(OutputStream os, byte[] key, byte[] iv) {
            super(os);
            this.key = key;
            this.iv = iv;
            this.buffer = new byte[BUFFER_SIZE];
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
            if (length == 0)
                return;

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

        @Override
        public void write(int b) throws IOException {
            checkClosed();
            if (bufferPosition >= BUFFER_SIZE) {
                flushBuffer();
            }
            buffer[bufferPosition++] = (byte) b;
        }

        private void flushBuffer() throws IOException {
            if (bufferPosition > 0) {
                processAndWrite(buffer, 0, bufferPosition);
                bufferPosition = 0;
            }
        }

        private void processAndWrite(byte[] data, int offset, int length) throws IOException {
            try {
                byte[] encrypted = OpenSslNativeCipher.encrypt(key, iv, slice(data, offset, length), streamOffset);
                out.write(encrypted);
                streamOffset += length;
            } catch (Throwable t) {
                throw new IOException("Encryption failed at offset " + streamOffset, t);
            }
        }

        private byte[] slice(byte[] data, int offset, int length) {
            if (offset == 0 && length == data.length) {
                return data;
            }
            byte[] sliced = new byte[length];
            System.arraycopy(data, offset, sliced, 0, length);
            return sliced;
        }

        @Override
        public void flush() throws IOException {
            checkClosed();
            flushBuffer();
            out.flush();
        }

        @Override
        public void close() throws IOException {
            if (isClosed)
                return;

            IOException exception = null;
            try {
                flushBuffer();
            } catch (IOException e) {
                exception = e;
            }

            try {
                super.close();
            } catch (IOException e) {
                if (exception == null)
                    exception = e;
            }

            isClosed = true;
            if (exception != null)
                throw exception;
        }

        private void checkClosed() throws IOException {
            if (isClosed) {
                throw new IOException("Stream is closed");
            }
        }
    }
}

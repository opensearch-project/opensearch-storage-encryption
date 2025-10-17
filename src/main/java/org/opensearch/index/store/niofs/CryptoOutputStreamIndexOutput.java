/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.niofs;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.security.Key;

import javax.crypto.Cipher;

import org.apache.lucene.store.OutputStreamIndexOutput;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.cipher.AesGcmCipherFactory;
import org.opensearch.index.store.cipher.OpenSslNativeCipher;

/**
 * An IndexOutput implementation that encrypts data before writing using native
 * OpenSSL AES-CTR.
 *
 * @opensearch.internal
 */
@SuppressForbidden(reason = "temporary bypass")
public final class CryptoOutputStreamIndexOutput extends OutputStreamIndexOutput {

    private static final int CHUNK_SIZE = 8_192;
    private static final int BUFFER_SIZE = 65_536;

    /**
     * Creates a new CryptoIndexOutput
     *
     * @param name The name of the output
     * @param path The path to write to
     * @param os The output stream
     * @param key The AES key
     * @param iv The initialization vector (must be 16 bytes)
     * @param provider The JCE provider to use
     * @throws IOException If there is an I/O error
     * @throws IllegalArgumentException If key or iv lengths are invalid
     */
    public CryptoOutputStreamIndexOutput(String name, Path path, OutputStream os, Key key, byte[] iv, java.security.Provider provider)
        throws IOException {
        super("FSIndexOutput(path=\"" + path + "\")", name, new EncryptedOutputStream(os, key, iv, provider), CHUNK_SIZE);
    }

    private static class EncryptedOutputStream extends FilterOutputStream {

        private final Key key;
        private final byte[] iv;
        private final byte[] buffer;
        private final Cipher cipher;
        private int bufferPosition = 0;
        private long streamOffset = 0;
        private boolean isClosed = false;

        EncryptedOutputStream(OutputStream os, Key key, byte[] iv, java.security.Provider provider) {
            super(os);
            this.key = key;
            this.iv = iv;
            this.buffer = new byte[BUFFER_SIZE];
            this.cipher = AesGcmCipherFactory.getCipher(provider);
            AesGcmCipherFactory.initCipher(this.cipher, key, iv, Cipher.ENCRYPT_MODE, streamOffset);
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
                byte[] encrypted = OpenSslNativeCipher.encrypt(key.getEncoded(), iv, slice(data, offset, length), streamOffset);
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
        public void close() throws IOException {
            IOException exception = null;

            try {
                checkClosed();
                flushBuffer();
                // Lucene writes footer here.
                // this will also flush the buffer.
                // Finalize GCM and handle any remaining encrypted bytes
                byte[] finalData = org.opensearch.index.store.cipher.AesGcmCipherFactory.finalizeAndGetTag(cipher);
                // finalData contains [remaining_encrypted_bytes][16_byte_tag]
                // Write any remaining encrypted bytes (excluding the tag)
                if (finalData.length > AesGcmCipherFactory.GCM_TAG_LENGTH) {
                    out.write(finalData, 0, finalData.length - AesGcmCipherFactory.GCM_TAG_LENGTH);
                }
                super.close();
            } catch (IOException e) {
                exception = e;
            } finally {
                isClosed = true;
            }

            if (exception != null)
                throw exception;
        }

        private void checkClosed() throws IOException {
            if (isClosed) {
                throw new IOException("Outout stream is already closed, this is unusual");
            }
        }
    }
}

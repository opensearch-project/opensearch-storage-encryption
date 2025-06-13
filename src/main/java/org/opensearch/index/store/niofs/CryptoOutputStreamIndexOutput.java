/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.niofs;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;

import org.apache.lucene.store.OutputStreamIndexOutput;

/**
 * An IndexOutput implementation that encrypts data before writing
 *
 * @opensearch.internal
 */

public final class CryptoOutputStreamIndexOutput extends OutputStreamIndexOutput {

    static final int CHUNK_SIZE = 8192;

    /**
    * Creates a new CryptoIndexOutput
    *
    * @param name The name of the output
    * @param path The path to write to
    * @param os The output stream
    * @param cipher The cipher to use for encryption
    * @throws IOException If there is an I/O error
    */
    public CryptoOutputStreamIndexOutput(String name, Path path, OutputStream os, Cipher cipher) throws IOException {
        super("FSIndexOutput(path=\"" + path + "\")", name, new EncryptedOutputStream(os, cipher), CHUNK_SIZE);
    }

    private static class EncryptedOutputStream extends FilterOutputStream {
        private final Cipher cipher;
        private final byte[] buffer;
        private int bufferPosition = 0;
        private static final int BUFFER_SIZE = 65536; // Increased buffer size (64KB)

        EncryptedOutputStream(OutputStream os, Cipher cipher) {
            super(os);
            this.cipher = cipher;
            this.buffer = new byte[BUFFER_SIZE];
        }

        @Override
        public void write(byte[] b, int offset, int length) throws IOException {
            if (length >= BUFFER_SIZE) {
                // For large writes, flush any buffered content first
                flushBuffer();

                // Process large chunks directly
                processAndWrite(b, offset, length);
            } else if (bufferPosition + length > BUFFER_SIZE) {
                // Buffer would overflow, flush first
                flushBuffer();
                System.arraycopy(b, offset, buffer, bufferPosition, length);
                bufferPosition += length;
            } else {
                // Add to buffer
                System.arraycopy(b, offset, buffer, bufferPosition, length);
                bufferPosition += length;
            }
        }

        private void processAndWrite(byte[] data, int offset, int length) throws IOException {
            try {
                byte[] encrypted = cipher.update(data, offset, length);
                if (encrypted != null && encrypted.length > 0) {
                    out.write(encrypted);
                }
            } catch (IllegalStateException e) {
                throw new IOException("Cipher update failed: " + e.getMessage(), e);
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
            if (bufferPosition >= BUFFER_SIZE) {
                flushBuffer();
            }
            buffer[bufferPosition++] = (byte) b;
        }

        @Override
        public void close() throws IOException {
            try {
                flushBuffer();
                byte[] finalBytes = cipher.doFinal();
                if (finalBytes != null && finalBytes.length > 0) {
                    out.write(finalBytes);
                }
            } catch (IllegalBlockSizeException | BadPaddingException e) {
                throw new IOException("Cipher finalization failed", e);
            } finally {
                super.close();
            }
        }

        @Override
        public void flush() throws IOException {
            flushBuffer();
            out.flush();
        }
    }
}

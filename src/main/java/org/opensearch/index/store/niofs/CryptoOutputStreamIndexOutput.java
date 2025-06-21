/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.niofs;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.Key;

import org.apache.lucene.store.OutputStreamIndexOutput;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.cipher.EncryptionMetadataTrailer;
import org.opensearch.index.store.cipher.OpenSslNativeCipher;
import org.opensearch.index.store.key.KeyResolver;

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

    public CryptoOutputStreamIndexOutput(String name, Path path, OutputStream os, KeyResolver keyResolver) throws IOException {
        super("FSIndexOutput(path=\"" + path + "\")", name, new EncryptedOutputStream(name, path, os, keyResolver), CHUNK_SIZE);
    }

    private static class EncryptedOutputStream extends FilterOutputStream {

        /**
        * Object used to prevent a race on the 'closed' instance variable.
        */
        private final Object closeLock = new Object();
        private final Key key;
        private final byte[] buffer;

        private int bufferPosition = 0;
        private long streamOffset = 0;
        private boolean isClosed = false;

        EncryptedOutputStream(String name, Path path, OutputStream os, KeyResolver keyResolver) throws IOException {
            super(os);
            this.key = keyResolver.deriveNewFileEncryptionKey(name);
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

        @Override
        public void close() throws IOException {
            if (isClosed) {
                return;
            }
            synchronized (closeLock) {
                if (isClosed) {
                    return;
                }
                isClosed = true;
            }

            Throwable flushException = null;
            try {
                flushBuffer();

                out.write(EncryptionMetadataTrailer.ENCRYPTION_MAGIC_STRING.getBytes(StandardCharsets.UTF_8));
                out.write(intToBytes(EncryptionMetadataTrailer.ENCRYPTION_KEY_FORMAT_VERSION));
                out.write(key.getEncoded());

                // flush to disk.
                super.flush();
            } catch (Throwable e) {
                flushException = e;
                throw e;
            } finally {
                if (flushException == null) {
                    out.close();
                } else {
                    try {
                        out.close();
                    } catch (Throwable closeException) {
                        if (flushException != closeException) {
                            closeException.addSuppressed(flushException);
                        }
                        throw closeException;
                    }
                }
            }
        }

        private void processAndWrite(byte[] data, int offset, int length) throws IOException {
            try {
                byte[] encrypted = OpenSslNativeCipher.encrypt(key.getEncoded(), slice(data, offset, length), streamOffset);
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

        private void flushBuffer() throws IOException {
            if (bufferPosition > 0) {
                processAndWrite(buffer, 0, bufferPosition);
                bufferPosition = 0;
            }
        }

        private void checkClosed() throws IOException {
            if (isClosed) {
                throw new IOException("Stream is closed");
            }
        }
    }

    private static byte[] intToBytes(int value) {
        return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value };
    }
}

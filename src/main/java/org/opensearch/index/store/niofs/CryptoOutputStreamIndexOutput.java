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
import java.security.Provider;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;

import javax.crypto.Cipher;

import org.apache.lucene.store.OutputStreamIndexOutput;
import org.opensearch.index.store.cipher.CipherFactory;

/**
 * An IndexOutput implementation that encrypts data before writing
 *
 * @opensearch.internal
 */

public final class CryptoOutputStreamIndexOutput extends OutputStreamIndexOutput {

    private static final int CHUNK_SIZE = 8192;                   // Lucene chunk size
    private static final int BUFFER_SIZE = 131072;                // 128 KB (16 chunks)
    private static final int PARALLEL_THRESHOLD_BYTES = 1048576;  // 1 MB (128 chunks)

    public CryptoOutputStreamIndexOutput(String name, Path path, OutputStream os, Key key, byte[] iv, Provider provider)
        throws IOException {
        super("FSIndexOutput(path=\"" + path + "\")", name, new EncryptedOutputStream(os, key, iv, provider), CHUNK_SIZE);
    }

    private static class EncryptedOutputStream extends FilterOutputStream {

        private final Key key;
        private final byte[] baseIV;
        private final Provider provider;
        private final byte[] buffer;

        private int bufferPosition = 0;
        private long streamOffset = 0L;

        EncryptedOutputStream(OutputStream os, Key key, byte[] baseIV, Provider provider) {
            super(os);
            this.key = key;
            this.baseIV = baseIV;
            this.provider = provider;
            this.buffer = new byte[BUFFER_SIZE];
        }

        @Override
        public void write(int b) throws IOException {
            if (bufferPosition >= BUFFER_SIZE) {
                flushBuffer();
            }
            buffer[bufferPosition++] = (byte) b;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (len >= BUFFER_SIZE) {
                flushBuffer();
                processAndWrite(b, off, len);
            } else if (bufferPosition + len > BUFFER_SIZE) {
                flushBuffer();
                System.arraycopy(b, off, buffer, bufferPosition, len);
                bufferPosition += len;
            } else {
                System.arraycopy(b, off, buffer, bufferPosition, len);
                bufferPosition += len;
            }
        }

        @Override
        public void flush() throws IOException {
            flushBuffer();
            out.flush();
        }

        @Override
        public void close() throws IOException {
            try {
                flushBuffer();
            } finally {
                super.close();
            }
        }

        private void flushBuffer() throws IOException {
            if (bufferPosition > 0) {
                processAndWrite(buffer, 0, bufferPosition);
                bufferPosition = 0;
            }
        }

        private void processAndWrite(byte[] data, int offset, int length) throws IOException {
            if (length < PARALLEL_THRESHOLD_BYTES) {
                encryptSequential(data, offset, length);
            } else {
                encryptParallel(data, offset, length);
            }
        }

        private void encryptSequential(byte[] data, int offset, int length) throws IOException {
            try {
                Cipher localCipher = CipherFactory.getCipher(provider);
                CipherFactory.initCipher(localCipher, key, baseIV, Cipher.ENCRYPT_MODE, streamOffset);

                byte[] encrypted = localCipher.update(data, offset, length);
                if (encrypted != null && encrypted.length > 0) {
                    out.write(encrypted);
                }

                streamOffset += length;
            } catch (Exception e) {
                throw new IOException("Sequential encryption failed", e);
            }
        }

        private void encryptParallel(byte[] data, int offset, int length) throws IOException {
            final int blockSize = CipherFactory.AES_BLOCK_SIZE_BYTES;
            final int blocks = (length + blockSize - 1) / blockSize;
            final byte[] encrypted = new byte[length];

            try (ForkJoinPool pool = new ForkJoinPool(Runtime.getRuntime().availableProcessors())) {
                pool.submit(() -> IntStream.range(0, blocks).parallel().forEach(blockIndex -> {
                    int blockOffset = offset + blockIndex * blockSize;
                    int chunkSize = Math.min(blockSize, length - blockIndex * blockSize);
                    long position = streamOffset + blockIndex * blockSize;

                    try {
                        Cipher localCipher = CipherFactory.getCipher(provider);
                        CipherFactory.initCipher(localCipher, key, baseIV, Cipher.ENCRYPT_MODE, position);

                        byte[] encryptedBlock = localCipher.update(data, blockOffset, chunkSize);
                        if (encryptedBlock != null) {
                            System.arraycopy(encryptedBlock, 0, encrypted, blockIndex * blockSize, encryptedBlock.length);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Parallel encryption failed at block " + blockIndex, e);
                    }
                })).get(); // Wait for all threads
            } catch (Exception e) {
                throw new IOException("Parallel encryption failed", e);
            }

            out.write(encrypted);
            streamOffset += length;
        }
    }

}

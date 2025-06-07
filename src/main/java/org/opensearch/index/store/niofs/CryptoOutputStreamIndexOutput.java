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

        EncryptedOutputStream(OutputStream os, Cipher cipher) {
            super(os);
            this.cipher = cipher;
        }

        @Override
        public void write(byte[] b, int offset, int length) throws IOException {
            try {
                while (length > 0) {
                    final int chunk = Math.min(length, CHUNK_SIZE);
                    byte[] res = cipher.update(b, offset, chunk);
                    if (res != null && res.length > 0) {
                        out.write(res);
                    }
                    offset += chunk;
                    length -= chunk;
                }
            } catch (IllegalStateException e) {
                throw new IOException("Cipher update failed: " + e.getMessage(), e);
            }
        }

        @Override
        public void write(int b) throws IOException {
            // delegate to write(byte[], int, int) for single-byte handling
            byte[] oneByte = new byte[] { (byte) b };
            write(oneByte, 0, 1);
        }

        @Override
        public void close() throws IOException {
            try {
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
    }
}

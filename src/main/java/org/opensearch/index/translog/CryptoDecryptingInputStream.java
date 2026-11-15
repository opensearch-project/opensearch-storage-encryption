/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.translog;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.Provider;
import java.util.Set;

import org.opensearch.index.store.key.KeyResolver;

/**
 * InputStream that reads from an encrypted translog file and returns decrypted bytes.
 * 
 * This stream uses CryptoFileChannelWrapper to handle decryption transparently.
 * When the core upload mechanism reads from this stream, it receives plaintext bytes
 * instead of encrypted bytes.
 *
 * @opensearch.internal
 */
public class CryptoDecryptingInputStream extends InputStream {

    private final FileChannel encryptedChannel;
    private final CryptoFileChannelWrapper decryptingChannel;
    private final ByteBuffer buffer;
    private boolean eof = false;

    /**
     * Creates a new CryptoDecryptingInputStream for the specified encrypted file.
     *
     * @param filePath the path to the encrypted translog file
     * @param keyResolver the key resolver for decryption
     * @param translogUUID the translog UUID used for encryption context
     * @param provider the JCE provider for translog cipher and digest
     * @throws IOException if the file cannot be opened or decryption setup fails
     */
    public CryptoDecryptingInputStream(Path filePath, KeyResolver keyResolver, String translogUUID, Provider provider) throws IOException {
        Set<OpenOption> openOptions = Set.of(StandardOpenOption.READ);
        this.encryptedChannel = FileChannel.open(filePath, StandardOpenOption.READ);

        // Wrap with crypto channel for decrypted stream
        this.decryptingChannel = new CryptoFileChannelWrapper(encryptedChannel, keyResolver, filePath, openOptions, translogUUID, provider);

        this.buffer = ByteBuffer.allocate(8192);
        buffer.flip();
    }

    @Override
    public int read() throws IOException {
        if (ensureBufferHasData() == -1) {
            return -1;
        }
        return buffer.get() & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        if (ensureBufferHasData() == -1) {
            return -1;
        }

        int toRead = Math.min(buffer.remaining(), len);
        buffer.get(b, off, toRead);
        return toRead;
    }

    /**
     * Ensures buffer has data to read. Reads and decrypts from channel if needed.
     * 
     * @return number of bytes available in buffer, or -1 if EOF
     * @throws IOException if read or decryption fails
     */
    private int ensureBufferHasData() throws IOException {
        if (eof) {
            return -1;
        }

        if (!buffer.hasRemaining()) {
            buffer.clear();
            int bytesRead = decryptingChannel.read(buffer);

            // Check for EOF
            if (bytesRead <= 0) {
                eof = true;
                return -1;
            }

            buffer.flip();
        }
        return buffer.remaining();
    }

    @Override
    public long skip(long n) throws IOException {
        if (n <= 0) {
            return 0;
        }

        long remaining = n;
        while (remaining > 0 && ensureBufferHasData() != -1) {
            int toSkip = (int) Math.min(buffer.remaining(), remaining);
            buffer.position(buffer.position() + toSkip);
            remaining -= toSkip;
        }
        return n - remaining;
    }

    @Override
    public int available() throws IOException {
        if (eof) {
            return 0;
        }
        return buffer.remaining();
    }

    @Override
    public void close() throws IOException {
        decryptingChannel.close();
    }
}

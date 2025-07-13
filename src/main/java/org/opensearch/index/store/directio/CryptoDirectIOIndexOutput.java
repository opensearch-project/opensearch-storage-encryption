/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIoUtils.getDirectOpenOption;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.store.cipher.OpenSslNativeCipher;
import org.opensearch.index.store.iv.KeyIvResolver;
import org.opensearch.index.store.mmap.PanamaNativeAccess;

public class CryptoDirectIOIndexOutput extends IndexOutput {

    private static final int BUFFER_SIZE = 65_536;
    private static final int BLOCK_SIZE = PanamaNativeAccess.getPageSize(); // often returns 4096

    private final FileChannel channel;
    private final KeyIvResolver keyIvResolver;
    private final ByteBuffer buffer;
    private final Checksum digest;

    private long filePos;
    private boolean isOpen;

    private final byte[] plaintextBuf = new byte[BUFFER_SIZE];
    private final ByteBuffer encryptedBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE + BLOCK_SIZE).alignedSlice(BLOCK_SIZE);

    public CryptoDirectIOIndexOutput(Path path, String name, KeyIvResolver keyIvResolver) throws IOException {
        super("DirectIOIndexOutput(path=\"" + path.toString() + "\")", name);

        this.keyIvResolver = keyIvResolver;
        buffer = ByteBuffer.allocateDirect(BUFFER_SIZE + BLOCK_SIZE - 1).alignedSlice(BLOCK_SIZE);
        channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW, getDirectOpenOption());
        digest = new BufferedChecksum(new CRC32());
        isOpen = true;
    }

    @Override
    public void writeByte(byte b) throws IOException {
        buffer.put(b);
        digest.update(b);
        if (!buffer.hasRemaining()) {
            encryptAndFlushToDisk();
        }
    }

    @Override
    public void writeBytes(byte[] src, int offset, int len) throws IOException {
        int toWrite = len;

        while (toWrite > 0) {
            final int left = buffer.remaining();
            final int chunk = Math.min(left, toWrite);
            buffer.put(src, offset, chunk);
            digest.update(src, offset, chunk);
            offset += chunk;
            toWrite -= chunk;

            if (!buffer.hasRemaining()) {
                encryptAndFlushToDisk();
            }
        }
    }

    private void encryptAndFlushToDisk() throws IOException {
        final int size = buffer.position();
        if (size == 0) {
            return;
        }

        buffer.flip(); // switch to read mode

        // Read into reusable plaintext buffer
        buffer.get(plaintextBuf, 0, size);

        byte[] key = keyIvResolver.getDataKey().getEncoded();
        byte[] iv = keyIvResolver.getIvBytes();

        byte[] encrypted;
        try {
            encrypted = OpenSslNativeCipher.encrypt(key, iv, plaintextBuf, filePos);
        } catch (Throwable t) {
            throw new IOException("Encryption failed at offset " + filePos, t);
        }

        // Compute aligned size
        int encryptedLen = encrypted.length;
        int paddedLen = ((encryptedLen + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE;

        // ✅ Copy encrypted data into aligned ByteBuffer
        encryptedBuffer.clear();
        encryptedBuffer.put(encrypted);

        // Pad any remaining space with zeroes
        for (int i = encryptedLen; i < paddedLen; i++) {
            encryptedBuffer.put((byte) 0);
        }

        encryptedBuffer.flip();

        channel.write(encryptedBuffer, filePos);
        filePos += size; // not paddedLen, since size is from plaintext

        buffer.clear(); // ready for next block

        // zerioize out plaintext
        Arrays.fill(plaintextBuf, 0, size, (byte) 0);
    }

    @Override
    public long getFilePointer() {
        return filePos + buffer.position();
    }

    @Override
    public long getChecksum() {
        return digest.getValue();
    }

    @Override
    public void close() throws IOException {
        if (isOpen) {
            isOpen = false;
            try {
                encryptAndFlushToDisk();
            } finally {
                try (FileChannel ch = channel) {
                    ch.truncate(getFilePointer());
                }
            }
        }
    }
}

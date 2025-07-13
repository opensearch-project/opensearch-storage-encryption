/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIoUtils.getDirectOpenOption;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.store.cipher.OpenSslNativeCipher;
import org.opensearch.index.store.iv.KeyIvResolver;
import org.opensearch.index.store.mmap.PanamaNativeAccess;

@SuppressWarnings("preview")

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
    private final ByteBuffer zeroPaddingBuffer = ByteBuffer.allocate(BLOCK_SIZE);

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

        byte[] key = keyIvResolver.getDataKey().getEncoded();
        byte[] iv = keyIvResolver.getIvBytes();

        int bytesWritten;
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment inputSeg = MemorySegment.ofBuffer(buffer.slice(0, size));
            MemorySegment outputSeg = MemorySegment.ofBuffer(encryptedBuffer);

            bytesWritten = OpenSslNativeCipher.encryptInto(arena, key, iv, inputSeg, outputSeg, filePos);

            inputSeg.fill((byte) 0);
        } catch (Throwable e) {
            throw new IOException(
                String
                    .format(
                        "Encryption failed: offset=%d, inputSize=%d, outputCapacity=%d, error=%s",
                        filePos,
                        size,
                        encryptedBuffer.capacity(),
                        e.getMessage()
                    ),
                e
            );
        }

        if (bytesWritten < size) {
            throw new IOException("Unexpected: fewer bytes written than input size. Possible cipher error.");
        }

        // pad ...
        int paddedLen = ((bytesWritten + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE;
        if (bytesWritten < paddedLen) {
            encryptedBuffer.position(bytesWritten);
            int paddingSize = paddedLen - bytesWritten;
            zeroPaddingBuffer.clear().limit(paddingSize);
            encryptedBuffer.put(zeroPaddingBuffer);
        }

        // Write encrypted + padded data
        encryptedBuffer.position(0).limit(paddedLen);
        int written = channel.write(encryptedBuffer, filePos);

        if (written != paddedLen) {
            throw new IOException(
                String
                    .format(
                        "Incomplete write at offset %d: expected %d bytes, wrote %d bytes. "
                            + "Possible causes: disk full, I/O error, or direct I/O alignment issue.",
                        filePos,
                        paddedLen,
                        written
                    )
            );
        }

        filePos += size;
        buffer.clear();

        MemorySegment.ofBuffer(encryptedBuffer).fill((byte) 0);
        encryptedBuffer.clear();
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

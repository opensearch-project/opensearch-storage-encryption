/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.niofs;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.cipher.OpenSslPanamaCipher;

@SuppressForbidden(reason = "temporary bypass")
final class CryptoBufferedIndexInputNative extends BufferedIndexInput {

    private static final int CHUNK_SIZE = 16_384;

    private final FileChannel channel;
    private final boolean isClone;
    private final long off;
    private final long end;
    private final byte[] key;
    private final byte[] iv;

    private final ByteBuffer tmpBuffer = ByteBuffer.allocate(CHUNK_SIZE);

    public CryptoBufferedIndexInputNative(String resourceDesc, FileChannel fc, IOContext context, byte[] key, byte[] iv)
        throws IOException {
        super(resourceDesc, context);
        this.channel = fc;
        this.off = 0L;
        this.end = fc.size();
        this.key = key;
        this.iv = iv;
        this.isClone = false;
    }

    private CryptoBufferedIndexInputNative(
        String resourceDesc,
        FileChannel fc,
        long off,
        long length,
        int bufferSize,
        byte[] key,
        byte[] iv
    )
        throws IOException {
        super(resourceDesc, bufferSize);
        this.channel = fc;
        this.off = off;
        this.end = off + length;
        this.key = key;
        this.iv = iv;
        this.isClone = true;
    }

    @Override
    public void close() throws IOException {
        if (!isClone) {
            channel.close();
        }
    }

    @Override
    public CryptoBufferedIndexInputNative clone() {
        try {
            return new CryptoBufferedIndexInputNative(
                this.toString(),
                channel,
                off + getFilePointer(),
                length() - getFilePointer(),
                getBufferSize(),
                key,
                iv
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to clone CryptoBufferedIndexInput", e);
        }
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.length()) {
            throw new IllegalArgumentException(
                "slice() " + sliceDescription + " out of bounds: offset=" + offset + ", length=" + length + ", fileLength=" + this.length()
            );
        }
        return new CryptoBufferedIndexInputNative(
            getFullSliceDescription(sliceDescription),
            channel,
            off + offset,
            length,
            getBufferSize(),
            key,
            iv
        );
    }

    @Override
    public long length() {
        return end - off;
    }

    @SuppressForbidden(reason = "FileChannel#read is efficient and used intentionally")
    private int read(ByteBuffer dst, long position) throws IOException {
        tmpBuffer.clear().limit(dst.remaining());
        int bytesRead = channel.read(tmpBuffer, position);
        if (bytesRead == -1) {
            return -1;
        }

        tmpBuffer.flip();
        byte[] encrypted = new byte[bytesRead];
        tmpBuffer.get(encrypted, 0, bytesRead);

        try {
            byte[] decrypted = OpenSslPanamaCipher.decrypt(key, iv, encrypted, position);
            dst.put(decrypted);
            return decrypted.length;
        } catch (Throwable t) {
            throw new IOException("Failed to decrypt block at position " + position, t);
        }
    }

    @Override
    protected void readInternal(ByteBuffer b) throws IOException {
        long pos = getFilePointer() + off;
        if (pos + b.remaining() > end) {
            throw new EOFException("read past EOF: pos=" + pos + ", end=" + end);
        }

        int readLength = b.remaining();
        while (readLength > 0) {
            final int toRead = Math.min(CHUNK_SIZE, readLength);
            b.limit(b.position() + toRead);
            final int bytesRead = read(b, pos);

            if (bytesRead < 0) {
                throw new EOFException("Unexpected EOF while reading decrypted data at pos=" + pos);
            }

            pos += bytesRead;
            readLength -= bytesRead;
        }
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
        if (pos > length()) {
            throw new EOFException("seek past EOF: pos=" + pos + ", length=" + length());
        }
        // No need to reinitialize a cipher context — decryption is stateless per call via Panama
    }
}

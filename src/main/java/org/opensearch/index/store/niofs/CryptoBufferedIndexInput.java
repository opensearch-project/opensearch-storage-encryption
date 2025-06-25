/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.niofs;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.Key;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.cipher.AesCtrCipherFactory;
import org.opensearch.index.store.cipher.EncryptionMetadataTrailer;

/**
 * An IndexInput implementation that decrypts data for reading.
 *
 * @opensearch.internal
 */
final class CryptoBufferedIndexInput extends BufferedIndexInput {

    private static final byte[] ZERO_SKIP = new byte[AesCtrCipherFactory.AES_BLOCK_SIZE_BYTES];
    private static final int CHUNK_SIZE = 16_384;

    private final FileChannel channel;
    private final boolean isClone;
    private final long off;
    private final long end;
    private final long logicalLength;
    private final Key key;
    private final SecretKeySpec keySpec;

    private ByteBuffer tmpBuffer = ByteBuffer.allocate(CHUNK_SIZE);

    // Top-level constructor — subtract's trailer
    public CryptoBufferedIndexInput(String resourceDesc, FileChannel fc, IOContext context, Key key) throws IOException {
        super(resourceDesc, context);
        this.channel = fc;
        this.off = 0L;
        this.end = fc.size();
        this.key = key;
        this.isClone = false;
        this.keySpec = new SecretKeySpec(key.getEncoded(), AesCtrCipherFactory.ALGORITHM);
        // todo add a validation here.
        this.logicalLength = end - off - EncryptionMetadataTrailer.ENCRYPTION_METADATA_TRAILER_SIZE;
    }

    // Slice constructor — do NOT subtract trailer
    public CryptoBufferedIndexInput(String resourceDesc, FileChannel fc, long off, long length, int bufferSize, Key key)
        throws IOException {
        super(resourceDesc, bufferSize);
        this.channel = fc;
        this.off = off;
        this.end = off + length;
        this.key = key;
        this.isClone = true;
        this.keySpec = new SecretKeySpec(key.getEncoded(), AesCtrCipherFactory.ALGORITHM);
        this.logicalLength = end - off; // full slice length — no trailer trimming here
    }

    @Override
    public void close() throws IOException {
        if (!isClone) {
            channel.close();
        }
    }

    @Override
    public CryptoBufferedIndexInput clone() {
        CryptoBufferedIndexInput clone = (CryptoBufferedIndexInput) super.clone();
        clone.tmpBuffer = ByteBuffer.allocate(CHUNK_SIZE);
        return clone;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.length()) {
            throw new IllegalArgumentException(
                "slice() " + sliceDescription + " out of bounds: offset=" + offset + ", length=" + length + ", fileLength=" + this.length()
            );
        }
        return new CryptoBufferedIndexInput(getFullSliceDescription(sliceDescription), channel, off + offset, length, getBufferSize(), key);
    }

    @Override
    public long length() {
        return logicalLength;
    }

    @SuppressForbidden(reason = "FileChannel#read is efficient and used intentionally")
    private int read(ByteBuffer dst, long position) throws IOException {
        tmpBuffer.clear().limit(dst.remaining());
        int bytesRead = channel.read(tmpBuffer, position);
        if (bytesRead == -1) {
            return -1;
        }
        tmpBuffer.flip();

        try {
            Cipher cipher = AesCtrCipherFactory.CIPHER_POOL.get();
            byte[] iv = AesCtrCipherFactory.computeOffsetIV(position);
            cipher.init(Cipher.DECRYPT_MODE, this.keySpec, new IvParameterSpec(iv));

            if (position % AesCtrCipherFactory.AES_BLOCK_SIZE_BYTES > 0) {
                cipher.update(ZERO_SKIP, 0, (int) (position % AesCtrCipherFactory.AES_BLOCK_SIZE_BYTES));
            }

            return (end - position > bytesRead) ? cipher.update(tmpBuffer, dst) : cipher.doFinal(tmpBuffer, dst);

        } catch (Exception ex) {
            throw new IOException("Failed to decrypt block at position " + position, ex);
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
    }
}

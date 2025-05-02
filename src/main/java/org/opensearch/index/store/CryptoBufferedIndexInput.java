/* * SPDX-License-Identifier: Apache-2.0 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.store;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.Optional;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.CryptoDirectory.CipherFactory;

/**
 * An IndexInput implementation that decrypts data for reading
 *
 * @opensearch.internal
 */
final class CryptoBufferedIndexInput extends BufferedIndexInput {
    /** The maximum chunk size for reads of 16384 bytes. */
    private static final int CHUNK_SIZE = 16384;
    ByteBuffer tmpBuffer = ByteBuffer.allocate(CHUNK_SIZE);

    /** the file channel we will read from */
    protected final FileChannel channel;
    /** is this instance a clone and hence does not own the file to close it */
    boolean isClone = false;
    /** start offset: non-zero in the slice case */
    protected final long off;
    /** end offset (start+length) */
    protected final long end;
    InputStream stream;
    Cipher cipher;
    final CryptoDirectory directory;

    public CryptoBufferedIndexInput(String resourceDesc, FileChannel fc, IOContext context, Cipher cipher, CryptoDirectory directory)
        throws IOException {
        super(resourceDesc, context);
        this.channel = fc;
        this.off = 0L;
        this.end = fc.size();
        this.stream = Channels.newInputStream(channel);
        this.cipher = cipher;
        this.directory = directory;
    }

    public CryptoBufferedIndexInput(
        String resourceDesc,
        FileChannel fc,
        long off,
        long length,
        int bufferSize,
        Cipher old,
        CryptoDirectory directory
    ) throws IOException {
        super(resourceDesc, bufferSize);
        this.channel = fc;
        this.off = off;
        this.end = off + length;
        this.isClone = true;
        this.directory = directory;
        this.stream = Channels.newInputStream(channel);
        cipher = CipherFactory.getCipher(old.getProvider());
        CipherFactory.initCipher(cipher, directory, Optional.of(old.getIV()), Cipher.DECRYPT_MODE, off);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        if (!isClone) {
            stream.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CryptoBufferedIndexInput clone() {
        CryptoBufferedIndexInput clone = (CryptoBufferedIndexInput) super.clone();
        clone.isClone = true;
        clone.cipher = CipherFactory.getCipher(cipher.getProvider());
        CipherFactory.initCipher(clone.cipher, directory, Optional.of(cipher.getIV()), Cipher.DECRYPT_MODE, getFilePointer() + off);
        clone.tmpBuffer = ByteBuffer.allocate(CHUNK_SIZE);
        return clone;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.length()) {
            throw new IllegalArgumentException(
                "slice() "
                    + sliceDescription
                    + " out of bounds: offset="
                    + offset
                    + ",length="
                    + length
                    + ",fileLength="
                    + this.length()
                    + ": "
                    + this
            );
        }
        return new CryptoBufferedIndexInput(
            getFullSliceDescription(sliceDescription),
            channel,
            off + offset,
            length,
            getBufferSize(),
            cipher,
            directory
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final long length() {
        return end - off;
    }

    @SuppressForbidden(reason = "FileChannel#read is a faster alternative to synchronized block")
    private int read(ByteBuffer dst, long position) throws IOException {
        int ret;
        int i;
        tmpBuffer.rewind().limit(dst.remaining());
        /* tmpBuffer.rewind();
        // FileChannel#read is forbidden
        /* synchronized (channel) {
            channel.position(position);
            i = stream.read(tmpBuffer.array(), 0, dst.remaining());
        }
        tmpBuffer.limit(i);
        */
        i = channel.read(tmpBuffer, position);
        tmpBuffer.flip();
        try {
            if (end - position > i) ret = cipher.update(tmpBuffer, dst);
            else ret = cipher.doFinal(tmpBuffer, dst);
        } catch (ShortBufferException | IllegalBlockSizeException | BadPaddingException ex) {
            throw new IOException("failed to decrypt blck.", ex);
        }
        return ret;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void readInternal(ByteBuffer b) throws IOException {
        long pos = getFilePointer() + off;

        if (pos + b.remaining() > end) {
            throw new EOFException(
                Thread.currentThread().getName()
                    + " read past EOF: "
                    + this
                    + " isClone? "
                    + isClone
                    + " off: "
                    + off
                    + " pos: "
                    + pos
                    + " end: "
                    + end
            );
        }

        try {
            int readLength = b.remaining();
            while (readLength > 0) {
                final int toRead = Math.min(CHUNK_SIZE, readLength);
                b.limit(b.position() + toRead);
                assert b.remaining() == toRead;
                final int i = read(b, pos);
                if (i < 0) {
                    throw new EOFException("read past EOF: " + this + " buffer: " + b + " chunkLen: " + toRead + " end: " + end);
                }
                assert i > 0 : "FileChannel.read with non zero-length bb.remaining() must always read at least "
                    + "one byte (FileChannel is in blocking mode, see spec of ReadableByteChannel)";
                pos += i;
                readLength -= i;
            }
            assert readLength == 0;
        } catch (IOException ioe) {
            throw new IOException(ioe.getMessage() + ": " + this, ioe);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void seekInternal(long pos) throws IOException {
        if (pos > length()) {
            throw new EOFException(Thread.currentThread().getName() + " read past EOF: pos=" + pos + " vs length=" + length() + ": " + this);
        }
        CipherFactory.initCipher(cipher, directory, Optional.empty(), Cipher.DECRYPT_MODE, pos + off);
    }
}

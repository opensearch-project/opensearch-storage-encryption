/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.mmapfs;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.cipher.CipherFactory;
import org.opensearch.index.store.iv.KeyIvResolver;

public final class MMapCryptoIndexInput extends FilterIndexInput {
    private final Cipher cipher;
    private final KeyIvResolver keyResolver;
    private ByteBuffersDataOutput.ByteBufferRecycler recycler;

    public MMapCryptoIndexInput(String resourceDescription, IndexInput in, Cipher cipher, KeyIvResolver keyResolver) {
        super(resourceDescription, in);
        this.cipher = cipher;
        this.keyResolver = keyResolver;
        recycler = new ByteBuffersDataOutput.ByteBufferRecycler((size) -> { return ByteBuffer.allocate(size); });
    }

    @Override
    public IndexInput clone() {
        Cipher newCipher = CipherFactory.getCipher(cipher.getProvider());
        CipherFactory.initCipher(newCipher, keyResolver.getDataKey(), keyResolver.getIvBytes(), Cipher.DECRYPT_MODE, getFilePointer());
        return new MMapCryptoIndexInput("clone", in.clone(), newCipher, keyResolver);
    }

    @Override
    public byte readByte() throws IOException {
        return cipher.update(new byte[] { in.readByte() })[0];
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        byte[] tmp = new byte[len];
        in.readBytes(tmp, 0, len);
        try {
            cipher.update(tmp, 0, len, b, offset);
        } catch (ShortBufferException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void skipBytes(long numBytes) throws IOException {
        // System.out.println("InskipBytes. numBytes: "+numBytes+" filePointer: "+getFilePointer());
        seek(getFilePointer() + numBytes);
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos < 0 || pos > length()) {
            throw new EOFException(
                Thread.currentThread().getName() + " read past EOF: pos=" + pos + " vs length=" + length() + ": " + this
            );
        }
        in.seek(pos);
        // System.out.println("InSeek. pos: "+pos+" filePointer: "+getFilePointer());
        CipherFactory.initCipher(cipher, keyResolver.getDataKey(), keyResolver.getIvBytes(), Cipher.DECRYPT_MODE, pos);
    }

    // Many of the "big" files create slices so we decrypt them in bulk
    // TODO use weak reference here to improve memory usage/management
    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        IndexInput delegate = in.slice(sliceDescription, offset, length);
        if (length > Integer.MAX_VALUE)
            throw new RuntimeException("Big slice!!: " + length);
        final int CHUNK_SIZE = 16 * 1024;
        ByteBuffer temp = recycler.allocate(CHUNK_SIZE);// ByteBuffer.allocate(Math.min(Integer.MAX_VALUE, (int)length));
        ByteBuffer tempOut = recycler.allocate(CHUNK_SIZE);// ByteBuffer.allocate(Math.min(Integer.MAX_VALUE, (int)length));
        ByteBuffersDataOutput output = new ByteBuffersDataOutput(length);
        int res = 0;
        Cipher newCipher = CipherFactory.getCipher(cipher.getProvider());
        // CipherFactory.initCipher(newCipher, directory, Optional.of(cipher.getIV()), Cipher.DECRYPT_MODE, offset);
        CipherFactory.initCipher(newCipher, keyResolver.getDataKey(), keyResolver.getIvBytes(), Cipher.DECRYPT_MODE, offset);
        while (length > 0) {
            final int chunk = Math.min((int) length, CHUNK_SIZE);
            delegate.readBytes(temp.array(), 0, chunk, true);

            try {
                res = newCipher.update(temp.rewind().limit(chunk), tempOut.rewind());
            } catch (ShortBufferException /*| IllegalBlockSizeException | BadPaddingException */ ex) {
                throw new IOException("failed to decrypt blck.", ex);
            }
            output.writeBytes(tempOut.rewind().limit(res));
            length -= chunk;
        }
        try {
            res = newCipher.doFinal(temp, tempOut.rewind());
            output.writeBytes(tempOut.rewind().limit(res));
        } catch (ShortBufferException | IllegalBlockSizeException | BadPaddingException ex) {
            throw new IOException("failed to decrypt blck.", ex);
        }
        delegate.close();
        recycler.reuse(temp);
        recycler.reuse(tempOut);
        return new ByteBuffersIndexInput(output.toDataInput(), "slice");
    }
}

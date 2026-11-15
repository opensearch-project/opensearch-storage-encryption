/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.niofs;

import static org.opensearch.index.store.cipher.AesCipherFactory.ALGORITHM;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Provider;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.cipher.AesCipherFactory;
import org.opensearch.index.store.cipher.EncryptionAlgorithm;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.footer.EncryptionFooter;
import org.opensearch.index.store.key.KeyResolver;

/**
 * An IndexInput implementation that decrypts data for reading
 *
 * @opensearch.internal
 */
final class CryptoBufferedIndexInput extends BufferedIndexInput {
    private static final ByteBuffer EMPTY_BYTEBUFFER = ByteBuffer.allocate(0);
    private static final int CHUNK_SIZE = 16_384;

    private final FileChannel channel;
    private final boolean isClone;
    private final long off;
    private final long end;
    private final KeyResolver keyResolver;
    private final SecretKeySpec keySpec;
    private final byte[] masterKey;
    private final byte[] messageId;
    private final int footerLength;
    private final long frameSize;
    private final int frameSizePower;
    private final EncryptionAlgorithm algorithm;
    private final EncryptionMetadataCache encryptionMetadataCache;
    private final Provider provider;

    private ByteBuffer tmpBuffer = EMPTY_BYTEBUFFER;

    private final String normalizedFilePath;

    public CryptoBufferedIndexInput(
        String resourceDesc,
        FileChannel fc,
        IOContext context,
        KeyResolver keyResolver,
        Path filePath,
        EncryptionMetadataCache encryptionMetadataCache,
        Provider provider
    )
        throws IOException {
        super(resourceDesc, context);
        this.channel = fc;
        this.off = 0L;
        this.end = fc.size();
        this.keyResolver = keyResolver;
        this.isClone = false;
        this.normalizedFilePath = EncryptionMetadataCache.normalizePath(filePath);
        this.encryptionMetadataCache = encryptionMetadataCache;
        this.provider = provider;

        // Get master key first
        this.masterKey = keyResolver.getDataKey().getEncoded();

        // Read footer and cache metadata atomically
        EncryptionFooter footer = EncryptionFooter
            .readViaFileChannel(normalizedFilePath, channel, masterKey, encryptionMetadataCache, provider);

        // Get metadata (already cached by readViaFileChannel)
        var metadata = encryptionMetadataCache.getOrLoadMetadata(normalizedFilePath, footer, masterKey, provider);
        this.messageId = metadata.getFooter().getMessageId();
        this.frameSize = metadata.getFooter().getFrameSize();
        this.frameSizePower = metadata.getFooter().getFrameSizePower();
        this.algorithm = EncryptionAlgorithm.fromId(metadata.getFooter().getAlgorithmId());
        this.keySpec = new SecretKeySpec(metadata.getFileKey(), ALGORITHM);

        // Calculate footer length
        this.footerLength = metadata.getFooter().getFooterLength();
    }

    public CryptoBufferedIndexInput(
        String resourceDesc,
        FileChannel fc,
        long off,
        long length,
        int bufferSize,
        KeyResolver keyResolver,
        SecretKeySpec keySpec,
        int footerLength,
        long frameSize,
        int frameSizePower,
        short algorithmId,
        byte[] masterKey,
        byte[] messageId,
        String normalizedFilePath,
        EncryptionMetadataCache encryptionMetadataCache,
        Provider provider
    )
        throws IOException {
        super(resourceDesc, bufferSize);
        this.channel = fc;
        this.off = off;
        this.end = off + length;
        this.isClone = true;
        this.keyResolver = keyResolver;
        this.keySpec = keySpec;  // Reuse keySpec from main file
        this.footerLength = footerLength;
        this.frameSize = frameSize;
        this.frameSizePower = frameSizePower;
        this.algorithm = EncryptionAlgorithm.fromId(algorithmId);
        this.masterKey = masterKey;  // Passed from parent
        this.messageId = messageId;  // Passed from parent
        this.normalizedFilePath = normalizedFilePath;
        this.encryptionMetadataCache = encryptionMetadataCache;
        this.provider = provider;
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
        clone.tmpBuffer = EMPTY_BYTEBUFFER;
        return clone;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.length()) {
            throw new IllegalArgumentException(
                "slice() " + sliceDescription + " out of bounds: offset=" + offset + ", length=" + length + ", fileLength=" + this.length()
            );
        }
        return new CryptoBufferedIndexInput(
            getFullSliceDescription(sliceDescription),
            channel,
            off + offset,
            length,
            getBufferSize(),
            keyResolver,
            keySpec,  // Pass the already-derived keySpec
            footerLength,
            frameSize,
            frameSizePower,
            algorithm.getAlgorithmId(),
            masterKey,  // Pass directory key
            messageId,      // Pass message ID
            normalizedFilePath,
            encryptionMetadataCache,
            provider
        );
    }

    @Override
    public long length() {
        // Exclude footer from logical file length (only for main file, not slices)
        if (isClone) {
            return end - off;  // Slices use exact length passed in
        } else {
            return end - off - footerLength;  // Main file excludes variable footer
        }
    }

    @SuppressForbidden(reason = "FileChannel#read is efficient and used intentionally")
    private int read(ByteBuffer dst, long position) throws IOException {
        if (tmpBuffer == EMPTY_BYTEBUFFER) {
            tmpBuffer = ByteBuffer.allocate(CHUNK_SIZE);
        }

        long frameNumber = position >>> frameSizePower;
        long offsetWithinFrame = position & ((1L << frameSizePower) - 1);
        long frameEnd = (frameNumber + 1) << frameSizePower;
        int maxReadInFrame = (int) Math.min(dst.remaining(), frameEnd - position);

        tmpBuffer.clear().limit(maxReadInFrame);
        int bytesRead = channel.read(tmpBuffer, position);
        if (bytesRead == -1) {
            return -1;
        }
        tmpBuffer.flip();

        try {
            Cipher cipher = algorithm.getDecryptionCipher(provider);
            byte[] frameIV = AesCipherFactory
                .computeFrameIV(
                    masterKey,
                    messageId,
                    frameNumber,
                    offsetWithinFrame,
                    this.normalizedFilePath,
                    encryptionMetadataCache,
                    provider
                );
            cipher.init(Cipher.DECRYPT_MODE, keySpec, new IvParameterSpec(frameIV));

            int pendingSkip = AesCipherFactory.advanceCtrSkip(cipher, AesCipherFactory.skipBytesForOffset(offsetWithinFrame));
            // Each read() re-inits the cipher, so always doFinal. Buffering providers
            // (Bouncy Castle) keep a partial AES block across update() even when skip is 0;
            // returning update() alone can emit 0 on the next independently inited read
            // and spin forever in readInternal().
            if (pendingSkip == 0) {
                return cipher.doFinal(tmpBuffer, dst);
            }

            // Buffering providers (Bouncy Castle) emit the skip prefix with later
            // ciphertext. Decrypt into a buffer large enough for skip + ciphertext,
            // then copy only the requested plaintext into dst.
            ByteBuffer out = ByteBuffer.allocate(pendingSkip + bytesRead + AesCipherFactory.AES_BLOCK_SIZE_BYTES);
            cipher.update(tmpBuffer, out);
            cipher.doFinal(ByteBuffer.allocate(0), out);
            out.flip();
            if (out.remaining() < pendingSkip) {
                throw new ShortBufferException("CTR skip prefix missing from decrypt output");
            }
            out.position(out.position() + pendingSkip);
            int toCopy = Math.min(out.remaining(), dst.remaining());
            ByteBuffer plaintext = out.slice();
            plaintext.limit(toCopy);
            dst.put(plaintext);
            return toCopy;
        } catch (ShortBufferException | IllegalBlockSizeException | BadPaddingException | InvalidAlgorithmParameterException
            | InvalidKeyException ex) {
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

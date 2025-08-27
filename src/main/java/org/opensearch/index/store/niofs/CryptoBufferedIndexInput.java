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
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;

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
import org.opensearch.index.store.footer.EncryptionFooter;
import org.opensearch.index.store.footer.EncryptionMetadataTrailer;
import org.opensearch.index.store.key.HkdfKeyDerivation;
import org.opensearch.index.store.key.KeyResolver;

/**
 * An IndexInput implementation that decrypts data for reading
 *
 * @opensearch.internal
 */
final class CryptoBufferedIndexInput extends BufferedIndexInput {
    private static final byte[] ZERO_SKIP = new byte[AesCipherFactory.AES_BLOCK_SIZE_BYTES];
    private static final ByteBuffer EMPTY_BYTEBUFFER = ByteBuffer.allocate(0);
    private static final int CHUNK_SIZE = 16_384;

    private final FileChannel channel;
    private final boolean isClone;
    private final long off;
    private final long end;
    private final KeyResolver keyResolver;
    private final SecretKeySpec keySpec;
    private final byte[] directoryKey;
    private final byte[] messageId;
    private final int footerLength;
    private final long frameSize;
    private final EncryptionAlgorithm algorithm;

    private ByteBuffer tmpBuffer = EMPTY_BYTEBUFFER;

    public CryptoBufferedIndexInput(String resourceDesc, FileChannel fc, IOContext context, KeyResolver keyResolver) throws IOException {
        super(resourceDesc, context);
        this.channel = fc;
        this.off = 0L;
        this.end = fc.size();
        this.keyResolver = keyResolver;
        this.isClone = false;
        
        // Get directory key first
        this.directoryKey = keyResolver.getDataKey().getEncoded();
        
        // Read footer with temporary key for authentication
        EncryptionFooter footer = readFooterFromFile();
        this.messageId = footer.getMessageId();
        this.frameSize = footer.getFrameSize();
        this.algorithm = EncryptionAlgorithm.fromId(footer.getAlgorithmId());
        
        // Derive file-specific key using messageId from footer
        byte[] derivedKey = HkdfKeyDerivation.deriveAesKey(directoryKey, messageId, "file-encryption");
        this.keySpec = new SecretKeySpec(derivedKey, ALGORITHM);
        
        // Calculate footer length
        long fileSize = channel.size();
        ByteBuffer buffer = ByteBuffer.allocate(EncryptionMetadataTrailer.MIN_FOOTER_SIZE);
        channel.read(buffer, fileSize - EncryptionMetadataTrailer.MIN_FOOTER_SIZE);
        this.footerLength = EncryptionFooter.calculateFooterLength(buffer.array());
    }

    public CryptoBufferedIndexInput(String resourceDesc, FileChannel fc, long off, long length, int bufferSize, KeyResolver keyResolver, SecretKeySpec keySpec, int footerLength, long frameSize, short algorithmId, byte[] directoryKey, byte[] messageId)
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
        this.algorithm = EncryptionAlgorithm.fromId(algorithmId);
        this.directoryKey = directoryKey;  // Passed from parent
        this.messageId = messageId;  // Passed from parent
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
            algorithm.getAlgorithmId(),
            directoryKey,  // Pass directory key
            messageId      // Pass message ID
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

        // initialize buffer lazy because Lucene may open input slices and clones ahead but never use them
        // see org.apache.lucene.store.BufferedIndexInput
        if (tmpBuffer == EMPTY_BYTEBUFFER) {
            tmpBuffer = ByteBuffer.allocate(CHUNK_SIZE);
        }

        // Calculate frame boundaries to avoid reading across frames
        int frameNumber = (int)(position / frameSize);
        long offsetWithinFrame = position % frameSize;
        long frameEnd = (frameNumber + 1) * frameSize;
        
        // Limit read to not cross frame boundary
        int maxReadInFrame = (int) Math.min(dst.remaining(), frameEnd - position);

        tmpBuffer.clear().limit(maxReadInFrame);
        int bytesRead = channel.read(tmpBuffer, position);
        if (bytesRead == -1) {
            return -1;
        }

        tmpBuffer.flip();

        try {
            // Use frame-based decryption with algorithm-based cipher
            Cipher cipher = algorithm.getDecryptionCipher();
            
            // Derive frame-specific IV
            byte[] frameIV = AesCipherFactory.computeFrameIV(directoryKey, messageId, frameNumber, offsetWithinFrame);
            
            cipher.init(Cipher.DECRYPT_MODE, keySpec, new IvParameterSpec(frameIV));
            
            if (offsetWithinFrame % AesCipherFactory.AES_BLOCK_SIZE_BYTES > 0) {
                cipher.update(ZERO_SKIP, 0, (int) (offsetWithinFrame % AesCipherFactory.AES_BLOCK_SIZE_BYTES));
            }

            return (end - position > bytesRead) ? cipher.update(tmpBuffer, dst) : cipher.doFinal(tmpBuffer, dst);
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
    
    /**
     * Read encryption footer from end of file
     */
    private EncryptionFooter readFooterFromFile() throws IOException {
        long fileSize = channel.size();
        if (fileSize < EncryptionMetadataTrailer.MIN_FOOTER_SIZE) {
            throw new IOException("File too small to contain encryption footer");
        }
        
        // First read minimum footer to get actual length
        ByteBuffer minBuffer = ByteBuffer.allocate(EncryptionMetadataTrailer.MIN_FOOTER_SIZE);
        channel.read(minBuffer, fileSize - EncryptionMetadataTrailer.MIN_FOOTER_SIZE);
        
        int footerLength = EncryptionFooter.calculateFooterLength(minBuffer.array());
        
        // Read complete footer
        ByteBuffer footerBuffer = ByteBuffer.allocate(footerLength);
        int bytesRead = channel.read(footerBuffer, fileSize - footerLength);
        
        if (bytesRead != footerLength) {
            throw new IOException("Failed to read complete footer");
        }
        
        // Use directory key for footer authentication (before file key derivation)
        return EncryptionFooter.deserialize(footerBuffer.array(), directoryKey);
    }
    

}

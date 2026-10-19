/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.translog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;

import org.apache.lucene.codecs.CodecUtil;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.key.HkdfKeyDerivation;
import org.opensearch.index.store.key.KeyResolver;

/**
 * Encrypts translog ({@code .tlog}) files with AES-256-CTR keyed by file offset.
 *
 * <p>The {@link TranslogHeader} stays plaintext. Every byte after it, at file offset {@code p}, is
 * XORed with the keystream for counter block {@code (p - headerSize) / 16} under a 64-bit nonce that
 * is unique to this file (derived from the shard key, the translog UUID and the file name, so every
 * translog generation gets its own nonce). Ciphertext is the same length as plaintext, so the logical
 * offsets OpenSearch uses are the physical offsets, and any byte range decrypts independently: bytes
 * appended a moment ago (realtime get during live indexing) and the tail of a file whose writer died
 * without closing it (translog replay, peer recovery) are always readable.
 *
 * <p>This replaces an AES-GCM 8KB-chunk format that wrote each chunk's authentication tag only when
 * the chunk filled or the channel closed, so reading the still-open chunk always failed
 * authentication, and that used one GCM nonce for every chunk of a translog.
 *
 * <p>Invariant: an offset of a given file is only ever written with the same plaintext (translog
 * files are append-only). Rewriting an offset with different bytes would reuse keystream.
 * Integrity is not cryptographically enforced; corruption is detected by the translog's per-operation
 * checksums.
 *
 * @opensearch.internal
 */
@SuppressForbidden(reason = "Channel operations required for translog encryption")
public class TranslogChunkManager {

    private static final String TRANSFORMATION = "AES/CTR/NoPadding";
    private static final int AES_BLOCK_SIZE = 16;
    private static final int NONCE_SIZE = 8;
    private static final String NONCE_CONTEXT = "translog-ctr-nonce-v1";
    private static final int TRANSFER_BUFFER_SIZE = 8192;

    private static final ThreadLocal<Cipher> CIPHER = ThreadLocal.withInitial(() -> {
        try {
            return Cipher.getInstance(TRANSFORMATION);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AES/CTR is unavailable", e);
        }
    });
    private static final ThreadLocal<ByteBuffer> TRANSFER_BUFFER_POOL = ThreadLocal
        .withInitial(() -> ByteBuffer.allocate(TRANSFER_BUFFER_SIZE));

    private final FileChannel delegate;
    private final KeyResolver keyResolver;
    private final Path filePath;
    private final int actualHeaderSize;
    private final byte[] nonce;

    /**
     * Creates a new TranslogChunkManager.
     *
     * @param delegate the underlying FileChannel for actual I/O operations
     * @param keyResolver the key resolver for encryption operations
     * @param filePath the translog file path; its name scopes the per-file nonce
     * @param translogUUID the translog UUID, for the header size and the per-file nonce
     */
    public TranslogChunkManager(FileChannel delegate, KeyResolver keyResolver, Path filePath, String translogUUID) {
        if (translogUUID == null) {
            throw new IllegalArgumentException("translogUUID is required for exact header size calculation");
        }
        this.delegate = delegate;
        this.keyResolver = keyResolver;
        this.filePath = filePath;
        String fileName = filePath.getFileName().toString();
        this.actualHeaderSize = fileName.endsWith(".tlog") ? calculateTranslogHeaderSize(translogUUID) : 0;
        this.nonce = deriveFileNonce(keyResolver.getDataKey().getEncoded(), translogUUID, fileName);
    }

    /**
     * Derives this file's 64-bit CTR nonce from the shard key, the translog UUID and the file name.
     *
     * @param key the shard data key
     * @param translogUUID the translog UUID
     * @param fileName the translog file name (encodes the generation)
     * @return the 8-byte nonce
     */
    static byte[] deriveFileNonce(byte[] key, String translogUUID, String fileName) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            sha256.update(translogUUID.getBytes(StandardCharsets.UTF_8));
            sha256.update((byte) 0);
            sha256.update(fileName.getBytes(StandardCharsets.UTF_8));
            byte[] fileId = Arrays.copyOf(sha256.digest(), 16);
            return HkdfKeyDerivation.deriveKey(key, fileId, NONCE_CONTEXT, NONCE_SIZE);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Failed to derive translog nonce", e);
        }
    }

    /**
     * Returns the size of the plaintext translog header.
     *
     * @return the header size in bytes
     */
    public int determineHeaderSize() {
        return actualHeaderSize;
    }

    /**
     * Local implementation of TranslogHeader.headerSizeInBytes() to avoid cross-classloader access issues.
     *
     * @param translogUUID the translog UUID used for calculating the UUID field size in the header
     * @return the header size in bytes including codec header, UUID field, and version-specific fields
     */
    private static int calculateTranslogHeaderSize(String translogUUID) {
        int uuidLength = translogUUID.getBytes(StandardCharsets.UTF_8).length;
        int size = CodecUtil.headerLength(TranslogHeader.TRANSLOG_CODEC); // Lucene codec header
        size += Integer.BYTES + uuidLength; // uuid length field + uuid bytes
        if (TranslogHeader.CURRENT_VERSION >= TranslogHeader.VERSION_PRIMARY_TERM) {
            size += Long.BYTES;    // primary term
            size += Integer.BYTES; // checksum
        }
        return size;
    }

    /** XORs {@code len} bytes of {@code data} in place with the keystream for file offset {@code filePosition}. */
    private void applyKeystream(byte[] data, int off, int len, long filePosition) throws IOException {
        long dataOffset = filePosition - actualHeaderSize;
        byte[] iv = new byte[AES_BLOCK_SIZE];
        System.arraycopy(nonce, 0, iv, 0, NONCE_SIZE);
        long counter = dataOffset / AES_BLOCK_SIZE;
        for (int i = 0; i < Long.BYTES; i++) {
            iv[AES_BLOCK_SIZE - 1 - i] = (byte) (counter >>> (8 * i));
        }
        int skip = (int) (dataOffset % AES_BLOCK_SIZE);
        try {
            Cipher cipher = CIPHER.get();
            cipher.init(Cipher.ENCRYPT_MODE, keyResolver.getDataKey(), new IvParameterSpec(iv));
            if (skip > 0) {
                cipher.update(new byte[skip]); // advance into the first counter block
            }
            int processed = cipher.update(data, off, len, data, off);
            if (processed != len) {
                throw new IOException("AES/CTR processed " + processed + " of " + len + " bytes for file:" + filePath);
            }
        } catch (GeneralSecurityException e) {
            throw new IOException("Failed to apply translog keystream at offset:" + filePosition + " file:" + filePath, e);
        }
    }

    /**
     * Reads and decrypts bytes at {@code position}. Header bytes are returned as stored.
     *
     * @param dst the buffer to read data into
     * @param position the file position to read from
     * @return the number of bytes read, or -1 at end of file
     * @throws IOException if reading or decryption fails
     */
    public int readFromChunks(ByteBuffer dst, long position) throws IOException {
        if (dst.remaining() == 0) {
            return 0;
        }
        if (position < actualHeaderSize) {
            ByteBuffer headerPart = dst.duplicate();
            headerPart.limit(headerPart.position() + (int) Math.min(dst.remaining(), actualHeaderSize - position));
            int read = delegate.read(headerPart, position);
            if (read > 0) {
                dst.position(dst.position() + read);
            }
            return read;
        }
        byte[] buf = new byte[dst.remaining()];
        ByteBuffer in = ByteBuffer.wrap(buf);
        int total = 0;
        while (in.hasRemaining()) {
            int read = delegate.read(in, position + total);
            if (read <= 0) {
                break;
            }
            total += read;
        }
        if (total == 0) {
            return position >= delegate.size() ? -1 : 0;
        }
        applyKeystream(buf, 0, total, position);
        dst.put(buf, 0, total);
        return total;
    }

    /**
     * Encrypts and writes bytes at {@code position}. Header bytes are written as given.
     *
     * @param src the buffer containing data to write
     * @param position the file position to write to
     * @return the number of bytes written
     * @throws IOException if encryption or writing fails
     */
    public int writeToChunks(ByteBuffer src, long position) throws IOException {
        if (src.remaining() == 0) {
            return 0;
        }
        if (position < actualHeaderSize) {
            ByteBuffer headerPart = src.duplicate();
            headerPart.limit(headerPart.position() + (int) Math.min(src.remaining(), actualHeaderSize - position));
            int written = delegate.write(headerPart, position);
            src.position(src.position() + written);
            return written;
        }
        byte[] buf = new byte[src.remaining()];
        src.get(buf);
        applyKeystream(buf, 0, buf.length, position);
        ByteBuffer out = ByteBuffer.wrap(buf);
        int written = 0;
        while (out.hasRemaining()) {
            written += delegate.write(out, position + written);
        }
        return written;
    }

    /**
     * Transfers decrypted bytes from this file to a target channel.
     *
     * @param position the starting position in the source
     * @param count the number of bytes to transfer
     * @param target the target channel to write to
     * @return the number of bytes transferred
     * @throws IOException if transfer fails
     */
    public long transferFromChunks(long position, long count, WritableByteChannel target) throws IOException {
        long transferred = 0;
        ByteBuffer buffer = TRANSFER_BUFFER_POOL.get();
        while (transferred < count) {
            buffer.clear();
            buffer.limit((int) Math.min(buffer.capacity(), count - transferred));
            int bytesRead = readFromChunks(buffer, position + transferred);
            if (bytesRead <= 0) {
                break;
            }
            buffer.flip();
            int bytesWritten = target.write(buffer);
            transferred += bytesWritten;
            if (bytesWritten < bytesRead) {
                break;
            }
        }
        return transferred;
    }

    /**
     * Transfers bytes from a source channel into this file, encrypting them.
     *
     * @param src the source channel to read from
     * @param position the starting position in the target
     * @param count the number of bytes to transfer
     * @return the number of bytes transferred
     * @throws IOException if transfer fails
     */
    public long transferToChunks(ReadableByteChannel src, long position, long count) throws IOException {
        long transferred = 0;
        ByteBuffer buffer = TRANSFER_BUFFER_POOL.get();
        while (transferred < count) {
            buffer.clear();
            buffer.limit((int) Math.min(buffer.capacity(), count - transferred));
            int bytesRead = src.read(buffer);
            if (bytesRead <= 0) {
                break;
            }
            buffer.flip();
            int bytesWritten = writeToChunks(buffer, position + transferred);
            transferred += bytesWritten;
            if (bytesWritten < bytesRead) {
                break;
            }
        }
        return transferred;
    }

    /**
     * No-op: CTR has no per-chunk state to finalize, so every written byte is already durable and
     * readable. Kept for callers that finalize the channel before upload.
     */
    public void close() {}
}

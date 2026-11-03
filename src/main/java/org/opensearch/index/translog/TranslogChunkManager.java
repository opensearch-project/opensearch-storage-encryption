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
import javax.crypto.spec.GCMParameterSpec;

import org.apache.lucene.codecs.CodecUtil;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.key.HkdfKeyDerivation;
import org.opensearch.index.store.key.KeyResolver;

/**
 * Encrypts translog ({@code .tlog}) files as a sequence of sealed AES-256-GCM frames.
 *
 * <p>On-disk layout:
 * <pre>
 * [TranslogHeader - plaintext]
 * [format header: magic (4) | version (4) | max frame size (4)]
 * [frame 0][frame 1]...   frame = [plaintext length (4) | ciphertext | tag (16)]
 * </pre>
 *
 * <p>Every write is sealed immediately as one or more frames of at most the maximum frame size, so
 * every byte OpenSearch has written can be read and authenticated right away (realtime gets during
 * live indexing, translog replay after a crash). A frame holds whatever one write contained, so
 * frames vary in size: a 6KB flush is one 6KB frame.
 *
 * <p>Each frame's 96-bit nonce is the file's 64-bit nonce (derived from the shard key, the translog
 * UUID and the file name) followed by the 32-bit frame index, so no nonce repeats under a key. The
 * additional authenticated data is the frame index, the plaintext length and the frame's logical
 * offset, so a frame cannot be resized, reordered or moved without failing authentication.
 *
 * <p>OpenSearch addresses the file by logical (plaintext) offsets. Frame start offsets are kept in
 * memory, and rebuilt on open by scanning the frame length headers. The scan stops at the first
 * incomplete or invalid frame. A frame torn by a crash lies beyond the last checkpoint, because
 * TranslogWriter forces the channel before it writes a checkpoint, so it is never read. A channel
 * that finds such a frame, or whose own frame write fails part way, rejects every later write:
 * the next frame would reuse the partial frame's index and nonce. OpenSearch never appends to such
 * a file; recovery reads it and writes new operations to a new generation.
 *
 * <p>The maximum frame size is stored in the format header, so a file is always read with the size
 * it was written with.
 *
 * @opensearch.internal
 */
@SuppressForbidden(reason = "Channel operations required for translog encryption")
public class TranslogChunkManager {

    /** Default maximum plaintext bytes per frame. */
    public static final int DEFAULT_MAX_FRAME_SIZE = 8192;

    static final int FORMAT_MAGIC = 0x544C4746; // "TLGF"
    static final int FORMAT_VERSION = 1;
    static final int FORMAT_HEADER_SIZE = 3 * Integer.BYTES;
    static final int FRAME_HEADER_SIZE = Integer.BYTES;
    static final int GCM_TAG_SIZE = 16;
    static final int FRAME_OVERHEAD = FRAME_HEADER_SIZE + GCM_TAG_SIZE;

    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int NONCE_SIZE = 8;
    private static final int GCM_IV_SIZE = 12;
    private static final int AAD_SIZE = Integer.BYTES + Integer.BYTES + Long.BYTES;
    private static final String NONCE_CONTEXT = "translog-gcm-nonce-v1";
    private static final int TRANSFER_BUFFER_SIZE = 8192;

    private static final ThreadLocal<Cipher> CIPHER = ThreadLocal.withInitial(() -> {
        try {
            return Cipher.getInstance(TRANSFORMATION);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AES/GCM is unavailable", e);
        }
    });
    private static final ThreadLocal<ByteBuffer> TRANSFER_BUFFER_POOL = ThreadLocal
        .withInitial(() -> ByteBuffer.allocate(TRANSFER_BUFFER_SIZE));

    /** A frame: its index, logical (plaintext) and physical (on-disk) start offsets, and plaintext length. */
    private record Frame(int index, long logicalStart, long physicalStart, int length) {
    }

    /** The last decrypted frame. Frames never change once written, so it can be shared by readers. */
    private record DecryptedFrame(int index, byte[] plaintext) {
    }

    private final FileChannel delegate;
    private final KeyResolver keyResolver;
    private final Path filePath;
    private final int actualHeaderSize;
    private final byte[] nonce;
    private final int newFileMaxFrameSize;

    // Frame index, guarded by this: frame i starts at logicalStarts[i] / physicalStarts[i] and holds lengths[i] bytes.
    private long[] logicalStarts = new long[64];
    private long[] physicalStarts = new long[64];
    private int[] lengths = new int[64];
    private int frameCount;
    private boolean formatHeaderPresent;
    private int maxFrameSize;
    private long logicalEnd;
    private long physicalEnd;
    // Set when the file has bytes after its last complete frame (a write torn by a crash) or a frame
    // write failed part way. Appending would then seal a new frame under the index, and so the nonce,
    // of the partial one, so every later write is rejected. Reads of complete frames are unaffected.
    private boolean appendsBlocked;

    private volatile DecryptedFrame lastDecrypted;

    /**
     * Creates a new TranslogChunkManager with the default maximum frame size.
     *
     * @param delegate the underlying FileChannel for actual I/O operations
     * @param keyResolver the key resolver for encryption operations
     * @param filePath the translog file path; its name scopes the per-file nonce
     * @param translogUUID the translog UUID, for the header size and the per-file nonce
     * @throws IOException if the existing file content cannot be indexed
     */
    public TranslogChunkManager(FileChannel delegate, KeyResolver keyResolver, Path filePath, String translogUUID) throws IOException {
        this(delegate, keyResolver, filePath, translogUUID, DEFAULT_MAX_FRAME_SIZE);
    }

    /**
     * Creates a new TranslogChunkManager.
     *
     * @param delegate the underlying FileChannel for actual I/O operations
     * @param keyResolver the key resolver for encryption operations
     * @param filePath the translog file path; its name scopes the per-file nonce
     * @param translogUUID the translog UUID, for the header size and the per-file nonce
     * @param newFileMaxFrameSize the maximum frame size if this is a new file; an existing file keeps the size it was written with
     * @throws IOException if the existing file content cannot be indexed
     */
    TranslogChunkManager(FileChannel delegate, KeyResolver keyResolver, Path filePath, String translogUUID, int newFileMaxFrameSize)
        throws IOException {
        if (translogUUID == null) {
            throw new IllegalArgumentException("translogUUID is required for exact header size calculation");
        }
        if (newFileMaxFrameSize <= 0) {
            throw new IllegalArgumentException("maximum frame size must be positive: " + newFileMaxFrameSize);
        }
        this.delegate = delegate;
        this.keyResolver = keyResolver;
        this.filePath = filePath;
        String fileName = filePath.getFileName().toString();
        this.actualHeaderSize = fileName.endsWith(".tlog") ? calculateTranslogHeaderSize(translogUUID) : 0;
        this.nonce = deriveFileNonce(keyResolver.getDataKey().getEncoded(), translogUUID, fileName);
        this.newFileMaxFrameSize = newFileMaxFrameSize;
        synchronized (this) {
            indexNewFrames();
        }
    }

    /**
     * Derives this file's 64-bit nonce prefix from the shard key, the translog UUID and the file name.
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

    /**
     * Returns the logical (plaintext) size of the file: the header plus every complete frame,
     * including frames appended by another channel since this one last looked.
     *
     * @return the logical size in bytes
     * @throws IOException if the file size cannot be read or new frames cannot be indexed
     */
    public synchronized long logicalSize() throws IOException {
        if (delegate.size() != physicalEnd) {
            indexNewFrames();
        }
        return logicalEnd;
    }

    /**
     * Returns the maximum frame size this file is read and written with.
     *
     * @return the maximum plaintext bytes per frame
     */
    synchronized int maxFrameSize() {
        return formatHeaderPresent ? maxFrameSize : newFileMaxFrameSize;
    }

    /**
     * Indexes frames appended since the last scan (by this or another channel). Reads only the frame
     * length headers; nothing is decrypted. Bytes left after the last complete frame block appends.
     */
    private void indexNewFrames() throws IOException {
        long fileSize = delegate.size();
        scanFrames(fileSize);
        if (physicalEnd < fileSize) {
            appendsBlocked = true;
        }
    }

    private void scanFrames(long fileSize) throws IOException {
        if (!formatHeaderPresent) {
            if (fileSize < actualHeaderSize + FORMAT_HEADER_SIZE) {
                // Header only (or a header or format header torn by a crash): no frames yet.
                logicalEnd = physicalEnd = Math.min(fileSize, actualHeaderSize);
                return;
            }
            ByteBuffer format = readPhysical(actualHeaderSize, FORMAT_HEADER_SIZE);
            int magic = format.getInt();
            int version = format.getInt();
            int frameSize = format.getInt();
            if (magic != FORMAT_MAGIC || version != FORMAT_VERSION || frameSize <= 0) {
                throw new IOException(
                    "Unsupported translog encryption format (magic="
                        + Integer.toHexString(magic)
                        + ", version="
                        + version
                        + ", maxFrameSize="
                        + frameSize
                        + ") in file:"
                        + filePath
                );
            }
            maxFrameSize = frameSize;
            formatHeaderPresent = true;
            logicalEnd = actualHeaderSize;
            physicalEnd = actualHeaderSize + FORMAT_HEADER_SIZE;
        }
        ByteBuffer lengthBuf = ByteBuffer.allocate(FRAME_HEADER_SIZE);
        while (physicalEnd + FRAME_OVERHEAD < fileSize) {
            lengthBuf.clear();
            if (readPhysicalInto(lengthBuf, physicalEnd) < FRAME_HEADER_SIZE) {
                return;
            }
            int length = lengthBuf.getInt(0);
            if (length <= 0 || length > maxFrameSize || physicalEnd + FRAME_OVERHEAD + length > fileSize) {
                // Incomplete or invalid trailing frame, beyond the last checkpoint: ignore it.
                return;
            }
            addFrame(length);
        }
    }

    private void addFrame(int length) throws IOException {
        if (frameCount == Integer.MAX_VALUE) {
            throw new IOException("Too many frames in translog file:" + filePath);
        }
        if (frameCount == lengths.length) {
            int newCapacity = (int) Math.min(Integer.MAX_VALUE, frameCount * 2L);
            logicalStarts = Arrays.copyOf(logicalStarts, newCapacity);
            physicalStarts = Arrays.copyOf(physicalStarts, newCapacity);
            lengths = Arrays.copyOf(lengths, newCapacity);
        }
        logicalStarts[frameCount] = logicalEnd;
        physicalStarts[frameCount] = physicalEnd;
        lengths[frameCount] = length;
        frameCount++;
        logicalEnd += length;
        physicalEnd += FRAME_OVERHEAD + length;
    }

    /** Returns the frame holding logical offset {@code position}, or null if it is past the last frame. */
    private synchronized Frame findFrame(long position) throws IOException {
        if (position >= logicalEnd) {
            indexNewFrames();
            if (position >= logicalEnd) {
                return null;
            }
        }
        int lo = 0;
        int hi = frameCount - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (logicalStarts[mid] <= position) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return new Frame(lo, logicalStarts[lo], physicalStarts[lo], lengths[lo]);
    }

    private void initCipher(Cipher cipher, int mode, int frameIndex, long logicalStart, int length) throws GeneralSecurityException {
        byte[] iv = new byte[GCM_IV_SIZE];
        System.arraycopy(nonce, 0, iv, 0, NONCE_SIZE);
        ByteBuffer.wrap(iv, NONCE_SIZE, Integer.BYTES).putInt(frameIndex);
        cipher.init(mode, keyResolver.getDataKey(), new GCMParameterSpec(GCM_TAG_SIZE * Byte.SIZE, iv));
        cipher.updateAAD(ByteBuffer.allocate(AAD_SIZE).putInt(frameIndex).putInt(length).putLong(logicalStart).array());
    }

    /** Reads, authenticates and decrypts a frame. */
    private byte[] decryptFrame(Frame frame) throws IOException {
        DecryptedFrame cached = lastDecrypted;
        if (cached != null && cached.index() == frame.index()) {
            return cached.plaintext();
        }
        ByteBuffer sealed = readPhysical(frame.physicalStart() + FRAME_HEADER_SIZE, frame.length() + GCM_TAG_SIZE);
        try {
            Cipher cipher = CIPHER.get();
            initCipher(cipher, Cipher.DECRYPT_MODE, frame.index(), frame.logicalStart(), frame.length());
            byte[] plaintext = cipher.doFinal(sealed.array(), 0, sealed.limit());
            lastDecrypted = new DecryptedFrame(frame.index(), plaintext);
            return plaintext;
        } catch (GeneralSecurityException e) {
            throw new IOException("Failed to authenticate translog frame " + frame.index() + " in file:" + filePath, e);
        }
    }

    private ByteBuffer readPhysical(long position, int length) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(length);
        if (readPhysicalInto(buf, position) < length) {
            throw new IOException("Unexpected end of translog file:" + filePath + " at offset:" + position);
        }
        buf.flip();
        return buf;
    }

    private int readPhysicalInto(ByteBuffer buf, long position) throws IOException {
        int total = 0;
        while (buf.hasRemaining()) {
            int read = delegate.read(buf, position + total);
            if (read <= 0) {
                break;
            }
            total += read;
        }
        return total;
    }

    private void writePhysical(ByteBuffer buf, long position) throws IOException {
        long written = 0;
        while (buf.hasRemaining()) {
            written += delegate.write(buf, position + written);
        }
    }

    /**
     * Reads and decrypts bytes at logical offset {@code position}. Header bytes are returned as stored.
     *
     * @param dst the buffer to read data into
     * @param position the logical file position to read from
     * @return the number of bytes read, or -1 at end of file
     * @throws IOException if reading or authentication fails
     */
    public int readFromChunks(ByteBuffer dst, long position) throws IOException {
        if (dst.remaining() == 0) {
            return 0;
        }
        if (position < actualHeaderSize) {
            long headerEnd = Math.min(actualHeaderSize, logicalSize());
            if (position >= headerEnd) {
                return -1;
            }
            ByteBuffer headerPart = dst.duplicate();
            headerPart.limit(headerPart.position() + (int) Math.min(dst.remaining(), headerEnd - position));
            int read = delegate.read(headerPart, position);
            if (read > 0) {
                dst.position(dst.position() + read);
            }
            return read;
        }
        int total = 0;
        while (dst.hasRemaining()) {
            Frame frame = findFrame(position + total);
            if (frame == null) {
                break;
            }
            byte[] plaintext = decryptFrame(frame);
            int offsetInFrame = (int) (position + total - frame.logicalStart());
            int n = Math.min(dst.remaining(), frame.length() - offsetInFrame);
            dst.put(plaintext, offsetInFrame, n);
            total += n;
        }
        return total == 0 ? -1 : total;
    }

    /**
     * Encrypts and appends bytes at logical offset {@code position}, sealing them as one or more
     * frames. Header bytes are written as given. Translog files are append-only, so
     * {@code position} must be the logical end of the file.
     *
     * @param src the buffer containing data to write
     * @param position the logical file position to write to
     * @return the number of bytes written
     * @throws IOException if the write is not an append, or encryption or writing fails
     */
    public synchronized int writeToChunks(ByteBuffer src, long position) throws IOException {
        if (src.remaining() == 0) {
            return 0;
        }
        if (appendsBlocked) {
            throw new IOException(
                "Cannot append to translog file:"
                    + filePath
                    + ": it has an incomplete frame after logical offset "
                    + logicalEnd
                    + " (a torn or failed write); appending would reuse that frame's nonce"
            );
        }
        if (position < actualHeaderSize) {
            if (formatHeaderPresent) {
                throw new IOException("Cannot rewrite the header of translog file:" + filePath);
            }
            ByteBuffer headerPart = src.duplicate();
            headerPart.limit(headerPart.position() + (int) Math.min(src.remaining(), actualHeaderSize - position));
            int written = delegate.write(headerPart, position);
            src.position(src.position() + written);
            logicalEnd = physicalEnd = Math.max(logicalEnd, position + written);
            return written;
        }
        if (position != logicalEnd) {
            throw new IOException(
                "Encrypted translog files are append-only: write at " + position + ", logical end " + logicalEnd + " in file:" + filePath
            );
        }
        int total = 0;
        boolean completed = false;
        try {
            if (!formatHeaderPresent) {
                ByteBuffer format = ByteBuffer.allocate(FORMAT_HEADER_SIZE);
                format.putInt(FORMAT_MAGIC).putInt(FORMAT_VERSION).putInt(newFileMaxFrameSize).flip();
                writePhysical(format, actualHeaderSize);
                maxFrameSize = newFileMaxFrameSize;
                formatHeaderPresent = true;
                physicalEnd = actualHeaderSize + FORMAT_HEADER_SIZE;
            }
            Cipher cipher = CIPHER.get();
            while (src.hasRemaining()) {
                int length = Math.min(src.remaining(), maxFrameSize);
                byte[] plaintext = new byte[length];
                src.get(plaintext);
                byte[] frame = new byte[FRAME_OVERHEAD + length];
                ByteBuffer.wrap(frame).putInt(length);
                initCipher(cipher, Cipher.ENCRYPT_MODE, frameCount, logicalEnd, length);
                cipher.doFinal(plaintext, 0, length, frame, FRAME_HEADER_SIZE);
                writePhysical(ByteBuffer.wrap(frame), physicalEnd);
                addFrame(length);
                total += length;
            }
            completed = true;
        } catch (GeneralSecurityException e) {
            throw new IOException("Failed to seal translog frame " + frameCount + " in file:" + filePath, e);
        } finally {
            if (completed == false) {
                // Part of a frame may be on disk; never write another frame under its index.
                appendsBlocked = true;
            }
        }
        return total;
    }

    /**
     * Truncating is only supported as a no-op ({@code size} at or past the end). Appending after a real
     * truncation would seal different plaintext under the nonce of a removed frame.
     *
     * @param size the new logical size
     * @throws IOException if {@code size} is before the logical end of the file
     */
    public synchronized void truncate(long size) throws IOException {
        if (size < logicalEnd) {
            throw new IOException(
                "Encrypted translog files cannot be truncated (size " + size + ", logical end " + logicalEnd + ") in file:" + filePath
            );
        }
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
     * No-op: every write is sealed into complete frames as it happens, so there is nothing to
     * finalize. Kept for callers that finalize the channel before upload.
     */
    public void close() {}
}

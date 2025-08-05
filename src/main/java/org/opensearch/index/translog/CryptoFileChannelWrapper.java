/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.translog;

import static org.opensearch.index.store.cipher.AesCipherFactory.computeOffsetIVForAesGcmEncrypted;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.security.Key;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.opensearch.index.store.cipher.AesGcmCipherFactory;
import org.opensearch.index.store.iv.KeyIvResolver;

/**
 * A FileChannel wrapper that provides transparent AES-GCM encryption/decryption
 * for translog files using 8KB authenticated chunks.
 *
 * This approach ensures OpenSearch can read translog metadata while keeping
 * the actual translog operations encrypted with authentication.
 *
 * File Format:
 * [TranslogHeader - Unencrypted]
 * [Chunk 0: ≤8KB encrypted + 16B auth tag]
 * [Chunk 1: ≤8KB encrypted + 16B auth tag]
 * ...
 * [Last Chunk: ≤8KB encrypted + 16B auth tag]
 *
 * @opensearch.internal
 */
public class CryptoFileChannelWrapper extends FileChannel {

    private static final Logger logger = LogManager.getLogger(CryptoFileChannelWrapper.class);

    // GCM chunk constants
    private static final int GCM_CHUNK_SIZE = 8192;                                    // 8KB data per chunk
    private static final int GCM_TAG_SIZE = AesGcmCipherFactory.GCM_TAG_LENGTH;       // 16 bytes auth tag
    private static final int CHUNK_WITH_TAG_SIZE = GCM_CHUNK_SIZE + GCM_TAG_SIZE;     // 8208 bytes max

    private final FileChannel delegate;
    private final KeyIvResolver keyIvResolver;
    private final Path filePath;
    private final String translogUUID;
    private final AtomicLong position;
    private final ReentrantReadWriteLock positionLock;
    private volatile boolean closed = false;

    // Header size - calculated exactly using TranslogHeader.headerSizeInBytes()
    private volatile int actualHeaderSize = -1;

    // TranslogHeader constants replicated to avoid cross-classloader access
    private static final String TRANSLOG_CODEC = "translog";
    private static final int VERSION_PRIMARY_TERM = 3;
    private static final int CURRENT_VERSION = VERSION_PRIMARY_TERM;

    /**
     * Helper class for chunk position mapping
     */
    private static class ChunkInfo {
        final int chunkIndex;           // Which chunk (0, 1, 2, ...)
        final int offsetInChunk;        // Position within the 8KB chunk
        final long diskPosition;        // Actual file position of chunk start

        ChunkInfo(int chunkIndex, int offsetInChunk, long diskPosition) {
            this.chunkIndex = chunkIndex;
            this.offsetInChunk = offsetInChunk;
            this.diskPosition = diskPosition;
        }
    }

    /**
     * Creates a new CryptoFileChannelWrapper that wraps the provided FileChannel.
     *
     * @param delegate the underlying FileChannel to wrap
     * @param keyIvResolver the key and IV resolver for encryption (unified with index files)
     * @param path the file path (used for logging and debugging)
     * @param options the file open options (used for logging and debugging)
     * @param translogUUID the translog UUID for exact header size calculation
     * @throws IOException if there is an error setting up the channel
     */
    public CryptoFileChannelWrapper(
        FileChannel delegate,
        KeyIvResolver keyIvResolver,
        Path path,
        Set<OpenOption> options,
        String translogUUID
    )
        throws IOException {
        if (translogUUID == null) {
            throw new IllegalArgumentException("translogUUID is required for exact header size calculation");
        }
        this.delegate = delegate;
        this.keyIvResolver = keyIvResolver;
        this.filePath = path;
        this.translogUUID = translogUUID;
        this.position = new AtomicLong(delegate.position());
        this.positionLock = new ReentrantReadWriteLock();
    }

    /**
     * Determines the exact header size using local calculation to avoid cross-classloader access.
     * This replicates the exact same logic as TranslogHeader.headerSizeInBytes() method.
     */
    private int determineHeaderSize() {
        if (actualHeaderSize > 0) {
            return actualHeaderSize;
        }

        String fileName = filePath.getFileName().toString();
        if (fileName.endsWith(".tlog")) {
            actualHeaderSize = calculateTranslogHeaderSize(translogUUID);
            logger.debug("Calculated exact header size: {} bytes for {} with UUID: {}", actualHeaderSize, filePath, translogUUID);
        } else {
            // Non-translog files (.ckp) don't need encryption anyway
            actualHeaderSize = 0;
            logger.debug("Non-translog file {}, header size: 0", filePath);
        }

        return actualHeaderSize;
    }

    /**
     * Local implementation of TranslogHeader.headerSizeInBytes() to avoid cross-classloader access issues.
     * This replicates the exact same calculation as the original method.
     *
     * @param translogUUID the translog UUID
     * @return the header size in bytes
     */
    private static int calculateTranslogHeaderSize(String translogUUID) {
        // Replicate: headerSizeInBytes(CURRENT_VERSION, new BytesRef(translogUUID).length)
        int uuidLength = translogUUID.getBytes().length;

        // Replicate the internal calculation
        int size = CodecUtil.headerLength(TRANSLOG_CODEC); // Lucene codec header
        size += Integer.BYTES + uuidLength; // uuid length field + uuid bytes

        // VERSION_PRIMARY_TERM = 3, CURRENT_VERSION = 3
        if (CURRENT_VERSION >= VERSION_PRIMARY_TERM) {
            size += Long.BYTES;    // primary term
            size += Integer.BYTES; // checksum
        }

        return size;
    }

    /**
     * Maps a file position to chunk information.
     */
    private ChunkInfo getChunkInfo(long filePosition) {
        long dataPosition = filePosition - determineHeaderSize();
        int chunkIndex = (int) (dataPosition / GCM_CHUNK_SIZE);
        int offsetInChunk = (int) (dataPosition % GCM_CHUNK_SIZE);
        long diskPosition = determineHeaderSize() + ((long) chunkIndex * CHUNK_WITH_TAG_SIZE);
        return new ChunkInfo(chunkIndex, offsetInChunk, diskPosition);
    }

    /**
     * Reads and decrypts a complete chunk from disk.
     * Returns empty array if chunk doesn't exist or channel is write-only.
     */
    private byte[] readAndDecryptChunk(int chunkIndex) throws IOException {
        try {
            // Calculate disk position for this chunk
            long diskPosition = determineHeaderSize() + ((long) chunkIndex * CHUNK_WITH_TAG_SIZE);

            // Check if chunk exists and we can read it
            if (!canReadChunk(diskPosition)) {
                return new byte[0]; // New chunk or write-only channel
            }

            // Read encrypted chunk + tag from disk
            ByteBuffer buffer = ByteBuffer.allocate(CHUNK_WITH_TAG_SIZE);
            int bytesRead = delegate.read(buffer, diskPosition);
            if (bytesRead <= GCM_TAG_SIZE) {
                return new byte[0]; // Empty or invalid chunk
            }

            // Extract encrypted data with tag
            byte[] encryptedWithTag = new byte[bytesRead];
            buffer.flip();
            buffer.get(encryptedWithTag);

            // Use existing key management
            Key key = keyIvResolver.getDataKey();
            byte[] baseIV = keyIvResolver.getIvBytes();

            // Use existing IV computation for this chunk
            long chunkOffset = (long) chunkIndex * GCM_CHUNK_SIZE;
            byte[] chunkIV = computeOffsetIVForAesGcmEncrypted(baseIV, chunkOffset);

            // Use existing GCM decryption with authentication
            return AesGcmCipherFactory.decryptWithTag(key, chunkIV, encryptedWithTag);

        } catch (Exception e) {
            throw new IOException("Failed to decrypt chunk " + chunkIndex, e);
        }
    }

    /**
     * Checks if we can read a chunk at the given disk position.
     * Returns false for write-only channels or if chunk doesn't exist.
     */
    private boolean canReadChunk(long diskPosition) {
        try {
            // Check if position is beyond current file size (new chunk)
            if (diskPosition >= delegate.size()) {
                return false;
            }

            // Test if channel is readable by attempting a zero-byte read
            ByteBuffer testBuffer = ByteBuffer.allocate(0);
            delegate.read(testBuffer, diskPosition);
            return true;

        } catch (java.nio.channels.NonReadableChannelException e) {
            // Channel is write-only
            return false;
        } catch (IOException e) {
            // Other read errors - assume can't read
            return false;
        }
    }

    /**
     * Encrypts and writes a complete chunk to disk.
     */
    private void encryptAndWriteChunk(int chunkIndex, byte[] plainData) throws IOException {
        try {
            // Use existing key management
            Key key = keyIvResolver.getDataKey();
            byte[] baseIV = keyIvResolver.getIvBytes();

            // Use existing IV computation for this chunk
            long chunkOffset = (long) chunkIndex * GCM_CHUNK_SIZE;
            byte[] chunkIV = computeOffsetIVForAesGcmEncrypted(baseIV, chunkOffset);

            // Use existing GCM encryption (includes authentication tag)
            byte[] encryptedWithTag = AesGcmCipherFactory.encryptWithTag(key, chunkIV, plainData, plainData.length);

            // Write to disk at chunk position
            long diskPosition = determineHeaderSize() + ((long) chunkIndex * CHUNK_WITH_TAG_SIZE);
            ByteBuffer buffer = ByteBuffer.wrap(encryptedWithTag);
            delegate.write(buffer, diskPosition);

        } catch (Exception e) {
            throw new IOException("Failed to encrypt chunk " + chunkIndex + " in file " + filePath, e);
        }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return read(dst, position.get());
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        ensureOpen();
        if (dst.remaining() == 0) {
            return 0;
        }

        positionLock.writeLock().lock();
        try {
            // Update position tracking for non-position-specific reads
            if (position == this.position.get()) {
                this.position.addAndGet(dst.remaining());
            }

            int headerSize = determineHeaderSize();

            // Header reads remain unchanged
            if (position < headerSize) {
                return delegate.read(dst, position);
            }

            // Chunk-based reading for encrypted data
            ChunkInfo chunkInfo = getChunkInfo(position);

            // Read and decrypt the needed chunk
            byte[] decryptedChunk = readAndDecryptChunk(chunkInfo.chunkIndex);

            // Extract requested data from decrypted chunk
            int available = Math.max(0, decryptedChunk.length - chunkInfo.offsetInChunk);
            int toRead = Math.min(dst.remaining(), available);

            if (toRead > 0) {
                dst.put(decryptedChunk, chunkInfo.offsetInChunk, toRead);
            }

            return toRead;
        } finally {
            positionLock.writeLock().unlock();
        }
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        ensureOpen();

        long totalBytesRead = 0;
        long currentPosition = position.get();

        for (int i = offset; i < offset + length && i < dsts.length; i++) {
            ByteBuffer dst = dsts[i];
            if (dst.remaining() > 0) {
                int bytesRead = read(dst, currentPosition + totalBytesRead);
                if (bytesRead <= 0) {
                    break;
                }
                totalBytesRead += bytesRead;
            }
        }

        if (totalBytesRead > 0) {
            position.addAndGet(totalBytesRead);
        }

        return totalBytesRead;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        long currentPosition = position.get();
        int bytesWritten = write(src, currentPosition);
        if (bytesWritten > 0) {
            position.addAndGet(bytesWritten);
        }
        return bytesWritten;
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        ensureOpen();
        if (src.remaining() == 0) {
            return 0;
        }

        positionLock.writeLock().lock();
        try {
            int headerSize = determineHeaderSize();

            // Header writes remain unchanged
            if (position < headerSize) {
                return delegate.write(src, position);
            }

            // Chunk-based encrypted writes
            ChunkInfo chunkInfo = getChunkInfo(position);

            // Read-modify-write chunk pattern
            byte[] existingChunk = readAndDecryptChunk(chunkInfo.chunkIndex);

            // Expand chunk buffer if needed
            int requiredSize = chunkInfo.offsetInChunk + src.remaining();
            byte[] modifiedChunk = existingChunk;
            if (existingChunk.length < requiredSize) {
                modifiedChunk = new byte[Math.min(requiredSize, GCM_CHUNK_SIZE)];
                System.arraycopy(existingChunk, 0, modifiedChunk, 0, existingChunk.length);
            }

            // Apply modifications
            int bytesToWrite = Math.min(src.remaining(), GCM_CHUNK_SIZE - chunkInfo.offsetInChunk);
            src.get(modifiedChunk, chunkInfo.offsetInChunk, bytesToWrite);

            // Encrypt and write back
            encryptAndWriteChunk(chunkInfo.chunkIndex, modifiedChunk);

            return bytesToWrite;
        } finally {
            positionLock.writeLock().unlock();
        }
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        ensureOpen();

        long totalBytesWritten = 0;
        long currentPosition = position.get();

        for (int i = offset; i < offset + length && i < srcs.length; i++) {
            ByteBuffer src = srcs[i];
            if (src.remaining() > 0) {
                int bytesWritten = write(src, currentPosition + totalBytesWritten);
                if (bytesWritten <= 0) {
                    break;
                }
                totalBytesWritten += bytesWritten;
            }
        }

        if (totalBytesWritten > 0) {
            position.addAndGet(totalBytesWritten);
        }

        return totalBytesWritten;
    }

    @Override
    public long position() throws IOException {
        ensureOpen();
        return position.get();
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
        ensureOpen();
        delegate.position(newPosition);
        position.set(newPosition);
        return this;
    }

    @Override
    public long size() throws IOException {
        ensureOpen();
        return delegate.size();
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        ensureOpen();
        delegate.truncate(size);
        long currentPosition = position.get();
        if (currentPosition > size) {
            position.set(size);
        }
        return this;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        ensureOpen();
        delegate.force(metaData);
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        ensureOpen();

        // For encrypted files, we need to decrypt data during transfer
        long transferred = 0;
        long remaining = count;
        ByteBuffer buffer = ByteBuffer.allocate(GCM_CHUNK_SIZE);

        while (remaining > 0 && transferred < count) {
            buffer.clear();
            int toRead = (int) Math.min(buffer.remaining(), remaining);
            buffer.limit(toRead);

            int bytesRead = read(buffer, position + transferred);
            if (bytesRead <= 0) {
                break;
            }

            buffer.flip();
            int bytesWritten = target.write(buffer);
            transferred += bytesWritten;
            remaining -= bytesWritten;

            if (bytesWritten < bytesRead) {
                break;
            }
        }

        return transferred;
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        ensureOpen();

        // For encrypted files, we need to encrypt data during transfer
        long transferred = 0;
        long remaining = count;
        ByteBuffer buffer = ByteBuffer.allocate(GCM_CHUNK_SIZE);

        while (remaining > 0 && transferred < count) {
            buffer.clear();
            int toRead = (int) Math.min(buffer.remaining(), remaining);
            buffer.limit(toRead);

            int bytesRead = src.read(buffer);
            if (bytesRead <= 0) {
                break;
            }

            buffer.flip();
            int bytesWritten = write(buffer, position + transferred);
            transferred += bytesWritten;
            remaining -= bytesWritten;

            if (bytesWritten < bytesRead) {
                break;
            }
        }

        return transferred;
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        ensureOpen();
        return delegate.lock(position, size, shared);
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        ensureOpen();
        return delegate.tryLock(position, size, shared);
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        ensureOpen();

        // For encrypted files, we cannot support memory mapping directly
        // because the mapped memory would contain encrypted data
        throw new UnsupportedOperationException(
            "Memory mapping is not supported for encrypted translog files. "
                + "Encrypted files require data to be decrypted during read operations."
        );
    }

    @Override
    protected void implCloseChannel() throws IOException {
        if (!closed) {
            closed = true;
            delegate.close();
        }
    }

    private void ensureOpen() throws ClosedChannelException {
        if (closed || !delegate.isOpen()) {
            throw new ClosedChannelException();
        }
    }
}

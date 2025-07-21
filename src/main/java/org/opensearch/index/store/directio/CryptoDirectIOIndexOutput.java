/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.Pool;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.RefCountedMemorySegmentCacheValue;
import org.opensearch.index.store.cipher.OpenSslNativeCipher;
import static org.opensearch.index.store.directio.DirectIOReader.getDirectOpenOption;
import static org.opensearch.index.store.directio.DirectIoConfigs.DIRECT_IO_ALIGNMENT;
import static org.opensearch.index.store.directio.DirectIoConfigs.SEGMENT_SIZE_BYTES;
import org.opensearch.index.store.iv.KeyIvResolver;

@SuppressWarnings("preview")
@SuppressForbidden(reason = "uses custom DirectIO")
public class CryptoDirectIOIndexOutput extends IndexOutput {
    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectIOIndexOutput.class);

    private static final int BUFFER_SIZE = SEGMENT_SIZE_BYTES;

    private final Pool<MemorySegment> memorySegmentPool;
    private final BlockCache<MemorySegment> blockCache;
    private final FileChannel channel;
    private final KeyIvResolver keyIvResolver;
    private final ByteBuffer buffer;
    private final Checksum digest;
    private final Path path;

    private long filePos;
    private boolean isOpen;

    private final ByteBuffer encryptedBuffer = ByteBuffer
        .allocateDirect(BUFFER_SIZE + DIRECT_IO_ALIGNMENT)
        .alignedSlice(DIRECT_IO_ALIGNMENT);
    private final ByteBuffer zeroPaddingBuffer = ByteBuffer.allocate(DIRECT_IO_ALIGNMENT);

    public CryptoDirectIOIndexOutput(
        Path path,
        String name,
        KeyIvResolver keyIvResolver,
        Pool<MemorySegment> memorySegmentPool,
        BlockCache<MemorySegment> blockCache
    )
        throws IOException {
        super("DirectIOIndexOutput(path=\"" + path + "\")", name);
        this.keyIvResolver = keyIvResolver;
        this.memorySegmentPool = memorySegmentPool;
        this.blockCache = blockCache;
        this.path = path;

        this.buffer = ByteBuffer.allocateDirect(BUFFER_SIZE + DIRECT_IO_ALIGNMENT - 1).alignedSlice(DIRECT_IO_ALIGNMENT);

        this.channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW, getDirectOpenOption());
        this.digest = new BufferedChecksum(new CRC32());
        this.isOpen = true;
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
            int chunk = Math.min(buffer.remaining(), toWrite);
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
        if (size == 0)
            return;

        buffer.flip();
        byte[] key = keyIvResolver.getDataKey().getEncoded();
        byte[] iv = keyIvResolver.getIvBytes();
        int bytesWritten;

        // Plaintext snapshot for caching (must be done before zeroing/encryption)
        ByteBuffer plainCopy = buffer.slice(0, size).asReadOnlyBuffer();

        try {
            // Encrypt
            MemorySegment inputSeg = MemorySegment.ofBuffer(buffer.slice(0, size));
            MemorySegment outputSeg = MemorySegment.ofBuffer(encryptedBuffer);
            try (Arena arena = Arena.ofConfined()) {
                bytesWritten = OpenSslNativeCipher.encryptInto(arena, key, iv, inputSeg, outputSeg, filePos);
            }

            // Pad to alignment
            int paddedLen = ((bytesWritten + DIRECT_IO_ALIGNMENT - 1) / DIRECT_IO_ALIGNMENT) * DIRECT_IO_ALIGNMENT;
            if (bytesWritten < paddedLen) {
                encryptedBuffer.position(bytesWritten);
                encryptedBuffer.put(zeroPaddingBuffer.clear().limit(paddedLen - bytesWritten));
            }

            // Write to disk
            encryptedBuffer.position(0).limit(paddedLen);
            int written = channel.write(encryptedBuffer, filePos);
            if (written != paddedLen) {
                throw new IOException("Incomplete write: expected=" + paddedLen + ", wrote=" + written);
            }

            // Zero out encrypted buffer
            MemorySegment.ofBuffer(encryptedBuffer).fill((byte) 0);
            encryptedBuffer.clear();

            // Cache plaintext if it was a full aligned block
            tryCachePlaintextBlock(plainCopy, size, filePos);

            filePos += size;
            buffer.clear();

        } catch (Throwable t) {
            throw new IOException("Encryption failed at offset " + filePos, t);
        }
    }

    private void tryCachePlaintextBlock(ByteBuffer plainCopy, int size, long offset) {
        if (size != BUFFER_SIZE)
            return;

        try {
            final MemorySegment pooled = memorySegmentPool.tryAcquire(10, TimeUnit.MILLISECONDS);
            if (pooled == null) {
                LOGGER.debug("Memory pool segment not available within timeout; skipping cache for {}", path);
                return;
            }

            final MemorySegment pooledSlice = pooled.asSlice(0, size);
            final MemorySegment plainSegment = MemorySegment.ofBuffer(plainCopy);

            MemorySegment.copy(plainSegment, 0, pooledSlice, 0, size);

            BlockCacheKey cacheKey = new DirectIOBlockCacheKey(path, offset);

            RefCountedMemorySegment refSegment = new RefCountedMemorySegment(
                pooled,
                size,
                segment -> memorySegmentPool.release(pooled) // SegmentReleaser
            );
            RefCountedMemorySegmentCacheValue cacheValue = new RefCountedMemorySegmentCacheValue(refSegment);

            blockCache.put(cacheKey, cacheValue);

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted while acquiring segment for cache.");
        } catch (IllegalStateException e) {
            LOGGER.debug("Failed to acquire segment from pool; skipping decrypted cache.");
        }
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

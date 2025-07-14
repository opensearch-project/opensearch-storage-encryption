/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIoUtils.getDirectOpenOption;

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
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.MemorySegmentPool;
import org.opensearch.index.store.block_cache.PooledBlockCacheValue;
import org.opensearch.index.store.cipher.OpenSslNativeCipher;
import org.opensearch.index.store.iv.KeyIvResolver;
import org.opensearch.index.store.mmap.PanamaNativeAccess;

@SuppressWarnings("preview")
public class CryptoDirectIOIndexOutput extends IndexOutput {
    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectIOIndexOutput.class);

    // todo derive from segment size of the pool.
    private static final int BUFFER_SIZE = 65_536;
    private static final int BLOCK_SIZE = PanamaNativeAccess.getPageSize(); // often returns 4096

    private final MemorySegmentPool memorySegmentPool;
    private final BlockCache blockCache;
    private final FileChannel channel;
    private final KeyIvResolver keyIvResolver;
    private final ByteBuffer buffer;
    private final Checksum digest;
    private final Path path;

    private long filePos;
    private boolean isOpen;

    private final ByteBuffer encryptedBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE + BLOCK_SIZE).alignedSlice(BLOCK_SIZE);
    private final ByteBuffer zeroPaddingBuffer = ByteBuffer.allocate(BLOCK_SIZE);

    public CryptoDirectIOIndexOutput(
        Path path,
        String name,
        KeyIvResolver keyIvResolver,
        MemorySegmentPool memorySegmentPool,
        BlockCache blockCache
    )
        throws IOException {
        super("DirectIOIndexOutput(path=\"" + path.toString() + "\")", name);

        this.keyIvResolver = keyIvResolver;
        this.memorySegmentPool = memorySegmentPool;
        this.blockCache = blockCache;
        this.path = path;

        buffer = ByteBuffer.allocateDirect(BUFFER_SIZE + BLOCK_SIZE - 1).alignedSlice(BLOCK_SIZE);
        channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW, getDirectOpenOption());
        digest = new BufferedChecksum(new CRC32());

        isOpen = true;
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
            final int left = buffer.remaining();
            final int chunk = Math.min(left, toWrite);
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
        MemorySegment pooled = null;
        long alignedOffset = filePos & ~(BLOCK_SIZE - 1); // Capture before increment

        try {
            // Encrypt
            MemorySegment inputSeg = MemorySegment.ofBuffer(buffer.slice(0, size));
            MemorySegment outputSeg = MemorySegment.ofBuffer(encryptedBuffer);

            try (Arena arena = Arena.ofConfined()) {
                bytesWritten = OpenSslNativeCipher.encryptInto(arena, key, iv, inputSeg, outputSeg, filePos);
            }

            inputSeg.fill((byte) 0);

            // Pad to alignment
            int paddedLen = ((bytesWritten + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE;
            if (bytesWritten < paddedLen) {
                encryptedBuffer.position(bytesWritten);
                int paddingSize = paddedLen - bytesWritten;
                zeroPaddingBuffer.clear().limit(paddingSize);
                encryptedBuffer.put(zeroPaddingBuffer);
            }

            // Write encrypted + padded data
            encryptedBuffer.position(0).limit(paddedLen);
            int written = channel.write(encryptedBuffer, filePos);
            if (written != paddedLen) {
                throw new IOException("Incomplete write: expected=" + paddedLen + ", wrote=" + written);
            }

            MemorySegment.ofBuffer(encryptedBuffer).fill((byte) 0);
            encryptedBuffer.clear();

            // Try acquiring a segment for caching decrypted content
            try {
                pooled = memorySegmentPool.tryAcquire(10, TimeUnit.MILLISECONDS);

                if (pooled != null) {
                    // Copy decrypted data into pooled segment
                    MemorySegment pooledSlice = pooled.asSlice(0, size);
                    MemorySegment plain = MemorySegment.ofBuffer(buffer.slice(0, size));
                    MemorySegment.copy(plain, 0, pooledSlice, 0, size);
                    plain.fill((byte) 0);

                    // Add to block cache
                    BlockCacheKey cacheKey = new BlockCacheKey(path, alignedOffset);
                    blockCache.put(cacheKey, new PooledBlockCacheValue(pooled, size, memorySegmentPool));

                    pooled = null; // ownership transferred to cache
                } else {
                    LOGGER.debug("Memory pool segment not available within timeout; skipping cache population.");
                }

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOGGER.warn("Interrupted while acquiring segment from pool; skipping decrypted cache.");
            } catch (IllegalStateException e) {
                LOGGER.debug("Could not acquire segment from pool in time; skipping decrypted cache.");
            }

            filePos += size;
            buffer.clear();

        } catch (Throwable t) {
            throw new IOException("Encryption failed at offset " + filePos, t);
        } finally {
            if (pooled != null) {
                pooled.fill((byte) 0);
                memorySegmentPool.release(pooled);
            }
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

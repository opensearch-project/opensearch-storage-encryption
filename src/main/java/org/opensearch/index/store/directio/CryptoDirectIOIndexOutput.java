/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIOReader.getDirectOpenOption;
import static org.opensearch.index.store.directio.DirectIoConfigs.DIRECT_IO_ALIGNMENT;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
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

@SuppressWarnings("preview")
@SuppressForbidden(reason = "uses custom DirectIO")
public class CryptoDirectIOIndexOutput extends IndexOutput {
    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectIOIndexOutput.class);

    private static final int BUFFER_SIZE = 262144;

    private final Pool<MemorySegment> memorySegmentPool;
    private final BlockCache<RefCountedMemorySegment> blockCache;
    private final FileChannel channel;
    private final ByteBuffer buffer;            // logical data buffer
    private final ByteBuffer ioBuffer;          // staging buffer for O_DIRECT writes
    private final ByteBuffer zeroPaddingBuffer; // aligned zero pad
    private final Checksum digest;
    private final Path path;

    // Positions
    private long physicalPos = 0L; // disk position (aligned, includes padding)
    private long logicalSize = 0L; // logical size (excludes padding)

    private boolean isOpen = true;
    private final boolean shouldAddToBufferPool;

    public CryptoDirectIOIndexOutput(
        Path path,
        String name,
        Pool<MemorySegment> memorySegmentPool,
        BlockCache<RefCountedMemorySegment> blockCache,
        boolean shouldAddToBufferPool
    )
        throws IOException {
        super("DirectIOIndexOutput(path=\"" + path + "\")", name);
        this.memorySegmentPool = memorySegmentPool;
        this.blockCache = blockCache;
        this.path = path;
        this.shouldAddToBufferPool = shouldAddToBufferPool;

        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }

        this.channel = FileChannel
            .open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, getDirectOpenOption());

        // Main logical buffer
        this.buffer = ByteBuffer.allocateDirect(BUFFER_SIZE + DIRECT_IO_ALIGNMENT - 1).alignedSlice(DIRECT_IO_ALIGNMENT);

        // IO staging buffer for one aligned write
        this.ioBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE + DIRECT_IO_ALIGNMENT).alignedSlice(DIRECT_IO_ALIGNMENT);

        // Zero padding buffer
        this.zeroPaddingBuffer = ByteBuffer.allocateDirect(DIRECT_IO_ALIGNMENT + DIRECT_IO_ALIGNMENT - 1).alignedSlice(DIRECT_IO_ALIGNMENT);

        this.digest = new BufferedChecksum(new CRC32());
    }

    @Override
    public void writeByte(byte b) throws IOException {
        if (!buffer.hasRemaining()) {
            flushToDisk();
        }
        buffer.put(b);
        digest.update(b);
    }

    @Override
    public void writeBytes(byte[] src, int offset, int len) throws IOException {
        int toWrite = len;
        while (toWrite > 0) {
            int left = buffer.remaining();
            if (left == 0) {
                flushToDisk();
                left = buffer.remaining();
            }
            int chunk = Math.min(left, toWrite);
            buffer.put(src, offset, chunk);
            digest.update(src, offset, chunk);
            offset += chunk;
            toWrite -= chunk;
        }
    }

    /**
     * Flush the logical buffer to disk in a single aligned O_DIRECT write.
     */
    private void flushToDisk() throws IOException {
        final int size = buffer.position();
        if (size == 0)
            return;

        // Snapshot plaintext for cache before manipulating buffers
        ByteBuffer plainCopy = buffer.asReadOnlyBuffer();
        plainCopy.flip();

        // Build staging buffer = data + zero padding
        buffer.flip();       // position=0, limit=size
        ioBuffer.clear();
        ioBuffer.put(buffer);

        int rem = size % DIRECT_IO_ALIGNMENT;
        int pad = (rem == 0) ? 0 : (DIRECT_IO_ALIGNMENT - rem);
        if (pad != 0) {
            zeroPaddingBuffer.clear();
            zeroPaddingBuffer.limit(pad);
            ioBuffer.put(zeroPaddingBuffer);
        }

        ioBuffer.flip(); // position=0, limit=size+pad (aligned length)

        // Single aligned write
        int totalWriteLen = size + pad;
        int written = channel.write(ioBuffer, physicalPos);
        if (written != totalWriteLen) {
            throw new IOException("Incomplete write: expected=" + totalWriteLen + ", wrote=" + written);
        }

        // Cache plaintext block if applicable
        if (shouldAddToBufferPool) {
            tryCachePlaintextBlock(plainCopy, size, logicalSize);
        }

        // Advance positions
        physicalPos += totalWriteLen;
        logicalSize += size;

        buffer.clear();
    }

    private void tryCachePlaintextBlock(ByteBuffer plainCopy, int size, long offset) {
        if (size != BUFFER_SIZE || memorySegmentPool.isUnderPressure()) {
            return;
        }

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
            RefCountedMemorySegment refSegment = new RefCountedMemorySegment(pooled, size, seg -> memorySegmentPool.release(pooled));
            RefCountedMemorySegmentCacheValue cacheValue = new RefCountedMemorySegmentCacheValue(refSegment);
            blockCache.put(cacheKey, cacheValue);

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted while acquiring segment for cache.");
        } catch (IllegalStateException e) {
            LOGGER.debug("Failed to acquire segment from pool; skipping cache.");
        }
    }

    @Override
    public long getFilePointer() {
        return logicalSize + buffer.position();
    }

    @Override
    public long getChecksum() {
        return digest.getValue();
    }

    @Override
    public void close() throws IOException {
        if (!isOpen)
            return;
        isOpen = false;

        IOException thrown = null;
        try {
            flushToDisk();
            try {
                // Trim any padding from the tail so readers see exact logical size
                channel.truncate(logicalSize);
            } catch (IOException ioe) {
                thrown = ioe;
            }
        } finally {
            try {
                channel.close();
            } catch (IOException ioe) {
                if (thrown == null)
                    thrown = ioe;
                else
                    thrown.addSuppressed(ioe);
            }
        }
        if (thrown != null)
            throw thrown;
    }
}

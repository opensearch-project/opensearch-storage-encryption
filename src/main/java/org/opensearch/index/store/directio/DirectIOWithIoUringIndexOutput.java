/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.async_io.IoUringFile;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.Pool;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.RefCountedMemorySegmentCacheValue;
import static org.opensearch.index.store.directio.DirectIOReader.getDirectOpenOption;
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_MASK;
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE;
import static org.opensearch.index.store.directio.DirectIoConfigs.DIRECT_IO_ALIGNMENT;
import static org.opensearch.index.store.directio.DirectIoConfigs.DIRECT_IO_WRITE_BUFFER_SIZE_POWER;

import io.netty.channel.IoEventLoopGroup;

@SuppressWarnings("preview")
@SuppressForbidden(reason = "uses custom DirectIO")
public class DirectIOWithIoUringIndexOutput extends IndexOutput {
    private static final Logger LOGGER = LogManager.getLogger(DirectIOWithIoUringIndexOutput.class);

    private static final int BUFFER_SIZE = 1 << DIRECT_IO_WRITE_BUFFER_SIZE_POWER;

    // Debug tracking
    private final ByteBuffer debugTracker = ByteBuffer.allocate(1024 * 1024); // Track first 1MB for verification

    private final Pool<MemorySegment> memorySegmentPool;
    private final BlockCache<RefCountedMemorySegment> blockCache;
    private final FileChannel channel;          // for sync operations (truncate)
    private final IoUringFile ioUringFile;      // for async write operations
    private final ByteBuffer buffer;            // logical data buffer
    private final ByteBuffer zeroPaddingBuffer; // aligned zero pad
    private final Checksum digest;
    private final Path path;
    private final IoEventLoopGroup group;

    // Async write management
    private final ConcurrentLinkedQueue<CompletableFuture<Integer>> pendingWrites = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pendingCount = new AtomicInteger(0);

    // Positions
    private long physicalPos = 0L; // disk position (aligned, includes padding)
    private long logicalSize = 0L; // logical size (excludes padding)

    private boolean isOpen = true;

    public DirectIOWithIoUringIndexOutput(
        Path path,
        String name,
        Pool<MemorySegment> memorySegmentPool,
        BlockCache<RefCountedMemorySegment> blockCache,
        IoEventLoopGroup group
    )
        throws IOException {
        super("DirectIOIndexOutput(path=\"" + path + "\")", name);
        this.memorySegmentPool = memorySegmentPool;
        this.blockCache = blockCache;
        this.path = path;
        this.group = group;

        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }

        this.channel = FileChannel
            .open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, getDirectOpenOption());
        // Initialize IoUringFile for all I/O operations
        this.ioUringFile = IoUringFile
            .open(
                path.toFile(),
                this.group.next(),
                IoUringFile.getDirectOpenOption(),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
            )
            .join(); // Block on initialization

        // Main logical buffer
        this.buffer = ByteBuffer.allocateDirect(BUFFER_SIZE + DIRECT_IO_ALIGNMENT - 1).alignedSlice(DIRECT_IO_ALIGNMENT);

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

        // Track data for verification
        if (debugTracker.hasRemaining()) {
            debugTracker.put(b);
        }
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

            // Track data for verification
            int trackChunk = Math.min(chunk, debugTracker.remaining());
            if (trackChunk > 0) {
                debugTracker.put(src, offset, trackChunk);
            }

            offset += chunk;
            toWrite -= chunk;
        }
    }

    private void flushToDisk() throws IOException {
        CompletableFuture<Integer> writeFuture = flushToDiskAsync();
        pendingWrites.offer(writeFuture);
        pendingCount.incrementAndGet();

        writeFuture.whenComplete((result, throwable) -> {
            pendingCount.decrementAndGet();
            pendingWrites.remove(writeFuture);
        });

        // Clear logical buffer AFTER submission
        buffer.clear();
    }

    private CompletableFuture<Integer> flushToDiskAsync() throws IOException {
        final int size = buffer.position();
        if (size == 0)
            return CompletableFuture.completedFuture(0);

        buffer.flip(); // position = 0, limit = size

        // Create a dedicated buffer for this async operation to avoid races
        int rem = size % DIRECT_IO_ALIGNMENT;
        int pad = (rem == 0) ? 0 : (DIRECT_IO_ALIGNMENT - rem);
        int totalWriteLen = size + pad;

        ByteBuffer dedicatedBuffer = ByteBuffer.allocateDirect(totalWriteLen + DIRECT_IO_ALIGNMENT - 1).alignedSlice(DIRECT_IO_ALIGNMENT);
        dedicatedBuffer.put(buffer); // Copy actual content

        // Prepare smaller cache segments within the large write
        final List<CacheBlock> cacheBlocks = new ArrayList<>();
        if (!memorySegmentPool.isUnderPressure()) {
            prepareCacheBlocks(dedicatedBuffer, size, logicalSize, cacheBlocks);
        }

        if (pad > 0) {
            zeroPaddingBuffer.clear();
            zeroPaddingBuffer.limit(pad);
            dedicatedBuffer.put(zeroPaddingBuffer); // Add padding
        }

        dedicatedBuffer.flip(); // Ready to be read by io_uring

        long addr = MemorySegment.ofBuffer(dedicatedBuffer).address();
        long currentPhysicalPos = physicalPos;

        // Advance positions
        physicalPos += totalWriteLen;
        logicalSize += size;

        return ioUringFile.writeAsync(addr, totalWriteLen, currentPhysicalPos).thenApply(written -> {
            try {
                if (written != totalWriteLen) {
                    throw new IllegalStateException("Short write: expected=" + totalWriteLen + ", got=" + written);
                }

                // Cache all blocks after successful write
                for (CacheBlock block : cacheBlocks) {
                    tryCachePlaintextSegment(block.segment, CACHE_BLOCK_SIZE, block.offset);
                }

                return written;
            } finally {
                // Prevent GC too early (optional safety) - keep dedicatedBuffer alive
                MemorySegment.ofBuffer(dedicatedBuffer).address();
            }
        });
    }

    private void prepareCacheBlocks(ByteBuffer source, int size, long baseOffset, List<CacheBlock> blocks) {
        int remaining = size;
        int sourcePos = 0;
        
        while (remaining >= CACHE_BLOCK_SIZE) {
            long blockOffset = baseOffset + sourcePos;
            
            // Only cache aligned blocks
            if ((blockOffset & CACHE_BLOCK_MASK) == 0) {
                ByteBuffer blockSlice = source.duplicate();
                blockSlice.position(sourcePos).limit(sourcePos + CACHE_BLOCK_SIZE);
                MemorySegment blockSegment = MemorySegment.ofBuffer(blockSlice);
                blocks.add(new CacheBlock(blockSegment, blockOffset));
            }
            
            sourcePos += CACHE_BLOCK_SIZE;
            remaining -= CACHE_BLOCK_SIZE;
        }
    }

    private void tryCachePlaintextSegment(MemorySegment cacheSegment, int size, long offset) {
        try {
            final MemorySegment pooled = memorySegmentPool.tryAcquire(10, TimeUnit.MILLISECONDS);
            if (pooled == null) {
                LOGGER.debug("Memory pool segment not available within timeout; skipping cache for {}", path);
                return;
            }

            final MemorySegment pooledSlice = pooled.asSlice(0, size);
            MemorySegment.copy(cacheSegment, 0, pooledSlice, 0, size);

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

    private static class CacheBlock {
        final MemorySegment segment;
        final long offset;
        
        CacheBlock(MemorySegment segment, long offset) {
            this.segment = segment;
            this.offset = offset;
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
            // Final flush to ensure any remaining data is written
            flushToDisk();

            // Capture all pending writes at this moment to avoid race conditions
            CompletableFuture<Void> allWrites = CompletableFuture.allOf(pendingWrites.toArray(CompletableFuture[]::new));

            try {
                // Wait with timeout to avoid indefinite blocking
                allWrites.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new IOException("Failed to complete pending writes", e);
            }

            // Trim padding
            try {
                channel.truncate(logicalSize);
            } catch (IOException ioe) {
                if (thrown == null)
                    thrown = ioe;
                else
                    thrown.addSuppressed(ioe);
            }

            // Verify written data matches intended data
            // verifyWrittenData();

        } finally {
            try {
                ioUringFile.close();
            } catch (Exception e) {
                IOException ioe = new IOException("Failed to close IoUringFile", e);
                if (thrown == null)
                    thrown = ioe;
                else
                    thrown.addSuppressed(ioe);
            }
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

    @SuppressWarnings("unused")
    private void verifyWrittenData() throws IOException {
        if (logicalSize == 0)
            return;

        // Read back data and compare with what we intended to write
        try (FileChannel readChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            long readSize = Math.min(logicalSize, debugTracker.position());
            if (readSize == 0)
                return;

            ByteBuffer readBuffer = ByteBuffer.allocate((int) readSize);
            int totalRead = 0;
            while (totalRead < readSize) {
                int bytesRead = readChannel.read(readBuffer);
                if (bytesRead == -1)
                    break;
                totalRead += bytesRead;
            }

            readBuffer.flip();
            debugTracker.flip();

            // Compare byte by byte
            boolean mismatch = false;
            int mismatchOffset = -1;
            for (int i = 0; i < Math.min(readBuffer.remaining(), debugTracker.remaining()); i++) {
                byte written = readBuffer.get(i);
                byte intended = debugTracker.get(i);
                if (written != intended) {
                    mismatch = true;
                    mismatchOffset = i;
                    break;
                }
            }

            if (mismatch) {
                LOGGER
                    .error(
                        "DATA CORRUPTION DETECTED in {}: Mismatch at offset {}, intended=0x{}, actual=0x{}, fileLength={}, expectedLength={}",
                        path,
                        mismatchOffset,
                        Integer.toHexString(debugTracker.get(mismatchOffset) & 0xFF),
                        Integer.toHexString(readBuffer.get(mismatchOffset) & 0xFF),
                        totalRead,
                        logicalSize
                    );

                // Log context around mismatch
                int start = Math.max(0, mismatchOffset - 16);
                int end = Math.min(readBuffer.limit(), mismatchOffset + 16);
                StringBuilder intended = new StringBuilder();
                StringBuilder actual = new StringBuilder();
                for (int i = start; i < end; i++) {
                    intended.append(String.format("%02x ", debugTracker.get(i) & 0xFF));
                    actual.append(String.format("%02x ", readBuffer.get(i) & 0xFF));
                }
                LOGGER.error("Context around offset {}:\nIntended: {}\nActual:   {}", mismatchOffset, intended, actual);

                throw new IOException("Data corruption detected: written data does not match intended data");
            } else {
                LOGGER.info("Data verification PASSED for {} (verified {} bytes)", path, readSize);
            }
        }
    }

}

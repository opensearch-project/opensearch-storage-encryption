/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIoConfigs.SEGMENT_SIZE_BYTES;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.store.OutputStreamIndexOutput;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.Pool;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.RefCountedMemorySegmentCacheValue;

/**
 * An IndexOutput implementation that encrypts data before writing using native
 * OpenSSL AES-CTR.
 *
 * @opensearch.internal
 */
@SuppressWarnings("preview")
@SuppressForbidden(reason = "temporary bypass")
public final class BufferIOWithCaching extends OutputStreamIndexOutput {

    private static final int CHUNK_SIZE = SEGMENT_SIZE_BYTES;
    private static final int BUFFER_SIZE = 65_536;

    /**
     * Creates a new CryptoIndexOutput
     *
     * @param name The name of the output
     * @param path The path to write to
     * @param os The output stream
     * @param key The AES key (must be 32 bytes for AES-256)
     * @param iv The initialization vector (must be 16 bytes)
     * @throws IOException If there is an I/O error
     * @throws IllegalArgumentException If key or iv lengths are invalid
     */
    public BufferIOWithCaching(
        String name,
        Path path,
        OutputStream os,
        byte[] key,
        byte[] iv,
        Pool<MemorySegment> memorySegmentPool,
        BlockCache<RefCountedMemorySegment> blockCache,
        boolean shouldAddToBufferPool
    )
        throws IOException {
        super(
            "FSIndexOutput(path=\"" + path + "\")",
            name,
            new EncryptedOutputStream(os, path, key, iv, shouldAddToBufferPool, memorySegmentPool, blockCache),
            CHUNK_SIZE
        );
    }

    private static class EncryptedOutputStream extends FilterOutputStream {

        private final byte[] key;
        private final byte[] iv;
        private final byte[] buffer;
        private final Path path;
        private final Pool<MemorySegment> memorySegmentPool;
        private final BlockCache<RefCountedMemorySegment> blockCache;
        private final boolean shouldAddToBufferPool;

        private int bufferPosition = 0;
        private long streamOffset = 0;
        private boolean isClosed = false;

        EncryptedOutputStream(
            OutputStream os,
            Path path,
            byte[] key,
            byte[] iv,
            boolean shouldAddToBufferPool,
            Pool<MemorySegment> memorySegmentPool,
            BlockCache<RefCountedMemorySegment> blockCache
        ) {
            super(os);
            this.path = path;
            this.key = key;
            this.iv = iv;
            this.buffer = new byte[BUFFER_SIZE];
            this.memorySegmentPool = memorySegmentPool;
            this.blockCache = blockCache;
            this.shouldAddToBufferPool = shouldAddToBufferPool;
        }

        @Override
        public void write(byte[] b, int offset, int length) throws IOException {
            checkClosed();
            if (b == null) {
                throw new NullPointerException("Input buffer cannot be null");
            }
            if (offset < 0 || length < 0 || offset + length > b.length) {
                throw new IndexOutOfBoundsException("Invalid offset or length");
            }
            if (length == 0)
                return;

            if (length >= BUFFER_SIZE) {
                flushBuffer();
                processAndWrite(path, b, offset, length);
            } else if (bufferPosition + length > BUFFER_SIZE) {
                flushBuffer();
                System.arraycopy(b, offset, buffer, bufferPosition, length);
                bufferPosition += length;
            } else {
                System.arraycopy(b, offset, buffer, bufferPosition, length);
                bufferPosition += length;
            }
        }

        @Override
        public void write(int b) throws IOException {
            checkClosed();
            if (bufferPosition >= BUFFER_SIZE) {
                flushBuffer();
            }
            buffer[bufferPosition++] = (byte) b;
        }

        private void flushBuffer() throws IOException {
            if (bufferPosition > 0) {
                processAndWrite(path, buffer, 0, bufferPosition);
                bufferPosition = 0;
            }
        }

        private void processAndWrite(Path path, byte[] data, int arrayOffset, int length) throws IOException {
            out.write(data, arrayOffset, length);

            if (shouldAddToBufferPool) {
                tryCachePlaintextBlock(path, data, arrayOffset, length, streamOffset);
            }

            streamOffset += length;
        }

        private void tryCachePlaintextBlock(Path path, byte[] data, int arrayOffset, int size, long fileOffset) {
            if (size < SEGMENT_SIZE_BYTES) {
                return;
            }

            if (memorySegmentPool.isUnderPressure()) {
                return;
            }

            try {
                MemorySegment pooled = memorySegmentPool.tryAcquire(10, TimeUnit.MILLISECONDS);
                if (pooled == null) {
                    return;
                }

                // Copy directly from byte array to pooled MemorySegment - eliminates intermediate allocations
                MemorySegment.copy(MemorySegment.ofArray(data), arrayOffset, pooled, 0, SEGMENT_SIZE_BYTES);

                // Insert into block cache
                BlockCacheKey cacheKey = new DirectIOBlockCacheKey(path, fileOffset);
                RefCountedMemorySegment refSegment = new RefCountedMemorySegment(
                    pooled,
                    SEGMENT_SIZE_BYTES,
                    seg -> memorySegmentPool.release(pooled)
                );
                blockCache.put(cacheKey, new RefCountedMemorySegmentCacheValue(refSegment));

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (IllegalStateException e) {
                // optional: LOGGER.debug("Failed to cache block at {}: {}", fileOffset, e.toString());
            }
        }

        @Override
        public void close() throws IOException {
            IOException exception = null;

            try {
                checkClosed();
                flushBuffer();
                // Lucene writes footer here.
                // this will also flush the buffer.
                super.close();
            } catch (IOException e) {
                exception = e;
            } finally {
                isClosed = true;
            }

            if (exception != null)
                throw exception;
        }

        private void checkClosed() throws IOException {
            if (isClosed) {
                throw new IOException("Outout stream is already closed, this is unusual");
            }
        }
    }
}

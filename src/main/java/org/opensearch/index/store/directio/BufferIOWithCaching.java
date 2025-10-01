/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_MASK;
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE;
import static org.opensearch.index.store.directio.DirectIoConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.PanamaNativeAccess;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;
import org.opensearch.index.store.cipher.OpenSslNativeCipher;
import org.opensearch.index.store.pool.Pool;

/**
 * An IndexOutput implementation that encrypts data before writing using native
 * OpenSSL AES-CTR.
 *
 * @opensearch.internal
 */
@SuppressWarnings("preview")
@SuppressForbidden(reason = "temporary bypass")
public final class BufferIOWithCaching extends OutputStreamIndexOutput {
    private static final Logger LOGGER = LogManager.getLogger(BufferIOWithCaching.class);

    private static final int CHUNK_SIZE = CACHE_BLOCK_SIZE;
    private static final int BUFFER_SIZE = 65_536;

    private static final int BLOCK = 1 << CACHE_BLOCK_SIZE_POWER;
    private static final int BLOCK_MASK = BLOCK - 1;

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
        Pool<RefCountedMemorySegment> memorySegmentPool,
        BlockCache<RefCountedMemorySegment> blockCache
    )
        throws IOException {
        super(
            "FSIndexOutput(path=\"" + path + "\")",
            name,
            new EncryptedOutputStream(os, path, key, iv, memorySegmentPool, blockCache),
            CHUNK_SIZE
        );
    }

    private static class EncryptedOutputStream extends FilterOutputStream {

        private final byte[] key;
        private final byte[] iv;
        private final byte[] buffer;
        private final Path path;
        private final Pool<RefCountedMemorySegment> memorySegmentPool;
        private final BlockCache<RefCountedMemorySegment> blockCache;

        private int bufferPosition = 0;
        private long streamOffset = 0;
        private boolean isClosed = false;

        EncryptedOutputStream(
            OutputStream os,
            Path path,
            byte[] key,
            byte[] iv,
            Pool<RefCountedMemorySegment> memorySegmentPool,
            BlockCache<RefCountedMemorySegment> blockCache
        ) {
            super(os);
            this.path = path;
            this.key = key;
            this.iv = iv;
            this.buffer = new byte[BUFFER_SIZE];
            this.memorySegmentPool = memorySegmentPool;
            this.blockCache = blockCache;
        }

        @Override
        public void write(byte[] b, int offset, int length) throws IOException {
            checkClosed();
            if (b == null)
                throw new NullPointerException("Input buffer cannot be null");
            if (offset < 0 || length < 0 || offset + length > b.length) {
                throw new IndexOutOfBoundsException("Invalid offset or length");
            }
            if (length == 0)
                return;

            if (length >= BUFFER_SIZE) {
                // leave large-write path as-is for now
                flushBuffer(); // will now be block-aligned
                processAndWrite(path, b, offset, length);
                return;
            }

            // CHUNKED WRITES: if this would overflow the buffer, top-off to a block boundary, then flush
            if (bufferPosition + length > BUFFER_SIZE) {
                int partial = bufferPosition & BLOCK_MASK;
                if (partial != 0) {
                    int need = BLOCK - partial;
                    int take = Math.min(need, length);
                    System.arraycopy(b, offset, buffer, bufferPosition, take);
                    bufferPosition += take;
                    offset += take;
                    length -= take;
                }
                // buffer now ends at a block boundary (or was already aligned)
                flushBuffer(); // flushes only whole 8KB blocks; holding partial blocks.
            }

            // normal copy
            System.arraycopy(b, offset, buffer, bufferPosition, length);
            bufferPosition += length;
        }

        @Override
        public void write(int b) throws IOException {
            checkClosed();
            if (bufferPosition >= BUFFER_SIZE) {
                flushBuffer();
            }
            buffer[bufferPosition++] = (byte) b;
        }

        /** Flush only whole CHUNK_SIZE blocks; keep any <CHUNK_SIZE tail in the buffer (no mid-file partials). */
        private void flushBuffer() throws IOException {
            if (bufferPosition == 0)
                return;

            final int flushable = (int) (bufferPosition & ~CACHE_BLOCK_MASK); // largest multiple of 8192
            if (flushable == 0)
                return; // keep tail (<CHUNK_SIZE) until we can complete it (or EOF)

            processAndWrite(path, buffer, 0, flushable);

            // slide tail to start
            final int tail = bufferPosition - flushable;
            if (tail > 0) {
                System.arraycopy(buffer, flushable, buffer, 0, tail);
            }
            bufferPosition = tail;
        }

        /** Force flush ALL buffered data including any tail < CHUNK_SIZE */
        private void forceFlushBuffer() throws IOException {
            if (bufferPosition > 0) {
                processAndWrite(path, buffer, 0, bufferPosition);
                bufferPosition = 0;
            }
        }

        private void processAndWrite(Path path, byte[] data, int arrayOffset, int length) throws IOException {
            int offsetInBuffer = 0;
            final MemorySegment full = MemorySegment.ofArray(data);

            while (offsetInBuffer < length) {
                long absoluteOffset = streamOffset + offsetInBuffer;
                long blockAlignedOffset = absoluteOffset & ~CACHE_BLOCK_MASK;
                int blockOffset = (int) (absoluteOffset & CACHE_BLOCK_MASK);
                int chunkLen = Math.min(length - offsetInBuffer, CACHE_BLOCK_SIZE - blockOffset);

                // Cache plaintext data for reads
                cacheBlockIfEligible(path, full, arrayOffset + offsetInBuffer, blockAlignedOffset, blockOffset, chunkLen);

                // Encrypt and write to disk
                writeEncryptedChunk(data, arrayOffset + offsetInBuffer, chunkLen, absoluteOffset);
                offsetInBuffer += chunkLen;
            }

            streamOffset += length;
        }

        private void cacheBlockIfEligible(
            Path path,
            MemorySegment sourceData,
            int sourceOffset,
            long blockAlignedOffset,
            int blockOffset,
            int chunkLen
        ) {
            // Cache only fully-aligned full blocks
            if (blockOffset == 0 && chunkLen == CACHE_BLOCK_SIZE) {
                try {
                    final RefCountedMemorySegment refSegment = memorySegmentPool.tryAcquire(5, TimeUnit.MILLISECONDS);
                    if (refSegment != null) {
                        final MemorySegment pooled = refSegment.segment();
                        final MemorySegment pooledSlice = pooled.asSlice(0, CACHE_BLOCK_SIZE);
                        // Cache plaintext data
                        MemorySegment.copy(sourceData, sourceOffset, pooledSlice, 0, CACHE_BLOCK_SIZE);

                        BlockCacheKey cacheKey = new FileBlockCacheKey(path, blockAlignedOffset);
                        blockCache.put(cacheKey, refSegment);
                    } else {
                        LOGGER.info("Failed to acquire from pool within specificed timeout path={} {} ms", path, 5);
                    }

                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    LOGGER.warn("Interrupted while acquiring segment for cache.");
                } catch (IllegalStateException e) {
                    LOGGER.debug("Failed to acquire segment from pool; skipping cache.");
                }
            }
        }

        private void writeEncryptedChunk(byte[] data, int offset, int length, long absoluteOffset) throws IOException {
            try {
                // Encrypt data for disk write using OpenSSL native cipher
                byte[] chunkToEncrypt = slice(data, offset, length);
                byte[] encrypted = OpenSslNativeCipher.encrypt(key, iv, chunkToEncrypt, absoluteOffset);
                out.write(encrypted);
            } catch (Throwable t) {
                throw new IOException("Encryption failed at offset " + absoluteOffset, t);
            }
        }

        private byte[] slice(byte[] data, int offset, int length) {
            if (offset == 0 && length == data.length) {
                return data;
            }
            byte[] sliced = new byte[length];
            System.arraycopy(data, offset, sliced, 0, length);
            return sliced;
        }

        @Override
        @SuppressWarnings("ConvertToTryWithResources")
        public void close() throws IOException {
            IOException exception = null;

            try {
                checkClosed();
                forceFlushBuffer(); // Force flush ALL data including tail
                // Lucene writes footer here.
                // this will also flush the buffer.
                super.close();

                // After file is complete, load final block (footer) into cache for immediate reads
                loadFinalBlocksIntoCache();

                // signal the kernel to flush the file cacehe
                // we don't call flush aggresevley to avoid cpu pressure.
                if (streamOffset > 32L * 1024 * 1024) {
                    String absolutePath = path.toAbsolutePath().toString();
                    // Drop cache BEFORE deletion while file handle is still valid
                    Thread.startVirtualThread(() -> PanamaNativeAccess.dropFileCache(absolutePath));
                }

            } catch (IOException e) {
                exception = e;
            } finally {
                isClosed = true;
            }

            if (exception != null)
                throw exception;
        }

        private void loadFinalBlocksIntoCache() {
            try {
                if (streamOffset <= 0)
                    return;

                long finalBlockOffset = (streamOffset - 1) & ~CACHE_BLOCK_MASK;
                BlockCacheKey blockKey = new FileBlockCacheKey(path, finalBlockOffset);
                blockCache.getOrLoad(blockKey);

            } catch (IOException e) {
                LOGGER.debug("Failed to load final block into cache for path={}: {}", path, e.toString());
            }
        }

        private void checkClosed() throws IOException {
            if (isClosed) {
                throw new IOException("Outout stream is already closed, this is unusual");
            }
        }
    }
}

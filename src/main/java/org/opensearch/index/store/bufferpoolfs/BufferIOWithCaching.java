/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_MASK;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.security.Key;
import java.security.Provider;
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
import org.opensearch.index.store.cipher.AesCipherFactory;
import org.opensearch.index.store.cipher.EncryptionAlgorithm;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.cipher.OpenSslNativeCipher;
import org.opensearch.index.store.footer.EncryptionFooter;
import org.opensearch.index.store.footer.EncryptionMetadataTrailer;
import org.opensearch.index.store.key.HkdfKeyDerivation;
import org.opensearch.index.store.pool.Pool;

/**
 * An IndexOutput implementation that encrypts data before writing using native
 * OpenSSL AES-GCM.
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
     * @param memorySegmentPool the pool for acquiring memory segments for caching
     * @param blockCache the cache for storing decrypted block data
     * @param provider the security provider
     * @param encryptionMetadataCache the encryption metadata cache
     * @throws IOException If there is an I/O error
     * @throws IllegalArgumentException If key length is invalid
     */
    public BufferIOWithCaching(
        String name,
        Path path,
        OutputStream os,
        byte[] key,
        Pool<RefCountedMemorySegment> memorySegmentPool,
        BlockCache<RefCountedMemorySegment> blockCache,
        Provider provider,
        EncryptionMetadataCache encryptionMetadataCache
    )
        throws IOException {
        super(
            "FSIndexOutput(path=\"" + path + "\")",
            name,
            new EncryptedOutputStream(os, path, key, memorySegmentPool, blockCache, provider, encryptionMetadataCache),
            CHUNK_SIZE
        );
    }

    private static class EncryptedOutputStream extends FilterOutputStream {

        private final EncryptionFooter footer;
        private final byte[] masterKey;
        private final Key fileKey;
        private final byte[] buffer;
        private final Path path;
        private final String normalizedPath;
        private final Pool<RefCountedMemorySegment> memorySegmentPool;
        private final BlockCache<RefCountedMemorySegment> blockCache;
        private final long frameSize;
        private final long frameSizeMask;

        private final EncryptionAlgorithm algorithm;
        private final Provider provider;
        private final EncryptionMetadataCache encryptionMetadataCache;

        // Frame tracking
        private MemorySegment currentCipher;
        private int currentFrameNumber = 0;
        private long currentFrameOffset = 0;
        private int bufferPosition = 0;
        private long streamOffset = 0;
        private int totalFrames = 0;
        private boolean isClosed = false;

        // Partial block tracking for final block caching
        private long lastCachedBlockOffset = -1;
        private byte[] partialBlockBuffer = new byte[CACHE_BLOCK_SIZE];
        private int partialBlockLength = 0;

        EncryptedOutputStream(
            OutputStream os,
            Path path,
            byte[] key,
            Pool<RefCountedMemorySegment> memorySegmentPool,
            BlockCache<RefCountedMemorySegment> blockCache,
            Provider provider,
            EncryptionMetadataCache encryptionMetadataCache
        ) {
            super(os);
            this.path = path;
            this.normalizedPath = EncryptionMetadataCache.normalizePath(path);
            this.masterKey = key;
            this.buffer = new byte[BUFFER_SIZE];
            this.memorySegmentPool = memorySegmentPool;
            this.blockCache = blockCache;
            this.provider = provider;
            this.encryptionMetadataCache = encryptionMetadataCache;

            this.frameSize = EncryptionMetadataTrailer.DEFAULT_FRAME_SIZE;
            this.frameSizeMask = frameSize - 1;

            this.algorithm = EncryptionAlgorithm.fromId((short) EncryptionMetadataTrailer.ALGORITHM_AES_256_GCM);

            this.footer = EncryptionFooter.generateNew(frameSize, (short) EncryptionMetadataTrailer.ALGORITHM_AES_256_GCM);

            // Derive file-specific key
            byte[] derivedKey = HkdfKeyDerivation.deriveFileKey(masterKey, footer.getMessageId());
            this.fileKey = new javax.crypto.spec.SecretKeySpec(derivedKey, "AES");

            // Initialize first frame cipher
            initializeFrameCipher(0, 0);
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

            // LARGE WRITES: bypass buffering for writes >= BUFFER_SIZE
            // Flush all buffered data first (including partial tail) to maintain stream ordering
            if (length >= BUFFER_SIZE) {
                // Force flush includes any partial tail < 8KB to ensure correct stream ordering
                // before the large write is processed directly
                if (bufferPosition > 0) {
                    forceFlushBuffer();
                }
                processAndWrite(b, offset, length);
                return;
            }

            // CHUNKED WRITES: if this would overflow the buffer, top-off to a block boundary, then flush
            // This ensures we only flush complete 8KB cache blocks, keeping partial tails buffered
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

            processAndWrite(buffer, 0, flushable);

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
                processAndWrite(buffer, 0, bufferPosition);
                bufferPosition = 0;
            }
        }

        private void processAndWrite(byte[] data, int arrayOffset, int length) throws IOException {
            int offsetInBuffer = 0;
            final MemorySegment full = MemorySegment.ofArray(data);

            while (offsetInBuffer < length) {
                long absoluteOffset = streamOffset + offsetInBuffer;
                long blockAlignedOffset = absoluteOffset & ~CACHE_BLOCK_MASK;
                int blockOffset = (int) (absoluteOffset & CACHE_BLOCK_MASK);
                int chunkLen = Math.min(length - offsetInBuffer, CACHE_BLOCK_SIZE - blockOffset);

                // Cache plaintext data for reads
                cacheBlockIfEligible(full, arrayOffset + offsetInBuffer, blockAlignedOffset, blockOffset, chunkLen);

                // Encrypt and write to disk
                writeEncryptedChunk(data, arrayOffset + offsetInBuffer, chunkLen, absoluteOffset);
                offsetInBuffer += chunkLen;
            }

            streamOffset += length;
        }

        private void cacheBlockIfEligible(
            MemorySegment sourceData,
            int sourceOffset,
            long blockAlignedOffset,
            int blockOffset,
            int chunkLen
        ) {
            // Cache fully-aligned full blocks immediately
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

                        // Track this as last cached block
                        lastCachedBlockOffset = blockAlignedOffset;
                        partialBlockLength = 0; // Reset partial tracking
                    } else {
                        LOGGER.debug("Failed to acquire from pool within specified timeout path={} {} ms", path, 5);
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    LOGGER.warn("Interrupted while acquiring segment for cache.");
                } catch (IllegalStateException e) {
                    LOGGER.debug("Failed to acquire segment from pool; skipping cache.");
                }
            } else {
                // Partial block - accumulate for potential final block caching
                // Check if this is a new block or continuation of current partial block
                if (blockAlignedOffset != lastCachedBlockOffset || partialBlockLength == 0) {
                    // Starting a new partial block
                    lastCachedBlockOffset = blockAlignedOffset;
                    partialBlockLength = 0;
                }

                // Accumulate into partial buffer
                MemorySegment.copy(sourceData, sourceOffset, MemorySegment.ofArray(partialBlockBuffer), blockOffset, chunkLen);
                partialBlockLength = Math.max(partialBlockLength, blockOffset + chunkLen);
            }
        }

        private void writeEncryptedChunk(byte[] data, int offset, int length, long absoluteOffset) throws IOException {
            int remaining = length;
            int dataOffset = offset;
            long currentOffset = absoluteOffset;

            while (remaining > 0) {
                int frameNumber = (int) (currentOffset >>> EncryptionMetadataTrailer.DEFAULT_FRAME_SIZE_POWER);
                long frameEnd = (long) (frameNumber + 1) << EncryptionMetadataTrailer.DEFAULT_FRAME_SIZE_POWER;

                if (frameNumber != currentFrameNumber) {
                    finalizeCurrentFrame();
                    initializeFrameCipher(frameNumber, currentOffset % frameSize);
                }

                int chunkSize = (int) Math.min(remaining, frameEnd - currentOffset);

                try {
                    // Use OpenSSL native cipher for encryption
                    byte[] encrypted = OpenSslNativeCipher.encryptUpdate(currentCipher, slice(data, dataOffset, chunkSize));
                    out.write(encrypted);

                    currentOffset += chunkSize;
                    currentFrameOffset += chunkSize;
                    remaining -= chunkSize;
                    dataOffset += chunkSize;
                } catch (Throwable t) {
                    throw new IOException("Encryption failed at offset " + currentOffset, t);
                }
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

                finalizeCurrentFrame();
                footer.setFrameCount(totalFrames);

                // Serialize footer with file key for authentication
                byte[] fileKeyBytes = fileKey.getEncoded();
                out.write(footer.serialize(null, fileKeyBytes));

                // Cache metadata for future reads
                encryptionMetadataCache.getOrLoadMetadata(normalizedPath, footer, this.masterKey);

                // close() only flushes to the OS (kernel page cache). It does NOT guarantee
                // * durability on disk (no fsync here). Lucene will provide the durability boundary by calling
                // * Directory.sync(files), which issues fsync/force(true) on the files that are part of the
                // * commit. After sync() returns, the data is crash-safe.
                super.close();

                // Cache the final partial block if present (avoids disk I/O for immediate reads)
                cacheFinalPartialBlock();

                // signal the kernel to flush the file cacehe
                // but we don't call flush aggresevley in small files
                // since most of the page cache would be still dirty as lucene might
                // have not yet fsycned them yet via Directory.sync and DONTNEED is ineffective.
                if (streamOffset > 32L * 1024 * 1024) {
                    String absolutePath = path.toAbsolutePath().toString();
                    // **** This doesn't effect durability ***
                    // - DONTNEED is a cache "hint" only. It never discards DIRTY pages. For dirty pages,
                    // the kernel first schedules/writebacks the data and drops the pages only after
                    // the write completes. Clean pages may be dropped immediately.
                    // - Therefore, using DONTNEED does not jeopardize durability: a later fsync
                    // (via Lucene's Directory.sync(files)) will still ensure persistence. If any
                    // writeback is outstanding, fsync/force(true) will wait for it to finish before
                    // returning, preserving the durability contract.
                    Thread.startVirtualThread(() -> PanamaNativeAccess.dropFileCache(absolutePath));
                }

            } catch (IOException e) {
                exception = e;
            } finally {
                isClosed = true;
                // Clean up any remaining native resources
                if (currentCipher != null) {
                    currentCipher = null;
                }
            }

            if (exception != null)
                throw exception;
        }

        private void cacheFinalPartialBlock() {
            if (partialBlockLength == 0 || lastCachedBlockOffset < 0 || streamOffset <= 0) {
                return;
            }

            // Verify this is truly the final block
            long expectedFinalBlockOffset = (streamOffset - 1) & ~CACHE_BLOCK_MASK;
            if (lastCachedBlockOffset != expectedFinalBlockOffset) {
                LOGGER
                    .warn(
                        "Partial block offset mismatch: tracked={}, expected={}, streamOffset={}",
                        lastCachedBlockOffset,
                        expectedFinalBlockOffset,
                        streamOffset
                    );
                return;
            }

            // Verify partial length matches file tail size
            int expectedTailSize = (int) (streamOffset - lastCachedBlockOffset);
            if (partialBlockLength != expectedTailSize) {
                LOGGER
                    .warn(
                        "Partial block size mismatch: tracked={}, expected={}, streamOffset={}",
                        partialBlockLength,
                        expectedTailSize,
                        streamOffset
                    );
                return;
            }

            // Safe to cache - this is definitely the file's final partial block
            try {
                RefCountedMemorySegment refSegment = memorySegmentPool.tryAcquire(5, TimeUnit.MILLISECONDS);
                if (refSegment != null) {
                    MemorySegment pooled = refSegment.segment().asSlice(0, CACHE_BLOCK_SIZE);
                    MemorySegment.copy(MemorySegment.ofArray(partialBlockBuffer), 0, pooled, 0, partialBlockLength);

                    BlockCacheKey cacheKey = new FileBlockCacheKey(path, lastCachedBlockOffset);
                    blockCache.put(cacheKey, refSegment);
                } else {
                    LOGGER.debug("Failed to acquire segment for final partial block caching path={}", path);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOGGER.warn("Interrupted while caching final partial block for path={}", path);
            } catch (IllegalStateException e) {
                LOGGER.debug("Failed to acquire segment from pool for final partial block; skipping cache.");
            }
        }

        private void checkClosed() throws IOException {
            if (isClosed) {
                throw new IOException("Output stream is already closed, this is unusual");
            }
        }

        private void initializeFrameCipher(int frameNumber, long offsetWithinFrame) {
            this.currentFrameNumber = frameNumber;
            this.currentFrameOffset = offsetWithinFrame;

            try {
                // Compute frame-specific IV
                byte[] frameIV = AesCipherFactory
                    .computeFrameIV(
                        masterKey,
                        footer.getMessageId(),
                        frameNumber,
                        offsetWithinFrame,
                        normalizedPath,
                        encryptionMetadataCache
                    );

                // Initialize new OpenSSL cipher context
                currentCipher = OpenSslNativeCipher.initGCMCipher(fileKey.getEncoded(), frameIV, offsetWithinFrame);

            } catch (Throwable t) {
                throw new RuntimeException("Failed to initialize frame cipher", t);
            }
        }

        private void finalizeCurrentFrame() throws IOException {
            if (currentCipher == null)
                return;

            try {
                // Finalize cipher and get authentication tag
                byte[] tag = OpenSslNativeCipher.finalizeAndGetTag(currentCipher);

                // Store tag in footer
                footer.addGcmTag(tag);

                // Increment total frames since we just finalized one
                totalFrames++;

                // Clear the context reference (already freed by finalizeAndGetTag)
                currentCipher = null;
            } catch (Throwable t) {
                throw new IOException("Failed to finalize frame " + currentFrameNumber, t);
            }
        }
    }
}

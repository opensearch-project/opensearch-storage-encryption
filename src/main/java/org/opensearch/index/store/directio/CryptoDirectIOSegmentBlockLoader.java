/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIoUtils.DIRECT_IO_ALIGNMENT;
import static org.opensearch.index.store.directio.DirectIoUtils.getDirectOpenOption;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.BlockLoader;
import org.opensearch.index.store.block_cache.MemorySegmentCacheValue;
import org.opensearch.index.store.block_cache.Pool;
import org.opensearch.index.store.cipher.OpenSslNativeCipher;
import org.opensearch.index.store.iv.KeyIvResolver;

@SuppressWarnings("preview")
public class CryptoDirectIOSegmentBlockLoader implements BlockLoader<MemorySegment> {
    private final Pool<MemorySegment> segmentPool;
    private final KeyIvResolver keyIvResolver;

    public CryptoDirectIOSegmentBlockLoader(Pool<MemorySegment> segmentPool, KeyIvResolver keyIvResolver) {
        this.segmentPool = segmentPool;
        this.keyIvResolver = keyIvResolver;
    }

    @Override
    public Optional<BlockCacheValue<MemorySegment>> load(BlockCacheKey key, int size) throws Exception {
        long offset = key.offset();

        // Try to acquire from pool with a small timeout
        // todo, make it configurable.
        MemorySegment target = segmentPool.tryAcquire(5, TimeUnit.MILLISECONDS);
        if (target == null) {
            return Optional.empty(); // Pool exhausted, skip caching
        }

        byte[] keyBytes = keyIvResolver.getDataKey().getEncoded();
        byte[] iv = keyIvResolver.getIvBytes();

        try (FileChannel channel = FileChannel.open(key.filePath(), StandardOpenOption.READ, getDirectOpenOption())) {
            loadAndDecrypt(channel, offset, target, keyBytes, iv);
            return Optional.of(new MemorySegmentCacheValue(target, size, segmentPool));
        } catch (IOException | RuntimeException e) {
            segmentPool.release(target);
            throw e;
        }
    }

    /**
    * Reads data using Direct I/O with proper alignment.
    * <p>
    * Direct I/O requires alignment to storage device sector boundaries.
    * </p>
    *
    * <p><b>File Layout:</b></p>
    * <pre>
    * ┌─────┬─────┬─────┬─────┬─────┬─────┐
    * │  0  │ 512 │1024 │1536 │2048 │2560 │ ← Sector boundaries
    * └─────┴─────┴─────┴─────┴─────┴─────┘
    * </pre>
    *
    * <p><b>Incorrect: Reading from offset 1000</b></p>
    * <pre>
    *                     ↓ start here
    * ┌─────┬─────┬─────┬─────┬─────┬─────┐
    * │  0  │ 512 │1024 │1536 │2048 │2560 │
    *                 ███│█████
    * </pre>
    *
    * <p><b>Correct: Reading from offset 1024</b></p>
    * <pre>
    *                      ↓ start here  
    * ┌─────┬─────┬─────┬─────┬─────┬─────┐
    * │  0  │ 512 │1024 │1536 │2048 │2560 │
    *                     │█████│█████│
    * </pre>
    *
    */
    public static void loadAndDecrypt(FileChannel channel, long offset, MemorySegment target, byte[] key, byte[] iv) throws IOException {
        int alignment = DIRECT_IO_ALIGNMENT;

        long alignedOffset = offset & ~(alignment - 1);
        long offsetDelta = offset - alignedOffset;
        long adjustedLength = offsetDelta + target.byteSize();
        long alignedLength = (adjustedLength + alignment - 1) & ~(alignment - 1);

        if (alignedLength > Integer.MAX_VALUE) {
            throw new IOException("Aligned read size too large: " + alignedLength);
        }

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment temp = arena.allocate(alignedLength, alignment);
            ByteBuffer buffer = temp.asByteBuffer();

            int bytesRead = channel.read(buffer, alignedOffset);
            if (bytesRead < offsetDelta + target.byteSize()) {
                throw new IOException("Incomplete read: expected=" + (offsetDelta + target.byteSize()) + ", actual=" + bytesRead);
            }

            try {
                OpenSslNativeCipher.decryptInPlace(arena, temp.address(), temp.byteSize(), key, iv, offset);
            } catch (Throwable e) {
                throw new RuntimeException("Decryption failed at offset: " + offset, e);

            }

            MemorySegment.copy(temp, offsetDelta, target, 0, target.byteSize());
        }
    }

}

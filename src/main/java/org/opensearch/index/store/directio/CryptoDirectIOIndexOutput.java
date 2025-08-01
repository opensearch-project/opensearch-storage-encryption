/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.opensearch.index.store.directio.DirectIOReader.getDirectOpenOption;
import static org.opensearch.index.store.directio.DirectIoConfigs.DIRECT_IO_ALIGNMENT;
import static org.opensearch.index.store.directio.DirectIoConfigs.INDEX_OUTPUT_BUFFER_SIZE_POWER;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.Pool;
import org.opensearch.index.store.block_cache.RefCountedMemorySegment;
import org.opensearch.index.store.iv.KeyIvResolver;

@SuppressWarnings("preview")
@SuppressForbidden(reason = "uses custom DirectIO")
public class CryptoDirectIOIndexOutput extends IndexOutput {
    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectIOIndexOutput.class);

    private static final int BUFFER_SIZE = (1 << INDEX_OUTPUT_BUFFER_SIZE_POWER);

    private final Pool<MemorySegment> memorySegmentPool;
    private final BlockCache<RefCountedMemorySegment> blockCache;
    private final FileChannel directIOchannel;
    private final FileChannel bufferIOchannel;
    private final KeyIvResolver keyIvResolver;
    private final ByteBuffer buffer;
    private final Checksum digest;
    private final Path path;
    private final boolean shouldAddToBufferPool;

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
        BlockCache<RefCountedMemorySegment> blockCache,
        boolean shouldAddToBufferPool
    )
        throws IOException {
        super("DirectIOIndexOutput(path=\"" + path + "\")", name);
        this.keyIvResolver = keyIvResolver;
        this.memorySegmentPool = memorySegmentPool;
        this.blockCache = blockCache;
        this.path = path;
        this.buffer = ByteBuffer.allocateDirect(BUFFER_SIZE + DIRECT_IO_ALIGNMENT - 1).alignedSlice(DIRECT_IO_ALIGNMENT);
        this.directIOchannel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW, getDirectOpenOption());
        this.bufferIOchannel = FileChannel.open(path, StandardOpenOption.WRITE);
        this.digest = new BufferedChecksum(new CRC32());
        this.isOpen = true;
        this.shouldAddToBufferPool = shouldAddToBufferPool;
    }

    @Override
    public void writeByte(byte b) throws IOException {
        buffer.put(b);
        digest.update(b);
        if (!buffer.hasRemaining()) {
            dump();
        }
    }

    @Override
    public void writeBytes(byte[] src, int offset, int len) throws IOException {
        int toWrite = len;
        while (true) {
            final int left = buffer.remaining();
            if (left <= toWrite) {
                buffer.put(src, offset, left);
                digest.update(src, offset, left);
                toWrite -= left;
                offset += left;
                dump();
            } else {
                buffer.put(src, offset, toWrite);
                digest.update(src, offset, toWrite);
                break;
            }
        }
    }

    private void dump() throws IOException {
        final int size = buffer.position();
        // we need to rewind, as we have to write full blocks (we truncate file later):
        buffer.rewind();

        directIOchannel.write(buffer, filePos);
        filePos += size;

        buffer.clear();
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
                dump();
            } finally {
                try (FileChannel ch = directIOchannel) {
                    ch.truncate(getFilePointer());
                }
            }
        }
    }
}

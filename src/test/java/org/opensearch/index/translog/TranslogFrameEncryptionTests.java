/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.translog;

import static org.opensearch.index.translog.TranslogChunkManager.FORMAT_HEADER_SIZE;
import static org.opensearch.index.translog.TranslogChunkManager.FRAME_HEADER_SIZE;
import static org.opensearch.index.translog.TranslogChunkManager.FRAME_OVERHEAD;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.Key;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.tests.mockfile.FilterFileChannel;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Behaviour of the sealed AES-GCM frame translog format through {@link CryptoFileChannelWrapper}, the
 * channel TranslogWriter and TranslogReader use for .tlog files.
 */
public class TranslogFrameEncryptionTests extends OpenSearchTestCase {

    private static final String TRANSLOG_UUID = "a9QxbK3pTzuWq0-Jf9lZ2w";
    private static final Set<OpenOption> WRITE = Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    private static final Set<OpenOption> READ = Set.of(StandardOpenOption.READ);
    private static final String MARKER = "secret-marker-";

    private KeyResolver keyResolver;
    private Path dir;
    private Path path;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        byte[] keyBytes = new byte[32];
        random().nextBytes(keyBytes);
        Key key = new SecretKeySpec(keyBytes, "AES");
        keyResolver = () -> key;
        dir = createTempDir();
        path = dir.resolve("translog-1.tlog");
    }

    /** A translog written op by op; offsets[i] is the logical offset where op i starts. */
    private static final class Written {
        final int headerSize;
        final List<byte[]> ops = new ArrayList<>();
        final List<Long> offsets = new ArrayList<>();
        long end;

        Written(int headerSize) {
            this.headerSize = headerSize;
            this.end = headerSize;
        }

        byte[] plaintext() {
            byte[] plain = new byte[(int) (end - headerSize)];
            for (int i = 0; i < ops.size(); i++) {
                System.arraycopy(ops.get(i), 0, plain, (int) (offsets.get(i) - headerSize), ops.get(i).length);
            }
            return plain;
        }
    }

    private CryptoFileChannelWrapper open(Path file, Set<OpenOption> options) throws IOException {
        return (CryptoFileChannelWrapper) new CryptoChannelFactory(keyResolver, TRANSLOG_UUID)
            .open(file, options.toArray(new OpenOption[0]));
    }

    private static int writeHeader(FileChannel channel) throws IOException {
        TranslogHeader header = new TranslogHeader(TRANSLOG_UUID, 1L);
        header.write(channel, false);
        return header.sizeInBytes();
    }

    /** Writes {@code count} ops of random length, one write (so one frame) per op. */
    private Written writeOps(CryptoFileChannelWrapper channel, int count) throws IOException {
        Written w = new Written(writeHeader(channel));
        for (int i = 0; i < count; i++) {
            byte[] op = (MARKER + i + "-" + "x".repeat(randomIntBetween(1, 700))).getBytes(StandardCharsets.UTF_8);
            append(channel, w, op);
        }
        return w;
    }

    /** Writes one op of each size in {@code sizes}, one write per op. */
    private Written writeSizes(CryptoFileChannelWrapper channel, int... sizes) throws IOException {
        Written w = new Written(writeHeader(channel));
        for (int size : sizes) {
            append(channel, w, randomBytes(size));
        }
        return w;
    }

    private static void append(FileChannel channel, Written w, byte[] op) throws IOException {
        w.offsets.add(w.end);
        w.ops.add(op);
        w.end += writeFully(channel, op, w.end);
    }

    private static int writeFully(FileChannel channel, byte[] data, long position) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(data);
        int total = 0;
        while (buf.hasRemaining()) {
            total += channel.write(buf, position + total);
        }
        return total;
    }

    private static byte[] readFully(FileChannel channel, long position, int length) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(length);
        int total = 0;
        while (buf.hasRemaining()) {
            int n = channel.read(buf, position + total);
            if (n <= 0) {
                throw new IOException("short read at " + (position + total) + " (" + n + ")");
            }
            total += n;
        }
        return buf.array();
    }

    private static byte[] randomBytes(int length) {
        byte[] data = new byte[length];
        random().nextBytes(data);
        return data;
    }

    /** Flips bits of the byte at {@code physicalOffset} in the file on disk. */
    private void corrupt(long physicalOffset, int xor) throws IOException {
        try (FileChannel raw = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer b = ByteBuffer.allocate(1);
            raw.read(b, physicalOffset);
            b.put(0, (byte) (b.get(0) ^ xor));
            b.rewind();
            raw.write(b, physicalOffset);
        }
    }

    public void testReadOfJustWrittenOpWhileWriterOpen() throws IOException {
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            Written w = writeOps(writer, 1);
            assertArrayEquals(w.ops.get(0), readFully(writer, w.offsets.get(0), w.ops.get(0).length));
        }
    }

    public void testRandomReadsWhileWriterOpen() throws IOException {
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            Written w = writeOps(writer, 80);
            int last = w.ops.size() - 1;
            assertArrayEquals(w.ops.get(last), readFully(writer, w.offsets.get(last), w.ops.get(last).length));
            for (int k = 0; k < 50; k++) {
                int i = randomIntBetween(0, last);
                assertArrayEquals("op " + i, w.ops.get(i), readFully(writer, w.offsets.get(i), w.ops.get(i).length));
            }
        }
    }

    public void testUnalignedRangeReadsAcrossFrames() throws IOException {
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            Written w = writeOps(writer, 40);
            byte[] plain = w.plaintext();
            for (int k = 0; k < 100; k++) {
                int from = randomIntBetween(0, plain.length - 1);
                int len = randomIntBetween(1, plain.length - from);
                assertArrayEquals(
                    "range " + from + "+" + len,
                    Arrays.copyOfRange(plain, from, from + len),
                    readFully(writer, w.headerSize + from, len)
                );
            }
        }
    }

    public void testReplayAfterWriterNeverClosed() throws IOException {
        CryptoFileChannelWrapper writer = open(path, WRITE);
        Written w = writeOps(writer, 60);
        writer.force(false);
        // Simulate a crash: the writer is abandoned without close(), then a new channel replays the file.
        try (CryptoFileChannelWrapper reader = open(path, READ)) {
            assertEquals(w.end, reader.size());
            for (int i = 0; i < w.ops.size(); i++) {
                assertArrayEquals("op " + i, w.ops.get(i), readFully(reader, w.offsets.get(i), w.ops.get(i).length));
            }
        } finally {
            writer.close();
        }
    }

    public void testVariableSizeFramesMapLogicalOffsets() throws IOException {
        Written w;
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            // A 6000-byte flush, then a full 8192-byte frame, then a 3000-byte flush.
            w = writeSizes(writer, 6000, 8192, 3000);
            assertEquals(w.headerSize + 6000 + 8192 + 3000, writer.size());
        }
        byte[] plain = w.plaintext();
        try (CryptoFileChannelWrapper reader = open(path, READ)) {
            // Logical 7000 is in frame 1 (which starts at logical 6000), although 7000 / 8192 = 0.
            assertArrayEquals(Arrays.copyOfRange(plain, 7000, 7100), readFully(reader, w.headerSize + 7000, 100));
            // A read that spans all three frames.
            assertArrayEquals(Arrays.copyOfRange(plain, 5990, 14200), readFully(reader, w.headerSize + 5990, 14200 - 5990));
        }
        // On disk: header, format header, then each frame is length + ciphertext + tag.
        assertEquals(w.headerSize + FORMAT_HEADER_SIZE + 3 * FRAME_OVERHEAD + 6000 + 8192 + 3000, Files.size(path));
    }

    public void testLargeWriteIsSplitIntoFrames() throws IOException {
        Written w;
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            w = writeSizes(writer, 20000); // 8192 + 8192 + 3616
        }
        assertEquals(w.headerSize + FORMAT_HEADER_SIZE + 3 * FRAME_OVERHEAD + 20000, Files.size(path));
        try (CryptoFileChannelWrapper reader = open(path, READ)) {
            assertArrayEquals(w.ops.get(0), readFully(reader, w.headerSize, 20000));
        }
    }

    public void testFrameSizeIsReadFromTheFile() throws IOException {
        Written w;
        try (
            CryptoFileChannelWrapper writer = new CryptoFileChannelWrapper(
                FileChannel.open(path, WRITE),
                keyResolver,
                path,
                WRITE,
                TRANSLOG_UUID,
                1024
            )
        ) {
            w = writeSizes(writer, 3000); // 1024 + 1024 + 952
        }
        assertEquals(w.headerSize + FORMAT_HEADER_SIZE + 3 * FRAME_OVERHEAD + 3000, Files.size(path));
        // Reopened with the default 8KB frame size, the file is read with the 1KB size it was written with.
        try (CryptoFileChannelWrapper reader = open(path, READ)) {
            assertEquals(1024, reader.getChunkManager().maxFrameSize());
            assertEquals(w.end, reader.size());
            assertArrayEquals(w.ops.get(0), readFully(reader, w.headerSize, 3000));
        }
    }

    public void testTamperedCiphertextIsRejected() throws IOException {
        Written w;
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            w = writeSizes(writer, 1000, 1000);
        }
        long frame1 = w.headerSize + FORMAT_HEADER_SIZE + FRAME_OVERHEAD + 1000;
        corrupt(frame1 + FRAME_HEADER_SIZE + 10, 0x01);
        try (CryptoFileChannelWrapper reader = open(path, READ)) {
            assertArrayEquals(w.ops.get(0), readFully(reader, w.headerSize, 1000));
            IOException e = expectThrows(IOException.class, () -> readFully(reader, w.headerSize + 1000, 1));
            assertTrue(e.getMessage(), e.getMessage().contains("Failed to authenticate translog frame 1"));
        }
    }

    public void testTamperedTagIsRejected() throws IOException {
        Written w;
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            w = writeSizes(writer, 1000);
        }
        corrupt(Files.size(path) - 1, 0x80);
        try (CryptoFileChannelWrapper reader = open(path, READ)) {
            expectThrows(IOException.class, () -> readFully(reader, w.headerSize, 1000));
        }
    }

    public void testTamperedLengthIsRejected() throws IOException {
        Written w;
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            w = writeSizes(writer, 1000, 1000);
        }
        // Change frame 0's length from 1000 (0x03E8) to 1001: the length is authenticated, so frame 0 fails.
        corrupt(w.headerSize + FORMAT_HEADER_SIZE + 3, 0x01);
        try (CryptoFileChannelWrapper reader = open(path, READ)) {
            expectThrows(IOException.class, () -> readFully(reader, w.headerSize, 10));
        }
    }

    public void testSwappedFramesAreRejected() throws IOException {
        Written w;
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            w = writeSizes(writer, 1000, 1000);
        }
        byte[] raw = Files.readAllBytes(path);
        int frame0 = w.headerSize + FORMAT_HEADER_SIZE;
        int frameSize = FRAME_OVERHEAD + 1000;
        byte[] swapped = raw.clone();
        System.arraycopy(raw, frame0, swapped, frame0 + frameSize, frameSize);
        System.arraycopy(raw, frame0 + frameSize, swapped, frame0, frameSize);
        Files.write(path, swapped);
        try (CryptoFileChannelWrapper reader = open(path, READ)) {
            expectThrows(IOException.class, () -> readFully(reader, w.headerSize, 10));
            expectThrows(IOException.class, () -> readFully(reader, w.headerSize + 1000, 10));
        }
    }

    public void testTornTrailingFrameIsIgnored() throws IOException {
        Written w;
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            w = writeSizes(writer, 1000, 1000, 1000);
        }
        // A crash while frame 2 was being written: only part of it reached the disk.
        long frame2 = w.headerSize + FORMAT_HEADER_SIZE + 2L * (FRAME_OVERHEAD + 1000);
        try (FileChannel raw = FileChannel.open(path, StandardOpenOption.WRITE)) {
            raw.truncate(frame2 + randomIntBetween(1, FRAME_OVERHEAD + 999));
        }
        try (CryptoFileChannelWrapper reader = open(path, READ)) {
            assertEquals(w.headerSize + 2000, reader.size());
            assertArrayEquals(w.ops.get(0), readFully(reader, w.offsets.get(0), 1000));
            assertArrayEquals(w.ops.get(1), readFully(reader, w.offsets.get(1), 1000));
            assertEquals(-1, reader.read(ByteBuffer.allocate(10), w.headerSize + 2000));
        }
    }

    public void testAppendAfterTornFrameIsRejected() throws IOException {
        Written w;
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            w = writeSizes(writer, 1000, 1000, 1000);
        }
        long frame2 = w.headerSize + FORMAT_HEADER_SIZE + 2L * (FRAME_OVERHEAD + 1000);
        long tornSize = frame2 + randomIntBetween(1, FRAME_OVERHEAD + 999);
        try (FileChannel raw = FileChannel.open(path, StandardOpenOption.WRITE)) {
            raw.truncate(tornSize);
        }
        // Reopened for writing: a new frame would get index 2, the index (and nonce) of the torn frame.
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            assertEquals(w.headerSize + 2000, writer.size());
            IOException e = expectThrows(IOException.class, () -> writeFully(writer, randomBytes(100), w.headerSize + 2000));
            assertTrue(e.getMessage(), e.getMessage().contains("incomplete frame"));
            assertArrayEquals(w.ops.get(1), readFully(writer, w.offsets.get(1), 1000));
        }
        assertEquals("the torn file is not modified", tornSize, Files.size(path));
    }

    public void testAppendAfterTornFormatHeaderIsRejected() throws IOException {
        int headerSize;
        try (FileChannel plain = FileChannel.open(path, WRITE)) {
            headerSize = writeHeader(plain);
            plain.write(ByteBuffer.wrap(randomBytes(randomIntBetween(1, FORMAT_HEADER_SIZE - 1))), headerSize);
        }
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            assertEquals(headerSize, writer.size());
            expectThrows(IOException.class, () -> writeFully(writer, randomBytes(100), headerSize));
        }
    }

    public void testFailedFrameWriteBlocksLaterWrites() throws IOException {
        AtomicBoolean failNextWrite = new AtomicBoolean();
        FileChannel failing = new FilterFileChannel(FileChannel.open(path, WRITE)) {
            @Override
            public int write(ByteBuffer src, long position) throws IOException {
                if (failNextWrite.getAndSet(false)) {
                    // Half the frame reaches the disk, then the write fails (for example, the disk is full).
                    ByteBuffer half = src.duplicate();
                    half.limit(half.position() + half.remaining() / 2);
                    super.write(half, position);
                    throw new IOException("simulated write failure");
                }
                return super.write(src, position);
            }
        };
        try (CryptoFileChannelWrapper writer = new CryptoFileChannelWrapper(failing, keyResolver, path, WRITE, TRANSLOG_UUID)) {
            Written w = writeSizes(writer, 1000);
            failNextWrite.set(true);
            expectThrows(IOException.class, () -> writeFully(writer, randomBytes(1000), w.end));
            // Retrying would seal different bytes under the failed frame's index and nonce.
            IOException e = expectThrows(IOException.class, () -> writeFully(writer, randomBytes(1000), w.end));
            assertTrue(e.getMessage(), e.getMessage().contains("incomplete frame"));
            assertArrayEquals(w.ops.get(0), readFully(writer, w.offsets.get(0), 1000));
        }
    }

    public void testReaderSeesFramesAppendedByWriter() throws IOException {
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            Written w = writeSizes(writer, 500);
            try (CryptoFileChannelWrapper reader = open(path, READ)) {
                assertEquals(w.end, reader.size());
                append(writer, w, randomBytes(700));
                // size() sees the new frame before any read past the old end.
                assertEquals(w.end, reader.size());
                assertArrayEquals(w.ops.get(1), readFully(reader, w.offsets.get(1), 700));
            }
        }
    }

    public void testHeaderOnlyFile() throws IOException {
        int headerSize;
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            headerSize = writeHeader(writer);
            assertEquals(headerSize, writer.size());
        }
        assertEquals("no format header until the first frame", headerSize, Files.size(path));
        try (CryptoFileChannelWrapper reader = open(path, READ)) {
            assertEquals(headerSize, reader.size());
            assertEquals(-1, reader.read(ByteBuffer.allocate(10), headerSize));
            TranslogHeader.read(path, reader);
        }
    }

    public void testUnsupportedFormatIsRejected() throws IOException {
        try (FileChannel plain = FileChannel.open(path, WRITE)) {
            int headerSize = writeHeader(plain);
            // Bytes in the earlier chunk or CTR formats: no format header after the TranslogHeader.
            plain.write(ByteBuffer.wrap(randomBytes(5000)), headerSize);
        }
        IOException e = expectThrows(IOException.class, () -> open(path, READ));
        assertTrue(e.getMessage(), e.getMessage().contains("Unsupported translog encryption format"));
    }

    public void testNonAppendWriteIsRejected() throws IOException {
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            Written w = writeSizes(writer, 1000);
            expectThrows(IOException.class, () -> writer.write(ByteBuffer.wrap(randomBytes(10)), w.headerSize + 500));
            expectThrows(IOException.class, () -> writer.write(ByteBuffer.wrap(randomBytes(10)), w.end + 1));
        }
    }

    public void testTruncateIsRejected() throws IOException {
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            Written w = writeSizes(writer, 1000, 1000);
            writer.truncate(w.end); // no-op
            expectThrows(IOException.class, () -> writer.truncate(w.headerSize + 1000));
            assertEquals(w.end, writer.size());
        }
    }

    public void testFileHidesPlaintext() throws IOException {
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            writeOps(writer, 30);
        }
        byte[] raw = Files.readAllBytes(path);
        assertFalse(new String(raw, StandardCharsets.ISO_8859_1).contains(MARKER));
    }

    public void testNoKeystreamReuseWithinFile() throws IOException {
        int headerSize;
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            headerSize = writeHeader(writer);
            writeFully(writer, new byte[3 * 8192], headerSize);
        }
        // Encrypting zeros writes the keystream itself. With a unique nonce per frame, no 32-byte
        // window of ciphertext repeats anywhere in the file.
        byte[] raw = Files.readAllBytes(path);
        Set<ByteBuffer> windows = new HashSet<>();
        for (int i = headerSize; i + 32 <= raw.length; i++) {
            ByteBuffer window = ByteBuffer.wrap(Arrays.copyOfRange(raw, i, i + 32));
            assertTrue("ciphertext repeats: 32-byte window at file offset " + i + " appears earlier", windows.add(window));
        }
    }

    public void testNonceIsUniquePerTranslogFile() {
        byte[] key = keyResolver.getDataKey().getEncoded();
        byte[] gen1 = TranslogChunkManager.deriveFileNonce(key, TRANSLOG_UUID, "translog-1.tlog");
        assertEquals(8, gen1.length);
        assertArrayEquals(gen1, TranslogChunkManager.deriveFileNonce(key, TRANSLOG_UUID, "translog-1.tlog"));
        assertFalse(Arrays.equals(gen1, TranslogChunkManager.deriveFileNonce(key, TRANSLOG_UUID, "translog-2.tlog")));
        assertFalse(Arrays.equals(gen1, TranslogChunkManager.deriveFileNonce(key, "otherUuidAAAAAAAAAAAAA", "translog-1.tlog")));
    }

    public void testSameOffsetsInDifferentGenerationsEncryptDifferently() throws IOException {
        byte[][] raw = new byte[2][];
        int headerSize = 0;
        for (int gen = 1; gen <= 2; gen++) {
            Path file = dir.resolve("translog-" + gen + ".tlog");
            try (CryptoFileChannelWrapper writer = open(file, WRITE)) {
                headerSize = writeHeader(writer);
                writeFully(writer, new byte[4096], headerSize);
            }
            raw[gen - 1] = Files.readAllBytes(file);
        }
        int dataStart = headerSize + FORMAT_HEADER_SIZE + FRAME_HEADER_SIZE;
        assertFalse(
            Arrays.equals(Arrays.copyOfRange(raw[0], dataStart, raw[0].length), Arrays.copyOfRange(raw[1], dataStart, raw[1].length))
        );
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.translog;

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

import javax.crypto.spec.SecretKeySpec;

import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Behaviour of the AES-CTR translog format through {@link CryptoFileChannelWrapper}, the channel
 * TranslogWriter and TranslogReader use for .tlog files.
 */
public class TranslogCtrEncryptionTests extends OpenSearchTestCase {

    private static final String TRANSLOG_UUID = "a9QxbK3pTzuWq0-Jf9lZ2w";
    private static final Set<OpenOption> WRITE = Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    private static final Set<OpenOption> READ = Set.of(StandardOpenOption.READ);
    private static final String MARKER = "secret-marker-";

    private KeyResolver keyResolver;
    private Path dir;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        byte[] keyBytes = new byte[32];
        random().nextBytes(keyBytes);
        Key key = new SecretKeySpec(keyBytes, "AES");
        keyResolver = () -> key;
        dir = createTempDir();
    }

    /** A translog written op by op; offsets[i] is where op i starts. */
    private static final class Written {
        final Path path;
        final int headerSize;
        final List<byte[]> ops = new ArrayList<>();
        final List<Long> offsets = new ArrayList<>();
        long end;

        Written(Path path, int headerSize) {
            this.path = path;
            this.headerSize = headerSize;
            this.end = headerSize;
        }
    }

    private CryptoFileChannelWrapper open(Path path, Set<OpenOption> options) throws IOException {
        return new CryptoFileChannelWrapper(FileChannel.open(path, options), keyResolver, path, options, TRANSLOG_UUID);
    }

    private Written writeOps(CryptoFileChannelWrapper channel, Path path, int count) throws IOException {
        TranslogHeader header = new TranslogHeader(TRANSLOG_UUID, 1L);
        header.write(channel, false);
        Written w = new Written(path, header.sizeInBytes());
        for (int i = 0; i < count; i++) {
            // Variable lengths so ops straddle AES blocks and the old 8KB chunk boundaries at odd offsets.
            byte[] op = (MARKER + i + "-" + "x".repeat(randomIntBetween(1, 700))).getBytes(StandardCharsets.UTF_8);
            w.offsets.add(w.end);
            w.ops.add(op);
            w.end += writeFully(channel, op, w.end);
        }
        return w;
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

    public void testReadOfJustWrittenOpWhileWriterOpen() throws IOException {
        Path path = dir.resolve("translog-1.tlog");
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            Written w = writeOps(writer, path, 1);
            assertArrayEquals(w.ops.get(0), readFully(writer, w.offsets.get(0), w.ops.get(0).length));
        }
    }

    public void testRandomReadsWhileWriterOpen() throws IOException {
        Path path = dir.resolve("translog-1.tlog");
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            Written w = writeOps(writer, path, 80);
            // The last op lives in what used to be the open (untagged) tail chunk.
            int last = w.ops.size() - 1;
            assertArrayEquals(w.ops.get(last), readFully(writer, w.offsets.get(last), w.ops.get(last).length));
            for (int k = 0; k < 50; k++) {
                int i = randomIntBetween(0, last);
                assertArrayEquals("op " + i, w.ops.get(i), readFully(writer, w.offsets.get(i), w.ops.get(i).length));
            }
        }
    }

    public void testUnalignedRangeReads() throws IOException {
        Path path = dir.resolve("translog-1.tlog");
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            Written w = writeOps(writer, path, 40);
            byte[] plain = new byte[(int) (w.end - w.headerSize)];
            for (int i = 0; i < w.ops.size(); i++) {
                byte[] op = w.ops.get(i);
                System.arraycopy(op, 0, plain, (int) (w.offsets.get(i) - w.headerSize), op.length);
            }
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
        Path path = dir.resolve("translog-1.tlog");
        CryptoFileChannelWrapper writer = open(path, WRITE);
        Written w = writeOps(writer, path, 60);
        writer.force(false);
        // Simulate a crash: the writer is abandoned without close(), then a new channel replays the file.
        try (CryptoFileChannelWrapper reader = open(path, READ)) {
            for (int i = 0; i < w.ops.size(); i++) {
                assertArrayEquals("op " + i, w.ops.get(i), readFully(reader, w.offsets.get(i), w.ops.get(i).length));
            }
        } finally {
            writer.close();
        }
    }

    public void testCiphertextHasPlaintextLengthAndHidesData() throws IOException {
        Path path = dir.resolve("translog-1.tlog");
        Written w;
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            w = writeOps(writer, path, 30);
        }
        byte[] raw = Files.readAllBytes(path);
        assertEquals("ciphertext must be the same length as plaintext", w.end, raw.length);
        assertFalse(new String(raw, StandardCharsets.ISO_8859_1).contains(MARKER));
    }

    public void testNoKeystreamReuseWithinFile() throws IOException {
        Path path = dir.resolve("translog-1.tlog");
        byte[] zeros = new byte[3 * 8192];
        int headerSize;
        try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
            TranslogHeader header = new TranslogHeader(TRANSLOG_UUID, 1L);
            header.write(writer, false);
            headerSize = header.sizeInBytes();
            writeFully(writer, zeros, headerSize);
        }
        // Encrypting zeros writes the keystream itself. A keystream that is never reused has no repeated
        // 32-byte window anywhere in the file, whatever the on-disk layout (chunked or not).
        byte[] raw = Files.readAllBytes(path);
        Set<ByteBuffer> windows = new HashSet<>();
        for (int i = headerSize; i + 32 <= raw.length; i++) {
            ByteBuffer window = ByteBuffer.wrap(Arrays.copyOfRange(raw, i, i + 32));
            assertTrue("keystream repeats: 32-byte window at file offset " + i + " appears earlier", windows.add(window));
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

    public void testSameOffsetsInDifferentGenerationsUseDifferentKeystreams() throws IOException {
        byte[] zeros = new byte[4096];
        byte[][] raw = new byte[2][];
        int headerSize = 0;
        for (int gen = 1; gen <= 2; gen++) {
            Path path = dir.resolve("translog-" + gen + ".tlog");
            try (CryptoFileChannelWrapper writer = open(path, WRITE)) {
                TranslogHeader header = new TranslogHeader(TRANSLOG_UUID, 1L);
                header.write(writer, false);
                headerSize = header.sizeInBytes();
                writeFully(writer, zeros, headerSize);
            }
            raw[gen - 1] = Files.readAllBytes(path);
        }
        assertFalse(
            Arrays.equals(Arrays.copyOfRange(raw[0], headerSize, raw[0].length), Arrays.copyOfRange(raw[1], headerSize, raw[1].length))
        );
    }
}

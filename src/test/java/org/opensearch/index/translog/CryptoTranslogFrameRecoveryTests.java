/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.translog;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.util.ArrayList;
import java.util.List;

import javax.crypto.spec.SecretKeySpec;

import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The encrypted translog driven through OpenSearch's own TranslogWriter, checkpoints and recovery:
 * realtime reads of just-added operations, and replay after the translog was never closed.
 */
public class CryptoTranslogFrameRecoveryTests extends OpenSearchTestCase {

    private static final String MARKER = "frame-recovery-marker-";
    private static final long PRIMARY_TERM = 1L;

    private KeyResolver keyResolver;
    private ShardId shardId;
    private Path translogDir;
    private String translogUUID;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        byte[] keyBytes = new byte[32];
        random().nextBytes(keyBytes);
        Key key = new SecretKeySpec(keyBytes, "AES");
        keyResolver = () -> key;
        shardId = new ShardId("index", "_na_", 0);
        translogDir = createTempDir();
        translogUUID = UUIDs.randomBase64UUID();
        Translog
            .createEmptyTranslog(
                translogDir,
                shardId,
                SequenceNumbers.NO_OPS_PERFORMED,
                PRIMARY_TERM,
                translogUUID,
                new CryptoChannelFactory(keyResolver, translogUUID)
            );
    }

    private CryptoTranslog openTranslog() throws IOException {
        TranslogConfig config = new TranslogConfig(
            shardId,
            translogDir,
            IndexSettingsModule.newIndexSettings("index", Settings.EMPTY),
            BigArrays.NON_RECYCLING_INSTANCE,
            "",
            false
        );
        return new CryptoTranslog(
            config,
            translogUUID,
            new DefaultTranslogDeletionPolicy(-1, -1, 0),
            () -> SequenceNumbers.NO_OPS_PERFORMED,
            () -> PRIMARY_TERM,
            seqNo -> {},
            keyResolver
        );
    }

    private byte[] source(int i) {
        return ("{\"" + MARKER + i + "\":\"" + "y".repeat(randomIntBetween(10, 3000)) + "\"}").getBytes(StandardCharsets.UTF_8);
    }

    private static void assertOp(Translog.Operation op, int i, byte[] expectedSource) {
        Translog.Index index = (Translog.Index) op;
        assertEquals("id" + i, index.id());
        assertArrayEquals("source of op " + i, expectedSource, BytesReference.toBytes(index.source()));
    }

    public void testRealtimeReadsAndReplayAfterCrash() throws IOException {
        List<byte[]> sources = new ArrayList<>();
        CryptoTranslog translog = openTranslog();
        int synced = 0;
        try {
            for (int i = 0; i < 500; i++) {
                byte[] src = source(i);
                sources.add(src);
                Translog.Location location = translog.add(new Translog.Index("id" + i, i, PRIMARY_TERM, src));
                // Realtime get: read the op back while it is still in the writer's open tail.
                assertOp(translog.readOperation(location), i, src);
                if (i % 50 == 49) {
                    translog.sync();
                    synced = i + 1;
                }
                if (i % 200 == 199) {
                    translog.rollGeneration();
                }
            }
            // Ops added after the last sync may or may not survive the crash.
            for (int i = 500; i < 530; i++) {
                byte[] src = source(i);
                sources.add(src);
                translog.add(new Translog.Index("id" + i, i, PRIMARY_TERM, src));
            }

            // Crash: the translog is never closed. A new translog recovers from the files on disk.
            try (CryptoTranslog recovered = openTranslog(); Translog.Snapshot snapshot = recovered.newSnapshot()) {
                List<Translog.Operation> ops = new ArrayList<>();
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    ops.add(op);
                }
                assertTrue("recovered " + ops.size() + " ops, synced " + synced, ops.size() >= synced);
                ops.sort((a, b) -> Long.compare(a.seqNo(), b.seqNo()));
                for (int i = 0; i < ops.size(); i++) {
                    assertEquals(i, ops.get(i).seqNo());
                    assertOp(ops.get(i), i, sources.get(i));
                }
            }
        } finally {
            IOUtils.close(translog);
        }

        try (DirectoryStream<Path> files = Files.newDirectoryStream(translogDir, "*.tlog")) {
            for (Path file : files) {
                String raw = new String(Files.readAllBytes(file), StandardCharsets.ISO_8859_1);
                assertFalse("plaintext in " + file.getFileName(), raw.contains(MARKER));
            }
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;
import java.util.Random;

import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.niofs.CryptoNIOFSDirectory;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Deterministic reproduction of the shared-FileChannel clone/close bug in
 * CryptoBufferedIndexInput.
 *
 * A root IndexInput and all of its clones share ONE FileChannel. Only the root
 * should close it. On unfixed code, Object.clone() bitwise-copies isClone=false into
 * a clone of the root, so closing that clone closes the shared channel. A subsequent
 * read through the (still-open) parent then fails with ClosedChannelException.
 *
 * On unfixed code (main): FAILS — read after clone.close() throws ClosedChannelException.
 * On fixed code:          PASSES — clone.close() is a no-op on the shared channel.
 */
public class CloneCloseReproTests extends OpenSearchTestCase {

    private Directory getDirectory(Path file) throws Exception {
        CryptoMetricsService.initialize(mock(MetricsRegistry.class));

        byte[] rawKey = new byte[32]; // 256-bit AES key
        new Random(42).nextBytes(rawKey);

        KeyResolver keyResolver = mock(KeyResolver.class);
        when(keyResolver.getDataKey()).thenReturn(new SecretKeySpec(rawKey, "AES"));

        Provider provider = Security.getProvider("SunJCE");
        assertNotNull("SunJCE provider should be available", provider);

        EncryptionMetadataCache cache = new EncryptionMetadataCache();
        return new CryptoNIOFSDirectory(FSLockFactory.getDefault(), file, provider, keyResolver, cache);
    }

    public void testReadAfterCloneCloseMustNotThrow() throws Exception {
        try (Directory dir = getDirectory(createTempDir())) {
            final String fileName = "shared-channel-file";
            final int dataSize = 64 * 1024; // large enough to force real channel reads

            // 1. Ingest some data.
            byte[] data = new byte[dataSize];
            new Random(7).nextBytes(data);
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                out.writeBytes(data, data.length);
            }

            // 2. Open an IndexInput on it and read some bytes from the parent.
            try (IndexInput parent = dir.openInput(fileName, IOContext.DEFAULT)) {
                parent.seek(0);
                byte[] before = new byte[16];
                parent.readBytes(before, 0, before.length);

                // 3. Create a clone and close the clone.
                // clone() shares the parent's FileChannel. On unfixed code the clone
                // inherits isClone=false, so clone.close() closes the SHARED channel.
                IndexInput clone = parent.clone();
                clone.close();

                // 4. Read from the parent again. The parent is still open, so this must
                // succeed. We seek FAR past the initial read so BufferedIndexInput can't
                // serve it from its in-memory buffer and is forced to hit the FileChannel.
                // On unfixed code the shared channel is already closed, so channel.read()
                // throws java.nio.channels.ClosedChannelException.
                final long farPos = 40_000L; // well beyond any BufferedIndexInput buffer, < logical length
                byte[] after = new byte[16];
                parent.seek(farPos);
                parent.readBytes(after, 0, after.length); // <-- throws ClosedChannelException on unfixed code

                // Sanity: bytes at farPos match the source data (footer-excluded region).
                byte[] expected = new byte[16];
                System.arraycopy(data, (int) farPos, expected, 0, 16);
                assertArrayEquals("parent should read correct bytes after clone.close()", expected, after);
            }
        }
    }
}

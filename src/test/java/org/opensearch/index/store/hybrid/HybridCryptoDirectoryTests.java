/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.hybrid;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;
import java.util.Random;
import java.util.Set;

import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.index.store.bufferpoolfs.BufferPoolDirectory;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.key.KeyResolver;

/**
 * Unit tests for HybridCryptoDirectory routing logic.
 */
public class HybridCryptoDirectoryTests {

    private Path tempDir;
    private KeyResolver keyResolver;
    private Provider provider;
    private EncryptionMetadataCache encryptionMetadataCache;
    private LockFactory lockFactory;
    private BufferPoolDirectory bufferPoolDirectory;
    private Set<String> nioExtensions;

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("hybrid-test");

        // Create mock KeyResolver
        keyResolver = mock(KeyResolver.class);
        byte[] rawKey = new byte[32];
        new Random().nextBytes(rawKey);
        when(keyResolver.getDataKey()).thenReturn(new SecretKeySpec(rawKey, "AES"));

        provider = Security.getProvider("SunJCE");
        assertNotNull("Provider should not be null", provider);

        encryptionMetadataCache = new EncryptionMetadataCache();
        lockFactory = FSLockFactory.getDefault();

        // Mock CryptoDirectIODirectory
        bufferPoolDirectory = mock(BufferPoolDirectory.class);
        when(bufferPoolDirectory.getDirectory()).thenReturn(tempDir);

        // Default NIO extensions (metadata/small files)
        nioExtensions = Set.of("si", "cfe", "fnm", "fdx", "fdt", "pos", "pay", "nvm", "dvm", "tvx", "tvd", "liv", "dii", "vem");
    }

    @After
    public void tearDown() throws Exception {
        if (tempDir != null && Files.exists(tempDir)) {
            Files.walk(tempDir).sorted((a, b) -> -a.compareTo(b)).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    // ignore
                }
            });
        }
    }

    @Test
    public void testCreateOutputRoutesToDirectIOForDataFiles() throws Exception {
        try (
            HybridCryptoDirectory hybridDir = new HybridCryptoDirectory(
                lockFactory,
                bufferPoolDirectory,
                provider,
                keyResolver,
                encryptionMetadataCache,
                nioExtensions
            )
        ) {
            // Mock the createOutput for DirectIO
            IndexOutput mockOutput = mock(IndexOutput.class);
            when(bufferPoolDirectory.createOutput(eq("test.tim"), any(IOContext.class))).thenReturn(mockOutput);

            // .tim is NOT in nioExtensions, should route to DirectIO
            IndexOutput output = hybridDir.createOutput("test.tim", IOContext.DEFAULT);
            assertEquals(mockOutput, output);
            verify(bufferPoolDirectory).createOutput(eq("test.tim"), any(IOContext.class));
        }
    }

    @Test
    public void testCreateOutputRoutesToNIOForMetadataFiles() throws Exception {
        HybridCryptoDirectory hybridDir = spy(
            new HybridCryptoDirectory(lockFactory, bufferPoolDirectory, provider, keyResolver, encryptionMetadataCache, nioExtensions)
        );

        try {
            // .si IS in nioExtensions, should route to NIO (super)
            // This will create an actual file via CryptoNIOFSDirectory
            IndexOutput output = hybridDir.createOutput("test.si", IOContext.DEFAULT);
            assertNotNull(output);
            output.close();

            // Verify DirectIO was NOT called
            verify(bufferPoolDirectory, never()).createOutput(eq("test.si"), any(IOContext.class));

            // Verify file was created
            assertTrue(Files.exists(tempDir.resolve("test.si")));
        } finally {
            hybridDir.close();
        }
    }

    @Test
    public void testOpenInputRoutesToDirectIOForDataFiles() throws Exception {
        try (
            HybridCryptoDirectory hybridDir = new HybridCryptoDirectory(
                lockFactory,
                bufferPoolDirectory,
                provider,
                keyResolver,
                encryptionMetadataCache,
                nioExtensions
            )
        ) {
            // Mock the openInput for DirectIO
            IndexInput mockInput = mock(IndexInput.class);
            when(bufferPoolDirectory.openInput(eq("test.doc"), any(IOContext.class))).thenReturn(mockInput);

            // Create dummy file so ensureCanRead passes
            Files.createFile(tempDir.resolve("test.doc"));

            // .doc is NOT in nioExtensions, should route to DirectIO
            IndexInput input = hybridDir.openInput("test.doc", IOContext.DEFAULT);
            assertEquals(mockInput, input);
            verify(bufferPoolDirectory).openInput(eq("test.doc"), any(IOContext.class));
        }
    }

    @Test
    public void testOpenInputRoutesToNIOForMetadataFiles() throws Exception {
        HybridCryptoDirectory hybridDir = spy(
            new HybridCryptoDirectory(lockFactory, bufferPoolDirectory, provider, keyResolver, encryptionMetadataCache, nioExtensions)
        );

        try {
            // First create a file via NIO
            IndexOutput output = hybridDir.createOutput("test.fnm", IOContext.DEFAULT);
            output.writeByte((byte) 42);
            output.close();

            // .fnm IS in nioExtensions, should route to NIO (super)
            IndexInput input = hybridDir.openInput("test.fnm", IOContext.DEFAULT);
            assertNotNull(input);
            input.close();

            // Verify DirectIO was NOT called
            verify(bufferPoolDirectory, never()).openInput(eq("test.fnm"), any(IOContext.class));
        } finally {
            hybridDir.close();
        }
    }

    @Test
    public void testDeleteFileRoutesToDirectIOForDataFiles() throws Exception {
        try (
            HybridCryptoDirectory hybridDir = new HybridCryptoDirectory(
                lockFactory,
                bufferPoolDirectory,
                provider,
                keyResolver,
                encryptionMetadataCache,
                nioExtensions
            )
        ) {
            // .cfs is NOT in nioExtensions, should route to DirectIO
            hybridDir.deleteFile("test.cfs");
            verify(bufferPoolDirectory).deleteFile("test.cfs");
        }
    }

    @Test
    public void testDeleteFileRoutesToNIOForMetadataFiles() throws Exception {
        HybridCryptoDirectory hybridDir = spy(
            new HybridCryptoDirectory(lockFactory, bufferPoolDirectory, provider, keyResolver, encryptionMetadataCache, nioExtensions)
        );

        try {
            // Create a file first
            Files.createFile(tempDir.resolve("test.dvm"));

            // .dvm IS in nioExtensions, should route to NIO (super)
            hybridDir.deleteFile("test.dvm");

            // Verify DirectIO was NOT called
            verify(bufferPoolDirectory, never()).deleteFile("test.dvm");

            // Verify file was deleted
            assertTrue(!Files.exists(tempDir.resolve("test.dvm")));
        } finally {
            hybridDir.close();
        }
    }

    @Test
    public void testRoutingForVariousExtensions() throws Exception {
        try (
            HybridCryptoDirectory hybridDir = new HybridCryptoDirectory(
                lockFactory,
                bufferPoolDirectory,
                provider,
                keyResolver,
                encryptionMetadataCache,
                nioExtensions
            )
        ) {
            IndexOutput mockOutput = mock(IndexOutput.class);

            // Test data file extensions (should go to DirectIO)
            String[] directIOExtensions = { "tim", "doc", "dvd", "nvd", "cfs", "kdd", "tip", "tmd" };
            for (String ext : directIOExtensions) {
                when(bufferPoolDirectory.createOutput(eq("test." + ext), any(IOContext.class))).thenReturn(mockOutput);
                IndexOutput output = hybridDir.createOutput("test." + ext, IOContext.DEFAULT);
                assertEquals("Extension ." + ext + " should route to DirectIO", mockOutput, output);
            }
        }
    }

    @Test
    public void testUnknownExtensionRoutesToDirectIO() throws Exception {
        try (
            HybridCryptoDirectory hybridDir = new HybridCryptoDirectory(
                lockFactory,
                bufferPoolDirectory,
                provider,
                keyResolver,
                encryptionMetadataCache,
                nioExtensions
            )
        ) {
            IndexOutput mockOutput = mock(IndexOutput.class);
            // .xyz is not in nioExtensions, should route to DirectIO
            when(bufferPoolDirectory.createOutput(eq("test.xyz"), any(IOContext.class))).thenReturn(mockOutput);

            IndexOutput output = hybridDir.createOutput("test.xyz", IOContext.DEFAULT);
            assertEquals(mockOutput, output);
            verify(bufferPoolDirectory).createOutput(eq("test.xyz"), any(IOContext.class));
        }
    }
}

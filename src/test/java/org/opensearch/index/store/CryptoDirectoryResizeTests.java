/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.Provider;
import java.security.Security;

import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Randomness;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.niofs.CryptoNIOFSDirectory;

/**
 * Tests for clone/resize operations in CryptoDirectoryFactory.
 * Verifies that keyfiles are correctly copied from source to target indices
 * during clone, split, and shrink operations.
 */
public class CryptoDirectoryResizeTests extends LuceneTestCase {

    private Path tempDir;
    private CryptoDirectoryFactory factory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        tempDir = createTempDir();
        factory = new CryptoDirectoryFactory();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    private void invokeHandleResizeKeyfileCopy(IndexSettings indexSettings, Path targetIndexDirectory) throws Exception {
        factory.handleResizeOperation(indexSettings, targetIndexDirectory);
    }

    /**
     * Helper method to create mock IndexSettings with resize metadata.
     */
    private IndexSettings createIndexSettings(String indexUuid, String sourceUuid, String sourceName) {
        Settings settings = Settings
            .builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid)
            .put("index.store.type", "cryptofs")
            .put("index.store.crypto.key_provider", "dummy")
            .put("index.resize.source.uuid", sourceUuid)
            .put("index.resize.source.name", sourceName)
            .build();

        return new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );
    }

    /**
     * Test that keyfile is copied from source to target during clone operation.
     */
    public void testCloneOperationCopiesKeyfile() throws Exception {
        String sourceUuid = "source-uuid-123";
        String targetUuid = "target-uuid-456";

        Path indicesDir = tempDir.resolve("indices");
        Files.createDirectories(indicesDir);

        Path sourceIndexDir = indicesDir.resolve(sourceUuid);
        Files.createDirectories(sourceIndexDir);

        Path targetIndexDir = indicesDir.resolve(targetUuid);
        Files.createDirectories(targetIndexDir);

        // Create source keyfile with test data
        byte[] sourceKeyData = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        try (FSDirectory dir = FSDirectory.open(sourceIndexDir)) {
            try (IndexOutput out = dir.createOutput("keyfile", IOContext.DEFAULT)) {
                out.writeInt(sourceKeyData.length);
                out.writeBytes(sourceKeyData, 0, sourceKeyData.length);
            }
        }

        Path sourceKeyfile = sourceIndexDir.resolve("keyfile");
        assertTrue("Source keyfile should exist", Files.exists(sourceKeyfile));

        // Create IndexSettings with clone metadata
        IndexSettings indexSettings = createIndexSettings(targetUuid, sourceUuid, "source-index");

        // Invoke the private method
        invokeHandleResizeKeyfileCopy(indexSettings, targetIndexDir);

        // Verify keyfile was copied to target
        Path targetKeyfile = targetIndexDir.resolve("keyfile");
        assertTrue("Target keyfile should exist after clone", Files.exists(targetKeyfile));

        // Verify keyfile contents are identical
        byte[] targetKeyData;
        try (FSDirectory dir = FSDirectory.open(targetIndexDir)) {
            try (org.apache.lucene.store.IndexInput in = dir.openInput("keyfile", IOContext.READONCE)) {
                int length = in.readInt();
                targetKeyData = new byte[length];
                in.readBytes(targetKeyData, 0, length);
            }
        }

        assertArrayEquals("Keyfile contents should match", sourceKeyData, targetKeyData);
    }

    /**
     * Test that split operation copies keyfile from source to target.
     */
    public void testSplitOperationCopiesKeyfile() throws Exception {
        String sourceUuid = "split-source-uuid";
        String targetUuid = "split-target-uuid";

        Path indicesDir = tempDir.resolve("indices");
        Files.createDirectories(indicesDir);

        Path sourceIndexDir = indicesDir.resolve(sourceUuid);
        Files.createDirectories(sourceIndexDir);

        Path targetIndexDir = indicesDir.resolve(targetUuid);
        Files.createDirectories(targetIndexDir);

        // Create source keyfile
        byte[] keyData = new byte[] { 9, 8, 7, 6, 5, 4, 3, 2, 1 };
        try (FSDirectory dir = FSDirectory.open(sourceIndexDir)) {
            try (IndexOutput out = dir.createOutput("keyfile", IOContext.DEFAULT)) {
                out.writeInt(keyData.length);
                out.writeBytes(keyData, 0, keyData.length);
            }
        }

        // Create IndexSettings with split metadata
        IndexSettings indexSettings = createIndexSettings(targetUuid, sourceUuid, "split-source");

        // Invoke the private method
        invokeHandleResizeKeyfileCopy(indexSettings, targetIndexDir);

        Path targetKeyfile = targetIndexDir.resolve("keyfile");
        assertTrue("Target keyfile should exist after split", Files.exists(targetKeyfile));

        byte[] targetKeyData;
        try (FSDirectory dir = FSDirectory.open(targetIndexDir)) {
            try (org.apache.lucene.store.IndexInput in = dir.openInput("keyfile", IOContext.READONCE)) {
                int length = in.readInt();
                targetKeyData = new byte[length];
                in.readBytes(targetKeyData, 0, length);
            }
        }

        assertArrayEquals("Split keyfile contents should match source", keyData, targetKeyData);
    }

    /**
     * Test that shrink operation copies keyfile from source to target.
     */
    public void testShrinkOperationCopiesKeyfile() throws Exception {
        String sourceUuid = "shrink-source-uuid";
        String targetUuid = "shrink-target-uuid";

        Path indicesDir = tempDir.resolve("indices");
        Files.createDirectories(indicesDir);

        Path sourceIndexDir = indicesDir.resolve(sourceUuid);
        Files.createDirectories(sourceIndexDir);

        Path targetIndexDir = indicesDir.resolve(targetUuid);
        Files.createDirectories(targetIndexDir);

        // Create source keyfile
        byte[] keyData = new byte[32];
        for (int i = 0; i < 32; i++) {
            keyData[i] = (byte) i;
        }
        try (FSDirectory dir = FSDirectory.open(sourceIndexDir)) {
            try (IndexOutput out = dir.createOutput("keyfile", IOContext.DEFAULT)) {
                out.writeInt(keyData.length);
                out.writeBytes(keyData, 0, keyData.length);
            }
        }

        // Create IndexSettings with shrink metadata
        IndexSettings indexSettings = createIndexSettings(targetUuid, sourceUuid, "shrink-source");

        // Invoke the private method
        invokeHandleResizeKeyfileCopy(indexSettings, targetIndexDir);

        Path targetKeyfile = targetIndexDir.resolve("keyfile");
        assertTrue("Target keyfile should exist after shrink", Files.exists(targetKeyfile));

        byte[] targetKeyData;
        try (FSDirectory dir = FSDirectory.open(targetIndexDir)) {
            try (org.apache.lucene.store.IndexInput in = dir.openInput("keyfile", IOContext.READONCE)) {
                int length = in.readInt();
                targetKeyData = new byte[length];
                in.readBytes(targetKeyData, 0, length);
            }
        }

        assertArrayEquals("Shrink keyfile contents should match source", keyData, targetKeyData);
    }

    /**
     * Test that non-resize operations don't copy keyfiles.
     */
    public void testNonResizeOperationDoesNotCopyKeyfile() throws Exception {
        String indexUuid = "regular-index-uuid";

        Path indicesDir = tempDir.resolve("indices");
        Files.createDirectories(indicesDir);

        Path indexDir = indicesDir.resolve(indexUuid);
        Files.createDirectories(indexDir);

        // Create IndexSettings WITHOUT resize metadata
        Settings settings = Settings
            .builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid)
            .put("index.store.type", "cryptofs")
            .put("index.store.crypto.key_provider", "dummy")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("regular-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        Path keyfile = indexDir.resolve("keyfile");
        assertFalse("Keyfile should not exist before operation", Files.exists(keyfile));

        // Invoke the private method - should not copy anything
        invokeHandleResizeKeyfileCopy(indexSettings, indexDir);

        // Keyfile should still not exist after non-resize operation
        assertFalse("Keyfile should not be created for non-resize operation", Files.exists(keyfile));
    }

    /**
     * Test that missing source keyfile is handled gracefully.
     */
    public void testMissingSourceKeyfileHandledGracefully() throws Exception {
        String sourceUuid = "missing-source-uuid";
        String targetUuid = "target-uuid";

        Path indicesDir = tempDir.resolve("indices");
        Files.createDirectories(indicesDir);

        // Create source directory but NO keyfile
        Path sourceIndexDir = indicesDir.resolve(sourceUuid);
        Files.createDirectories(sourceIndexDir);

        Path targetIndexDir = indicesDir.resolve(targetUuid);
        Files.createDirectories(targetIndexDir);

        IndexSettings indexSettings = createIndexSettings(targetUuid, sourceUuid, "missing-source");

        // Should not throw - missing source keyfile should be logged and handled
        invokeHandleResizeKeyfileCopy(indexSettings, targetIndexDir);

        // Verify target keyfile was not created
        Path targetKeyfile = targetIndexDir.resolve("keyfile");
        assertFalse("Target keyfile should not exist when source is missing", Files.exists(targetKeyfile));
    }

    /**
     * Test that existing target keyfile is not overwritten.
     */
    public void testExistingTargetKeyfileNotOverwritten() throws Exception {
        String sourceUuid = "source-existing-uuid";
        String targetUuid = "target-existing-uuid";

        Path indicesDir = tempDir.resolve("indices");
        Files.createDirectories(indicesDir);

        Path sourceIndexDir = indicesDir.resolve(sourceUuid);
        Files.createDirectories(sourceIndexDir);

        Path targetIndexDir = indicesDir.resolve(targetUuid);
        Files.createDirectories(targetIndexDir);

        // Create source keyfile
        byte[] sourceKeyData = new byte[] { 1, 2, 3 };
        try (FSDirectory dir = FSDirectory.open(sourceIndexDir)) {
            try (IndexOutput out = dir.createOutput("keyfile", IOContext.DEFAULT)) {
                out.writeInt(sourceKeyData.length);
                out.writeBytes(sourceKeyData, 0, sourceKeyData.length);
            }
        }

        // Create EXISTING target keyfile with different data
        byte[] existingTargetKeyData = new byte[] { 9, 9, 9 };
        try (FSDirectory dir = FSDirectory.open(targetIndexDir)) {
            try (IndexOutput out = dir.createOutput("keyfile", IOContext.DEFAULT)) {
                out.writeInt(existingTargetKeyData.length);
                out.writeBytes(existingTargetKeyData, 0, existingTargetKeyData.length);
            }
        }

        IndexSettings indexSettings = createIndexSettings(targetUuid, sourceUuid, "source-index");

        // Invoke the private method
        invokeHandleResizeKeyfileCopy(indexSettings, targetIndexDir);

        // Verify target keyfile still has original data (not overwritten)
        byte[] targetKeyData;
        try (FSDirectory dir = FSDirectory.open(targetIndexDir)) {
            try (org.apache.lucene.store.IndexInput in = dir.openInput("keyfile", IOContext.READONCE)) {
                int length = in.readInt();
                targetKeyData = new byte[length];
                in.readBytes(targetKeyData, 0, length);
            }
        }

        assertArrayEquals("Existing target keyfile should not be overwritten", existingTargetKeyData, targetKeyData);
    }

    /**
     * End-to-end test: Write encrypted documents to source index, copy keyfile,
     * copy encrypted segment files, and verify cloned index can decrypt and read documents.
     *
     * This test simulates the actual clone flow:
     * 1. Create source index with cryptofs settings (keyfile created automatically)
     * 2. Write and encrypt documents
     * 3. Clone operation copies keyfile
     * 4. Clone operation copies segment files
     * 5. Target index can decrypt and read documents
     */
    public void testEndToEndCloneWithEncryptedDocuments() throws Exception {
        String sourceUuid = "source-e2e-uuid";
        String targetUuid = "target-e2e-uuid";

        Path indicesDir = tempDir.resolve("indices");
        Files.createDirectories(indicesDir);

        Path sourceIndexDir = indicesDir.resolve(sourceUuid);
        Files.createDirectories(sourceIndexDir);

        Path targetIndexDir = indicesDir.resolve(targetUuid);
        Files.createDirectories(targetIndexDir);

        // Create a shared encryption key for testing
        byte[] rawKey = new byte[32]; // 256-bit AES key
        java.util.Random rnd = Randomness.get();
        rnd.nextBytes(rawKey);

        // Create KeyResolver that returns the same key for both source and target
        KeyResolver keyResolver = mock(KeyResolver.class);
        when(keyResolver.getDataKey()).thenReturn(new SecretKeySpec(rawKey, "AES"));

        Provider provider = Security.getProvider("SunJCE");

        // Create keyfile in source directory (as DefaultKeyResolver would do)
        // In real scenario, this is done by CryptoDirectoryFactory -> ShardKeyResolverRegistry -> DefaultKeyResolver
        byte[] encryptedKeyData = new byte[32]; // Simulates encrypted DEK from KMS
        rnd.nextBytes(encryptedKeyData);
        try (FSDirectory dir = FSDirectory.open(sourceIndexDir)) {
            try (IndexOutput out = dir.createOutput("keyfile", IOContext.DEFAULT)) {
                out.writeInt(encryptedKeyData.length);
                out.writeBytes(encryptedKeyData, 0, encryptedKeyData.length);
            }
        }

        // Step 1: Write encrypted documents to source directory
        EncryptionMetadataCache sourceCache = new EncryptionMetadataCache();
        Directory sourceDir = new CryptoNIOFSDirectory(FSLockFactory.getDefault(), sourceIndexDir, provider, keyResolver, sourceCache);

        // Write some test documents
        IndexWriterConfig config = new IndexWriterConfig();
        try (IndexWriter writer = new IndexWriter(sourceDir, config)) {
            // Add test documents
            Document doc1 = new Document();
            doc1.add(new StringField("id", "1", Field.Store.YES));
            doc1.add(new StringField("content", "test document one", Field.Store.YES));
            writer.addDocument(doc1);

            Document doc2 = new Document();
            doc2.add(new StringField("id", "2", Field.Store.YES));
            doc2.add(new StringField("content", "test document two", Field.Store.YES));
            writer.addDocument(doc2);

            Document doc3 = new Document();
            doc3.add(new StringField("id", "3", Field.Store.YES));
            doc3.add(new StringField("content", "test document three", Field.Store.YES));
            writer.addDocument(doc3);

            writer.commit();
        }

        // Verify source can read the documents
        try (DirectoryReader reader = DirectoryReader.open(sourceDir)) {
            assertEquals("Source should have 3 documents", 3, reader.numDocs());

            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs hits = searcher.search(new MatchAllDocsQuery(), 10);
            assertEquals("Should find all 3 documents", 3, hits.scoreDocs.length);
        }

        sourceDir.close();

        // Step 2: Simulate clone operation - copy keyfile
        IndexSettings indexSettings = createIndexSettings(targetUuid, sourceUuid, "source-index");
        invokeHandleResizeKeyfileCopy(indexSettings, targetIndexDir);

        // Verify keyfile was copied
        Path targetKeyfile = targetIndexDir.resolve("keyfile");
        assertTrue("Target keyfile should exist after clone", Files.exists(targetKeyfile));

        // Step 3: Simulate Lucene's segment file copy (copy all files except keyfile)
        try (var stream = Files.list(sourceIndexDir)) {
            for (String fileName : stream.map(p -> p.getFileName().toString()).toArray(String[]::new)) {
                if (!fileName.equals("keyfile") && !fileName.equals("write.lock")) {
                    Files.copy(sourceIndexDir.resolve(fileName), targetIndexDir.resolve(fileName), StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }

        // Step 4: Open target directory and verify it can decrypt and read documents
        EncryptionMetadataCache targetCache = new EncryptionMetadataCache();
        Directory targetDir = new CryptoNIOFSDirectory(FSLockFactory.getDefault(), targetIndexDir, provider, keyResolver, targetCache);

        try (DirectoryReader reader = DirectoryReader.open(targetDir)) {
            assertEquals("Target should have 3 documents", 3, reader.numDocs());

            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs hits = searcher.search(new MatchAllDocsQuery(), 10);
            assertEquals("Should find all 3 documents in cloned index", 3, hits.scoreDocs.length);

            // Verify we can actually read the document content
            Document doc = reader.storedFields().document(hits.scoreDocs[0].doc);
            assertTrue("Document should have id field", doc.get("id") != null);
            assertTrue("Document should have content field", doc.get("content") != null);
            assertTrue("Content should be one of our test documents", doc.get("content").startsWith("test document"));
        }

        targetDir.close();
    }
}

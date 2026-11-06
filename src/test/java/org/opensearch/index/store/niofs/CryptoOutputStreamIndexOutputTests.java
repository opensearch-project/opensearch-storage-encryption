/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.niofs;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Security;

import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.footer.EncryptionMetadataTrailer;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for the footer written by {@link CryptoOutputStreamIndexOutput}.
 */
public class CryptoOutputStreamIndexOutputTests extends OpenSearchTestCase {

    /** A file that fits in one frame records one frame and one GCM tag. */
    public void testSingleFrameFileRecordsFrameCountOfOne() throws Exception {
        KeyResolver keyResolver = mock(KeyResolver.class);
        when(keyResolver.getDataKey()).thenReturn(new SecretKeySpec(randomByteArrayOfLength(32), "AES"));
        Path file = createTempDir().resolve("single-frame");

        try (
            OutputStream os = Files.newOutputStream(file);
            IndexOutput out = new CryptoOutputStreamIndexOutput(
                "single-frame",
                file,
                os,
                keyResolver,
                Security.getProvider("SunJCE"),
                EncryptionMetadataTrailer.ALGORITHM_AES_256_GCM,
                file,
                new EncryptionMetadataCache()
            )
        ) {
            byte[] data = randomByteArrayOfLength(randomIntBetween(1, 200_000));
            out.writeBytes(data, 0, data.length);
        }

        byte[] bytes = Files.readAllBytes(file);
        // Footer tail: ... TagCount(4) FrameSize(8) FrameCount(4) MessageId(16) KeyMetadataLength(2) AlgorithmId(2) FooterLength(4) Magic
        int frameCountPos = bytes.length - EncryptionMetadataTrailer.MAGIC.length - EncryptionMetadataTrailer.FOOTER_LENGTH_SIZE
            - EncryptionMetadataTrailer.ALGORITHM_ID_SIZE - EncryptionMetadataTrailer.KEY_METADATA_LENGTH_SIZE
            - EncryptionMetadataTrailer.MESSAGE_ID_SIZE - EncryptionMetadataTrailer.FRAME_COUNT_SIZE;
        int tagCountPos = frameCountPos - EncryptionMetadataTrailer.FRAME_SIZE_SIZE - EncryptionMetadataTrailer.TAG_COUNT_SIZE;

        assertEquals("tag count", 1, ByteBuffer.wrap(bytes, tagCountPos, 4).getInt());
        assertEquals("frame count", 1, ByteBuffer.wrap(bytes, frameCountPos, 4).getInt());
    }
}

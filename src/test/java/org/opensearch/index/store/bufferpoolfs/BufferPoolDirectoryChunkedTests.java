/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.tests.store.BaseChunkedDirectoryTestCase;
import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.index.store.CaffeineThreadLeakFilter;
import org.opensearch.index.store.CryptoTestDirectoryFactory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

/**
 * Runs Lucene's BaseChunkedDirectoryTestCase against BufferPoolDirectory.
 *
 * <p>This inherits all ~55 base directory contract tests from BaseDirectoryTestCase
 * PLUS ~14 additional tests that exercise cross-block reads for GroupVInt,
 * clone/slice close with chunks, exhaustive seeking across boundaries,
 * slice-of-slice across chunks, random chunk sizes with full index write/read,
 * and cross-boundary reads for bytes/longs/floats.
 *
 * <p><strong>Limitation:</strong> BufferPoolDirectory has a fixed block size of 8192
 * (CACHE_BLOCK_SIZE), so the {@code maxChunkSize} parameter from the framework is
 * ignored. The chunk-specific tests create data sized to cross the <em>requested</em>
 * chunk boundary (8, 16, 64 bytes, etc.), not the 8192-byte block boundary. As a result,
 * the ~14 chunk-boundary tests pass but do not actually stress the 8192-byte block
 * boundaries. Real block-boundary coverage comes from the ~55 base tests and from
 * {@code CachedMemorySegmentIndexInputRandomReadTests}.
 */
@ThreadLeakFilters(filters = CaffeineThreadLeakFilter.class)
public class BufferPoolDirectoryChunkedTests extends BaseChunkedDirectoryTestCase {

    @Override
    protected Directory getDirectory(Path path, int maxChunkSize) throws IOException {
        // BufferPoolDirectory has a fixed block size of 8192; maxChunkSize is ignored.
        return CryptoTestDirectoryFactory.createBufferPoolDirectory(path, FSLockFactory.getDefault());
    }

    // ==================== Overrides for known issues ====================

    @Override
    public void testCreateTempOutput() throws Throwable {
        try (Directory dir = getDirectory(createTempDir())) {
            CryptoTestDirectoryFactory.assertTempOutputRoundTrip(dir, atLeast(50), () -> newIOContext(random()));
        }
    }

    @Override
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/opensearch-storage-encryption/issues/47")
    public void testSliceOutOfBounds() {}

    @Override
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/opensearch-storage-encryption/issues/47")
    public void testThreadSafetyInListAll() {}
}

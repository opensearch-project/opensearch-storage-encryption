/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.hybrid;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.index.store.CaffeineThreadLeakFilter;
import org.opensearch.index.store.CryptoTestDirectoryFactory;
import org.opensearch.index.store.OpenSearchBaseDirectoryTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

/**
 * Runs Lucene's full directory contract test suite against HybridCryptoDirectory.
 *
 * <p>Uses {@link CryptoTestDirectoryFactory#createBufferPoolRoutedHybridDirectory} which
 * routes ALL files through the BufferPool encryption path. This is necessary because
 * Lucene's contract tests use extensionless file names (e.g., "foobar", "byte") which
 * would otherwise always route to NIO due to {@code HybridCryptoDirectory.delegeteBufferPool("")}
 * returning false. The NIO path is already covered by {@code CryptoDirectoryTests}.
 *
 * <p>Extension-based routing logic is separately tested by {@code HybridCryptoDirectoryTests}.
 */
@ThreadLeakFilters(filters = CaffeineThreadLeakFilter.class)
public class HybridCryptoDirectoryBaseTests extends OpenSearchBaseDirectoryTestCase {

    @Override
    protected Directory getDirectory(Path file) throws IOException {
        return CryptoTestDirectoryFactory.createBufferPoolRoutedHybridDirectory(file, org.apache.lucene.store.FSLockFactory.getDefault());
    }

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

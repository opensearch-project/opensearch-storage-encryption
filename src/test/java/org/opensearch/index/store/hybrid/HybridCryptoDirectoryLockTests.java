/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.hybrid;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.tests.store.BaseLockFactoryTestCase;
import org.opensearch.index.store.CaffeineThreadLeakFilter;
import org.opensearch.index.store.CryptoTestDirectoryFactory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

/**
 * Runs Lucene's 5 lock factory tests (including stress test with concurrent
 * IndexWriter + IndexSearcher) against HybridCryptoDirectory.
 */
@ThreadLeakFilters(filters = CaffeineThreadLeakFilter.class)
public class HybridCryptoDirectoryLockTests extends BaseLockFactoryTestCase {

    @Override
    protected Directory getDirectory(Path path) throws IOException {
        return CryptoTestDirectoryFactory.createHybridCryptoDirectory(path, FSLockFactory.getDefault());
    }
}

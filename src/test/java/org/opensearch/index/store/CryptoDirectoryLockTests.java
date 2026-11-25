/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.tests.store.BaseLockFactoryTestCase;

/**
 * Runs Lucene's 5 lock factory tests (including stress test with concurrent
 * IndexWriter + IndexSearcher) against CryptoNIOFSDirectory.
 */
public class CryptoDirectoryLockTests extends BaseLockFactoryTestCase {

    @Override
    protected Directory getDirectory(Path path) throws IOException {
        return CryptoTestDirectoryFactory.createCryptoNIOFSDirectory(path, FSLockFactory.getDefault());
    }
}

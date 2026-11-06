/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import org.junit.After;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.pool.PoolBuilder;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * The shared pool is static, so it outlives a node. A node started later in the same JVM must not
 * reuse a pool that an earlier node built from its own settings and then closed.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class CryptoDirectoryFactorySharedPoolTests extends OpenSearchTestCase {

    @After
    public void closePool() {
        CryptoDirectoryFactory.closeSharedPool();
    }

    private static Settings poolSettings(double cacheToPoolRatio) {
        return Settings
            .builder()
            .put("plugins.crypto.enabled", true)
            .put("node.store.crypto.pool_size_percentage", 0.001)
            .put("node.store.crypto.warmup_percentage", 0.0)
            .put("node.store.crypto.cache_to_pool_ratio", cacheToPoolRatio)
            .build();
    }

    public void testCloseSharedPoolLetsTheNextNodeBuildItsOwnPool() {
        CryptoDirectoryFactory.setNodeSettings(poolSettings(0.5));
        PoolBuilder.PoolResources first = CryptoDirectoryFactory.ensurePoolInitialized();
        assertSame(first, CryptoDirectoryFactory.ensurePoolInitialized());

        CryptoDirectoryFactory.closeSharedPool();
        assertNull(CryptoDirectoryFactory.getSharedBlockCache());
        assertNull(CryptoDirectoryFactory.getSharedFileChannelCache());

        CryptoDirectoryFactory.setNodeSettings(poolSettings(0.8));
        PoolBuilder.PoolResources second = CryptoDirectoryFactory.ensurePoolInitialized();
        assertNotSame(first, second);
        assertTrue("the new pool must use the new node's cache-to-pool ratio", second.getMaxCacheBlocks() > first.getMaxCacheBlocks());
    }

    public void testCloseSharedPoolIsIdempotent() {
        CryptoDirectoryFactory.setNodeSettings(poolSettings(0.5));
        CryptoDirectoryFactory.ensurePoolInitialized();
        CryptoDirectoryFactory.closeSharedPool();
        CryptoDirectoryFactory.closeSharedPool();
        assertNull(CryptoDirectoryFactory.getSharedBlockCache());
    }
}

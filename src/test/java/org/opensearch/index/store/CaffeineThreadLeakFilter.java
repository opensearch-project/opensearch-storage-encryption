/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import org.opensearch.common.SuppressForbidden;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * Thread leak filter for Caffeine cache's ForkJoinPool worker threads.
 * Caffeine uses the ForkJoinPool.commonPool() for async operations,
 * which creates worker threads that the test framework may detect as leaks.
 */
@SuppressForbidden(reason = "Thread matching for test leak filtering")
public class CaffeineThreadLeakFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith("ForkJoinPool.commonPool-worker-") || t.getName().startsWith("pool-gc-debt-monitor");
    }
}

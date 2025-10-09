/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import org.opensearch.index.store.PanamaNativeAccess;

public class DirectIoConfigs {
    public static final int DIRECT_IO_ALIGNMENT = Math.max(512, getPageSizeSafe());
    public static final int DIRECT_IO_WRITE_BUFFER_SIZE_POWER = 18;

    /**
     * Total size of the reserved memory pool in bytes.
     * Can be overridden via system property: opensearch.storage.pool.size.bytes
     * Default: 32GB
     */
    public static final long RESEVERED_POOL_SIZE_IN_BYTES = Long
        .parseLong(System.getProperty("opensearch.storage.pool.size.bytes", String.valueOf(32L * 1024 * 1024 * 1024)));

    /**
     * Percentage of memory pool to pre-allocate during warm-up (0.0 to 1.0).
     * In Java 21, Arena.allocate() requires direct memory to be allocated upfront.
     * we warm-up pre-allocates memory segments to avoid allocation overhead during I/O operations,
     * but can cause OutOfMemoryError in memory-constrained environments like tests.
     *
     * Can be overridden via system property: opensearch.storage.warmup.percentage
     * Default: 0.2 (20% warm-up)
     * Tests typically set this to 0 to avoid direct buffer memory exhaustion.
     */
    public static final double WARM_UP_PERCENTAGE = Double.parseDouble(System.getProperty("opensearch.storage.warmup.percentage", "0.2"));

    public static final int CACHE_BLOCK_SIZE_POWER = 13;
    public static final int CACHE_BLOCK_SIZE = 1 << CACHE_BLOCK_SIZE_POWER;
    public static final long CACHE_BLOCK_MASK = CACHE_BLOCK_SIZE - 1;

    public static final int CACHE_INITIAL_SIZE = 65536;

    public static final int READ_AHEAD_QUEUE_SIZE = 4096;

    private static int getPageSizeSafe() {
        try {
            return PanamaNativeAccess.getPageSize();
        } catch (Throwable e) {
            // Native access not available (class initialization failed, native library not found, etc.)
            // Fall back to common page size
            return 4096;
        }
    }
}

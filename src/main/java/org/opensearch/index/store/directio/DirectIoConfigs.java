/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import org.opensearch.index.store.PanamaNativeAccess;

/**
 * Configuration constants for Direct I/O operations and caching.
 * 
 * <p>This class defines system-wide configuration parameters used by the Direct I/O
 * subsystem, including alignment requirements, buffer sizes, cache configurations,
 * and memory pool settings. Many values can be overridden via system properties
 * for testing and performance tuning.
 *
 * @opensearch.internal
 */
public class DirectIoConfigs {

    // Prevent instantiation
    private DirectIoConfigs() {
        throw new AssertionError("Utility class - do not instantiate");
    }

    /** 
     * Alignment requirement for Direct I/O operations in bytes.
     * Must be at least 512 bytes or the system page size, whichever is larger.
     */
    public static final int DIRECT_IO_ALIGNMENT = Math.max(512, getPageSizeSafe());

    /** 
     * Power of 2 for Direct I/O write buffer size (2^18 = 256KB).
     */
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

    /** 
     * Power of 2 for cache block size (2^13 = 8KB blocks).
     */
    public static final int CACHE_BLOCK_SIZE_POWER = 13;

    /** 
     * Size of each cache block in bytes (8KB).
     */
    public static final int CACHE_BLOCK_SIZE = 1 << CACHE_BLOCK_SIZE_POWER;

    /** 
     * Bit mask for cache block alignment (block_size - 1).
     */
    public static final long CACHE_BLOCK_MASK = CACHE_BLOCK_SIZE - 1;

    /** 
     * Initial size for cache data structures (64K entries).
     */
    public static final int CACHE_INITIAL_SIZE = 65536;

    /** 
     * Size of the read-ahead operation queue.
     */
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

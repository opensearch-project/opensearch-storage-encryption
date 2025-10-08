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

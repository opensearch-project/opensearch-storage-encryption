/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import org.opensearch.index.store.PanamaNativeAccess;

/**
 * Static configuration constants for the encrypted storage buffer pool and Direct I/O operations.
 *
 * <p>These configurations are intentionally static and immutable, not dynamic settings.
 * They are determined at JVM startup based on system properties and cannot be changed
 * at runtime. This design ensures:
 * <ul>
 *   <li>Consistent behavior across all indices using encrypted storage</li>
 *   <li>Memory allocations and buffer sizes remain stable throughout the JVM lifecycle</li>
 *   <li>Direct I/O alignment requirements are satisfied based on system page size</li>
 *   <li>No runtime overhead from dynamic configuration lookups</li>
 * </ul>
 *
 * <p>If you need to change these values, they must be set via JVM properties or code changes,
 * and require a node restart to take effect.
 */
public class StaticConfigs {

    // Prevent instantiation
    private StaticConfigs() {
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

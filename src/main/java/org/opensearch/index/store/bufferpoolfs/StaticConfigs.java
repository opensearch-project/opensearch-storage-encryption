/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import java.nio.file.Path;

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
     * Default alignment for Direct I/O operations in bytes.
     * This is a safe fallback (512 bytes) used when the filesystem block size
     * cannot be determined. Callers that have a path available should prefer
     * {@link #getDirectIOAlignment(Path)} for the actual filesystem block size.
     */
    public static final int DIRECT_IO_ALIGNMENT = 512;

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
     * Returns the correct Direct I/O alignment for the filesystem containing the given path.
     *
     * <p>Direct I/O requires buffers and offsets to be aligned to the filesystem's logical
     * block size, not the kernel's virtual memory page size. Using the page size (e.g., 4096
     * or 64KB on ARM) instead of the filesystem block size (typically 512 or 4096) can waste
     * memory and cause incorrect alignment on systems with non-standard page sizes.
     *
     * @param path a path on the target filesystem
     * @return the filesystem block size in bytes (guaranteed to be a power of 2)
     */
    public static int getDirectIOAlignment(Path path) {
        return Math.max(DIRECT_IO_ALIGNMENT, PanamaNativeAccess.getFileSystemBlockSize(path));
    }
}

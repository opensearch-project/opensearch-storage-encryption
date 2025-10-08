/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import org.opensearch.index.store.PanamaNativeAccess;

public class DirectIoConfigs {
    public static final int DIRECT_IO_ALIGNMENT = Math.max(512, getPageSizeSafe());
    public static final int DIRECT_IO_WRITE_BUFFER_SIZE_POWER = 18;

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

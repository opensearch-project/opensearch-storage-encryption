/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import org.opensearch.index.store.mmap.PanamaNativeAccess;

public class DirectIoConfigs {
    public static final int DIRECT_IO_ALIGNMENT = Math.max(512, PanamaNativeAccess.getPageSize());
    public static final int INDEX_OUTPUT_BUFFER_SIZE_POWER = 20; // 1mb

    public static final long RESEVERED_POOL_SIZE_IN_BYTES = 32L * 1024 * 1024 * 1024;
    public static final double WARM_UP_PERCENTAGE = 0.2;

    public static final int MMAP_SEGMENT_POWER = 34;

    public static final int CACHE_BLOCK_SIZE_POWER = 13;
    public static final int CACHE_BLOCK_SIZE = 1 << CACHE_BLOCK_SIZE_POWER;
    public static final long CACHE_BLOCK_MASK = CACHE_BLOCK_SIZE - 1;

    public static final long PER_DIR_CACHE_SIZE = 3L * 1024 * 1024 * 1024;
    public static final int CACHE_INITIAL_SIZE = 32768;
    public static final long MAX_CACHE_SIZE = PER_DIR_CACHE_SIZE >> CACHE_BLOCK_SIZE_POWER;
    public static final int BLOCK_EXPIRY_AFTER_ACCESS_MINS = 15;

    public static final int READ_AHEAD_QUEUE_SIZE = 4096;
}

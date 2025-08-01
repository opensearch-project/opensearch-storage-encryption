/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import org.opensearch.index.store.mmap.PanamaNativeAccess;

public class DirectIoConfigs {
    public static final int DIRECT_IO_ALIGNMENT = Math.max(512, PanamaNativeAccess.getPageSize());
    public static final int SEGMENT_SIZE_POWER = 14; // 16kb.
    public static final int SEGMENT_SIZE_BYTES = 1 << SEGMENT_SIZE_POWER;
    public static final int INDEX_OUTPUT_BUFFER_SIZE_POWER = 20; // 1mb
    public static final long RESEVERED_POOL_SIZE_IN_BYTES = 32L * 1024 * 1024 * 1024;
    public static final int PER_DIR_CACHE_SIZE = 256 * 1024 * 1024;
    public static final double WARM_UP_PERCENTAGE = 0.1;
}

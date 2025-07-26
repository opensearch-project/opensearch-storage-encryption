/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import org.opensearch.index.store.mmap.PanamaNativeAccess;

public class DirectIoConfigs {
    public static final int DIRECT_IO_ALIGNMENT = Math.max(512, PanamaNativeAccess.getPageSize());
    public static final int SEGMENT_SIZE_BYTES = 65_536;
    public static final int CHUNK_SIZE_POWER = 16;
    public static final long RESEVERED_POOL_SIZE_IN_BYTES = 5L * 1024 * 1024 * 1024;
    public static final double WARM_UP_PERCENTAGE = 0.2;
}

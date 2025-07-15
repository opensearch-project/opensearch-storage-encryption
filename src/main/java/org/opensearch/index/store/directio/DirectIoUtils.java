/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import java.nio.file.OpenOption;
import java.util.Arrays;

import org.opensearch.index.store.mmap.PanamaNativeAccess;

public class DirectIoUtils {
    public static final int DIRECT_IO_ALIGNMENT = Math.max(512, PanamaNativeAccess.getPageSize());
    public static final int SEGMENT_SIZE_BYTES = 65_536;
    public static final int CHUNK_SIZE_POWER = 16;
    public static final int PER_DIRECTORY_RESEVERED_POOL_SIZE_IN_BYTES = 1024 * 1024 * 1024;
    public static final double WARM_UP_PERCENTAGE = 0.1;
    public static final double SEGMENT_POOL_TO_CACHE_SIZE_RATIO = 0.8;

    private static final OpenOption ExtendedOpenOption_DIRECT; // visible for test

    static {
        OpenOption option;
        try {
            final Class<? extends OpenOption> clazz = Class.forName("com.sun.nio.file.ExtendedOpenOption").asSubclass(OpenOption.class);
            option = Arrays.stream(clazz.getEnumConstants()).filter(e -> e.toString().equalsIgnoreCase("DIRECT")).findFirst().orElse(null);
        } catch (@SuppressWarnings("unused") Exception e) {
            option = null;
        }
        ExtendedOpenOption_DIRECT = option;
    }

    public static OpenOption getDirectOpenOption() {
        if (ExtendedOpenOption_DIRECT == null) {
            throw new UnsupportedOperationException(
                "com.sun.nio.file.ExtendedOpenOption.DIRECT is not available in the current JDK version."
            );
        }
        return ExtendedOpenOption_DIRECT;
    }
}

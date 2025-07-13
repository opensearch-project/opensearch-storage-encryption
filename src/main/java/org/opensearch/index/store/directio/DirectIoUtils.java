/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import java.nio.file.OpenOption;
import java.util.Arrays;

public class DirectIoUtils {
    public static final int DIRECT_IO_ALIGNMENT = 512;
    public static final int MAX_CHUNK_SIZE = 22;

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

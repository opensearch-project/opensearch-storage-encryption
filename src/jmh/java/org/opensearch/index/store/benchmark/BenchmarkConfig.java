/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Shared configuration and utilities for read benchmarks.
 */
public final class BenchmarkConfig {

    private BenchmarkConfig() {}

    /** Deterministic seed for reproducible range generation across runs. */
    public static final long RANGE_SEED = 0xCAFEBABE_DEADBEEFL;

    /**
     * Builds a deterministic byte pattern where each byte depends on its position.
     * Adjacent cache blocks have visibly different content.
     */
    public static byte[] buildDeterministicPattern(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) ((i * 31 + (i >>> 8) * 7 + (i >>> 16) * 13) & 0xFF);
        }
        return data;
    }

    /**
     * Deletes a directory and all its contents recursively.
     */
    public static void deleteRecursively(Path dir) throws IOException {
        if (dir == null || !Files.exists(dir))
            return;
        Files.walk(dir).sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
            try {
                Files.deleteIfExists(p);
            } catch (IOException ignored) {}
        });
    }
}

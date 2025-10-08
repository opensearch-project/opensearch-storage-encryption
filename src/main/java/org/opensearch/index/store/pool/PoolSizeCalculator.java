/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.monitor.os.OsProbe;

/**
 * Utility class for calculating memory pool sizes based on node configuration and available memory.
 */
public final class PoolSizeCalculator {

    private static final Logger LOGGER = LogManager.getLogger(PoolSizeCalculator.class);

    /** Default fallback used when memory calculation fails. */
    private static final long DEFAULT_POOL_SIZE_BYTES = 4L * 1024 * 1024 * 1024;

    /** Minimum pool size (1 GB) to ensure adequate memory allocation. */
    private static final long MIN_POOL_SIZE_BYTES = 1024L * 1024 * 1024;

    private static final long GB_TO_BYTES = 1024L * 1024L * 1024L;
    private static final long MB_TO_BYTES = 1024L * 1024L;

    /**
     * Percentage of estimated off-heap memory to reserve for the pool.
     */
    public static final Setting<Double> NODE_POOL_SIZE_PERCENTAGE_SETTING = Setting
        .doubleSetting("node.store.pool_size_percentage", 0.30, 0.0, 1.0, Property.NodeScope);

    /** Explicit pool size override (in MB). Default: -1 (auto-calculate). Minimum: 64 MB. */
    public static final Setting<Long> NODE_POOL_SIZE_MB_SETTING = Setting
        .longSetting(
            "node.store.pool_size_mb",
            -1L,  // -1 means "auto"
            -1L,
            Long.MAX_VALUE,
            v -> {
                if (v != -1L && v < 64L) {
                    throw new IllegalArgumentException("node.store.pool_size_mb must be -1 (auto) or at least 64 MB, got: " + v);
                }
            },
            Property.NodeScope
        );

    /** Percentage of pool to pre-allocate during warm-up (0.0–1.0). */
    public static final Setting<Double> NODE_POOL_WARMUP_PERCENTAGE_SETTING = Setting
        .doubleSetting("node.store.pool_warmup_percentage", 0.2, 0.0, 1.0, Property.NodeScope);

    public static long calculatePoolSize(Settings settings) {
        long explicitMb = NODE_POOL_SIZE_MB_SETTING.get(settings);
        if (explicitMb != -1L) {
            long bytes = explicitMb * MB_TO_BYTES;
            LOGGER
                .info(
                    "Using explicit pool size: {} MB ({}) GB",
                    explicitMb,
                    String.format(Locale.ROOT, "%.1f", bytes / (double) GB_TO_BYTES)
                );
            return bytes;
        }

        double pct = NODE_POOL_SIZE_PERCENTAGE_SETTING.get(settings);
        long maxHeap = Runtime.getRuntime().maxMemory();

        try {
            long totalPhysical = OsProbe.getInstance().getTotalPhysicalMemorySize();
            long offHeap = Math.max(0, totalPhysical - maxHeap);
            long calc = (long) (offHeap * pct);
            calc = Math.max(MIN_POOL_SIZE_BYTES, calc);
            calc = Math.min(offHeap, calc);
            if (calc == 0)
                calc = DEFAULT_POOL_SIZE_BYTES;

            LOGGER
                .info(
                    String
                        .format(
                            Locale.ROOT,
                            "Calculated pool size: %s (%.1f GB) [total=%.1f GB, heap=%.1f GB, offheap=%.1f GB, pct=%.2f]",
                            toString(calc),
                            calc / (double) GB_TO_BYTES,
                            totalPhysical / (double) GB_TO_BYTES,
                            maxHeap / (double) GB_TO_BYTES,
                            offHeap / (double) GB_TO_BYTES,
                            pct
                        )
                );
            return calc;

        } catch (Exception e) {
            boolean isTest = Boolean.getBoolean("tests.enabled") || "local".equalsIgnoreCase(System.getenv("STAGE"));

            if (isTest) {
                throw new IllegalStateException("Failed to detect total physical memory in test mode", e);
            }

            LOGGER.warn("Failed to detect total physical memory; using default {}", toString(DEFAULT_POOL_SIZE_BYTES), e);
            return DEFAULT_POOL_SIZE_BYTES;
        }
    }

    private static String toString(long bytes) {
        return String.format(Locale.ROOT, "%,d bytes", bytes);
    }

    private PoolSizeCalculator() {}
}

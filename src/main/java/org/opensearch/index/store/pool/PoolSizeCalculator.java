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

    /** Minimum pool size: 256 MB */
    private static final long MIN_POOL_SIZE_MB = 256;

    /**
     * Percentage of off-heap memory to use for the pool.
     */
    public static final Setting<Double> NODE_POOL_SIZE_PERCENTAGE_SETTING = Setting
        .doubleSetting("node.store.crypto.pool_size_percentage", 0.3, 0.0, 1.0, Property.NodeScope);

    /**
     * Ratio of pool size to cache size.
     */
    public static final Setting<Double> NODE_POOL_TO_CACHE_RATIO_SETTING = Setting
        .doubleSetting("node.store.crypto.pool_to_cache_ratio", 1.5, 1.0, 10.0, Property.NodeScope);

    /**
     * Percentage of cache blocks to warmup at initialization.
     * Default is 0.2 (20% of blocks pre-allocated).
     */
    public static final Setting<Double> NODE_WARMUP_PERCENTAGE_SETTING = Setting
        .doubleSetting("node.store.crypto.warmup_percentage", 0.2, 0.0, 1.0, Property.NodeScope);

    private static final long MB_TO_BYTES = 1024L * 1024L;
    private static final long GB_TO_BYTES = 1024L * 1024L * 1024L;

    /**
     * Calculates the pool size based on off-heap memory.
     *
     * pool_size = (totalPhysicalMemory - maxHeap) * pool_size_percentage
     *
     * @param settings the node settings for configuration
     * @return the calculated pool size in bytes
     * @throws RuntimeException if physical memory cannot be detected
     */
    public static long calculatePoolSize(Settings settings) {
        double percentage = NODE_POOL_SIZE_PERCENTAGE_SETTING.get(settings);

        long maxHeap = Runtime.getRuntime().maxMemory();
        long totalPhysical;
        try {
            totalPhysical = OsProbe.getInstance().getTotalPhysicalMemorySize();
        } catch (Exception e) {
            throw new RuntimeException("Failed to detect total physical memory for pool size calculation", e);
        }

        if (totalPhysical <= 0) {
            throw new RuntimeException("Invalid total physical memory detected: " + totalPhysical);
        }

        long offHeap = Math.max(0, totalPhysical - maxHeap);
        long calculated = (long) (offHeap * percentage);

        // Apply minimum bound
        long minBytes = MIN_POOL_SIZE_MB * MB_TO_BYTES;
        calculated = Math.max(minBytes, calculated);

        LOGGER
            .info(
                String
                    .format(
                        Locale.ROOT,
                        "Calculated pool size = %d MB (%.1f GB) [total=%.1f GB, heap=%.1f GB, offheap=%.1f GB, percentage=%.1f%%]",
                        calculated / MB_TO_BYTES,
                        calculated / (double) GB_TO_BYTES,
                        totalPhysical / (double) GB_TO_BYTES,
                        maxHeap / (double) GB_TO_BYTES,
                        offHeap / (double) GB_TO_BYTES,
                        percentage * 100
                    )
            );
        return calculated;
    }

    private PoolSizeCalculator() {}
}

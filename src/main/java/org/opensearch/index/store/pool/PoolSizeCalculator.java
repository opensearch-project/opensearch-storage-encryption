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
     * Ratio of cache size to pool size.
     * cache_size = pool_size * ratio
     * This setting can be overridden, but if not set, automatic tiering based on off-heap size applies.
     */
    public static final Setting<Double> NODE_CACHE_TO_POOL_RATIO_SETTING = Setting
        .doubleSetting("node.store.crypto.cache_to_pool_ratio", 0.75, 0.1, 1.0, Property.NodeScope);

    /** Threshold for small instance: 10 GB off-heap memory */
    private static final long SMALL_INSTANCE_THRESHOLD_GB = 10;
    private static final long MEDIUM_INSTANCE_THRESHOLD_GB = 32;

    /**
     * Percentage of cache blocks to warmup at initialization.
     */
    public static final Setting<Double> NODE_WARMUP_PERCENTAGE_SETTING = Setting
        .doubleSetting("node.store.crypto.warmup_percentage", 0.05, 0.0, 1.0, Property.NodeScope);

    private static final long MB_TO_BYTES = 1024L * 1024L;
    private static final long GB_TO_BYTES = 1024L * 1024L * 1024L;

    /**
     * Calculates the cache-to-pool ratio based on off-heap memory size.
     *
     * @param offHeapBytes the available off-heap memory in bytes
     * @param settings the node settings for configuration
     * @return the calculated cache-to-pool ratio
     */
    public static double calculateCacheToPoolRatio(long offHeapBytes, Settings settings) {
        if (settings.hasValue(NODE_CACHE_TO_POOL_RATIO_SETTING.getKey())) {
            return NODE_CACHE_TO_POOL_RATIO_SETTING.get(settings);
        }

        // Apply tiered ratio based on off-heap size
        long offHeapGB = offHeapBytes / GB_TO_BYTES;
        if (offHeapGB < SMALL_INSTANCE_THRESHOLD_GB) {
            double lowCacheToPoolRatio = 0.5;
            LOGGER
                .info(
                    "Instance with low offheap (off-heap={} GB < {} GB), using reduced {} cache-to-pool ratio",
                    offHeapGB,
                    SMALL_INSTANCE_THRESHOLD_GB
                );
            return lowCacheToPoolRatio;
        }

        // Default ratio for large instances - query from setting default
        return NODE_CACHE_TO_POOL_RATIO_SETTING.get(settings);
    }

    /**
     * Calculates the warmup percentage based on off-heap memory size.
     *
     * @param offHeapBytes the available off-heap memory in bytes
     * @param settings the node settings for configuration
     * @return the calculated warmup percentage
     */
    public static double calculateWarmupPercentage(long offHeapBytes, Settings settings) {
        // Check if user has explicitly set the warmup percentage
        if (settings.hasValue(NODE_WARMUP_PERCENTAGE_SETTING.getKey())) {
            return NODE_WARMUP_PERCENTAGE_SETTING.get(settings);
        }

        long offHeapGB = offHeapBytes / GB_TO_BYTES;
        if (offHeapGB < MEDIUM_INSTANCE_THRESHOLD_GB) {
            LOGGER
                .info(
                    "Instance with low offheap (off-heap={} GB < {} GB), disabling warmup (0%) to reduce initial memory pressure",
                    offHeapGB,
                    MEDIUM_INSTANCE_THRESHOLD_GB
                );
            return 0.0;
        }

        // Default warmup for large instances - query from setting default
        return NODE_WARMUP_PERCENTAGE_SETTING.get(settings);
    }

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

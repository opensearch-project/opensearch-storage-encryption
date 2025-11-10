/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.pool.SegmentType;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

import lombok.NonNull;

/**
 * Registry for crypto operations metrics and tracing.
 * Provides centralized access to telemetry data collection for encryption/decryption operations.
 */
public class CryptoMetricsService {
    private static final Logger LOGGER = LogManager.getLogger(CryptoMetricsService.class);

    private static volatile CryptoMetricsService instance;
    private final MetricsRegistry metricsRegistry;
    private final Histogram poolStatsHistogram;
    private final Histogram cacheStatsHistogram;
    private final Counter errorCounter;

    // Metric names
    private static final String POOL_STATS_NAME = "crypto.pool.stats";
    private static final String CACHE_STATS_NAME = "crypto.cache.stats";
    private static final String ERROR_COUNTER_NAME = "crypto.error.total";

    // Metric descriptions
    private static final String POOL_STATS_DESC = "Crypto Pool statistics";
    private static final String CACHE_STATS_DESC = "Crypto Cache statistics";
    private static final String ERROR_COUNTER_DESC = "Total crypto operation errors";

    // Units
    private static final String COUNT_UNIT = "count";

    // Tag names
    private static final String ERROR_TYPE_TAG = "error_type";
    private static final String STAT_TYPE_TAG = "stat_type";
    private static final String INDEX_NAME = "index_name";

    // Error message
    private static final String NOT_INITIALIZED_ERROR = "CryptoMetricsRegistry not initialized.";

    /**
     * Private constructor for singleton pattern.
     * @param metricsRegistry the metrics registry for collecting metrics
     */
    private CryptoMetricsService(@NonNull MetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
        this.errorCounter = createCounter(ERROR_COUNTER_NAME, ERROR_COUNTER_DESC, COUNT_UNIT);
        this.poolStatsHistogram = createHistogram(POOL_STATS_NAME, POOL_STATS_DESC, COUNT_UNIT);
        this.cacheStatsHistogram = createHistogram(CACHE_STATS_NAME, CACHE_STATS_DESC, COUNT_UNIT);
    }

    /**
     * Initializes the singleton instance.
     * @param metricsRegistry the metrics registry for collecting metrics
     */
    public static synchronized void initialize(@NonNull MetricsRegistry metricsRegistry) {
        if (instance == null) {
            instance = new CryptoMetricsService(metricsRegistry);
        }
    }

    /**
     * Gets the singleton instance.
     * @return the CryptoMetricsRegistry instance
     * @throws IllegalStateException if not initialized
     */
    public static CryptoMetricsService getInstance() {
        if (instance == null) {
            throw new IllegalStateException(NOT_INITIALIZED_ERROR);
        }
        return instance;
    }

    /**
     * Records pool statistics as separate time series.
     * @param maxSegments maximum segments
     * @param allocated allocated segments
     * @param free free segments
     * @param utilization utilization percentage (0-100)
     * @param allocation allocation percentage (0-100)
     */
    public void recordPoolStats(SegmentType segmentType, int maxSegments, int allocated, int free, double utilization, double allocation) {
        LOGGER.info("Publishing pool stats metric");
        if (poolStatsHistogram == null)
            return;

        Tags baseTags = Tags.create().addTag("segment_type", segmentType.getValue());
        poolStatsHistogram.record(maxSegments, baseTags.addTag(STAT_TYPE_TAG, "max"));
        poolStatsHistogram.record(allocated, baseTags.addTag(STAT_TYPE_TAG, "allocated"));
        poolStatsHistogram.record(free, baseTags.addTag(STAT_TYPE_TAG, "free"));
        poolStatsHistogram.record(utilization, baseTags.addTag(STAT_TYPE_TAG, "utilization"));
        poolStatsHistogram.record(allocation, baseTags.addTag(STAT_TYPE_TAG, "allocation"));
    }

    /**
     * Records cache statistics as separate time series.
     * @param size cache size
     * @param hits hit count
     * @param misses miss count
     * @param hitRate hit rate percentage (0-100)
     * @param loads load count
     * @param evictions eviction count
     * @param avgLoadTimeMs average load time in milliseconds
     */
    public void recordCacheStats(long size, long hits, long misses, double hitRate, long loads, long evictions, double avgLoadTimeMs) {
        LOGGER.info("Publishing cache stats metric");
        if (cacheStatsHistogram == null)
            return;

        cacheStatsHistogram.record(size, Tags.create().addTag(STAT_TYPE_TAG, "size"));
        cacheStatsHistogram.record(hits, Tags.create().addTag(STAT_TYPE_TAG, "hits"));
        cacheStatsHistogram.record(misses, Tags.create().addTag(STAT_TYPE_TAG, "misses"));
        cacheStatsHistogram.record(hitRate, Tags.create().addTag(STAT_TYPE_TAG, "hit_rate"));
        cacheStatsHistogram.record(loads, Tags.create().addTag(STAT_TYPE_TAG, "loads"));
        cacheStatsHistogram.record(evictions, Tags.create().addTag(STAT_TYPE_TAG, "evictions"));
        cacheStatsHistogram.record(avgLoadTimeMs, Tags.create().addTag(STAT_TYPE_TAG, "avg_load_time"));
    }

    /**
     * Records error count by error type.
     * @param errorType the type of error
     */
    public void recordError(@NonNull ErrorType errorType) {
        LOGGER.info("Publishing error metric");
        if (errorCounter != null) {
            errorCounter.add(1.0, Tags.create().addTag(ERROR_TYPE_TAG, errorType.getValue()));
        }
    }

    /**
     * Records error count by error type at index level.
     * @param errorType the type of error
     * @param indexName the index name
     */
    public void recordError(@NonNull ErrorType errorType, @NonNull String indexName) {
        LOGGER.info("Publishing error metric for index: {}", indexName);
        if (errorCounter != null) {
            errorCounter.add(1.0, Tags.create().addTag(ERROR_TYPE_TAG, errorType.getValue()).addTag(INDEX_NAME, indexName));
        }
    }

    // Private helper methods
    private Counter createCounter(String name, String description, String unit) {
        return metricsRegistry != null ? metricsRegistry.createCounter(name, description, unit) : null;
    }

    private Histogram createHistogram(String name, String description, String unit) {
        return metricsRegistry != null ? metricsRegistry.createHistogram(name, description, unit) : null;
    }

}

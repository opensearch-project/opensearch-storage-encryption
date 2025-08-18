/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.metrics;

import software.amazon.cloudwatchlogs.emf.exception.DimensionSetExceededException;
import software.amazon.cloudwatchlogs.emf.exception.InvalidDimensionException;
import software.amazon.cloudwatchlogs.emf.logger.MetricsLogger;
import software.amazon.cloudwatchlogs.emf.model.DimensionSet;
import software.amazon.cloudwatchlogs.emf.model.Unit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

import org.opensearch.common.Randomness;

import java.util.Map;

public class CryptoMetricsLogger {
    private static volatile String NAMESPACE = "OpenSearch/StorageEncryption";
    private static final Logger logger = LogManager.getLogger(CryptoMetricsLogger.class);
    private static final Map<String, DimensionSet> DIMENSION_CACHE = new ConcurrentHashMap<>();
    private static final int BATCH_SIZE = 10;
    private static volatile double SAMPLING_RATE = 1.0;

    private static final ThreadLocal<MetricsLogger> THREAD_LOCAL_LOGGER =
            ThreadLocal.withInitial(() -> {
                try {
                    MetricsLogger ml = new MetricsLogger();
                    ml.setNamespace(NAMESPACE);
                    return ml;
                } catch (Exception e) {
                    logger.warn("Failed to create MetricsLogger for thread: {}", Thread.currentThread().getName(), e);
                    return null;
                }
            });

    private static final ThreadLocal<Integer> BATCH_COUNTER = ThreadLocal.withInitial(() -> 0);

    public static class MetricsContext {
        private final String operation;
        private final String directoryType;

        public MetricsContext(String operation, String directoryType) {
            this.operation = operation;
            this.directoryType = directoryType;
        }

        public DimensionSet toDimensionSet() throws InvalidDimensionException, DimensionSetExceededException {
            String key = operation + ":" + directoryType;
            return DIMENSION_CACHE.computeIfAbsent(key, k -> {
                try {
                    return DimensionSet.of("Operation", operation, "DirectoryType", directoryType);
                } catch (InvalidDimensionException | DimensionSetExceededException e) {
                    logger.warn("Failed to create DimensionSet for key: {}", k, e);
                    return null;
                }
            });
        }
    }

    private CryptoMetricsLogger() {
        // Private constructor to prevent instantiation
    }

    private static MetricsLogger getMetricsLogger() {
        return THREAD_LOCAL_LOGGER.get();
    }

    public static void recordBytes(String metricName, long bytes, MetricsContext context) {
        try {
            recordMetric(metricName, bytes, Unit.BYTES, context.toDimensionSet());
        } catch (Exception e) {
            logger.warn("Failed to record bytes metric: {}", metricName, e);
        }
    }

    public static void recordCount(String metricName, long count, MetricsContext context) {
        try {
            recordMetric(metricName, count, Unit.COUNT, context.toDimensionSet());
        } catch (Exception e) {
            logger.warn("Failed to record count metric: {}", metricName, e);
        }
    }

    public static void recordRate(String metricName, double rate, Unit unit, MetricsContext context) {
        try {
            recordMetric(metricName, rate, unit, context.toDimensionSet());
        } catch (Exception e) {
            logger.warn("Failed to record rate metric: {}", metricName, e);
        }
    }

    public static void setSamplingRate(double samplingRate) {
        SAMPLING_RATE = Math.max(0.0, Math.min(1.0, samplingRate));
    }
    
    public static void setNamespace(String namespace) {
        if(!namespace.isBlank()) {
            NAMESPACE = namespace;
        }
    }


    private static void recordMetric(String metricName, Number value, Unit unit, DimensionSet dimensions) {
        try {
            // Apply sampling
            if (Randomness.get().nextDouble() > SAMPLING_RATE) {
                return;
            }

            MetricsLogger metrics = getMetricsLogger();
            if (metrics != null) {
                metrics.putDimensions(dimensions);
                metrics.putMetric(metricName, value.doubleValue(), unit);

                int count = BATCH_COUNTER.get() + 1;
                BATCH_COUNTER.set(count);

                if (count >= BATCH_SIZE) {
                    metrics.flush();
                    BATCH_COUNTER.set(0);
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to record metric: {}", metricName, e);
        }
    }
}
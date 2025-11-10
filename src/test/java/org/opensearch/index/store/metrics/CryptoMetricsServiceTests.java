/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.metrics;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;

import org.junit.After;
import org.junit.Before;
import org.opensearch.index.store.pool.SegmentType;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.test.OpenSearchTestCase;

public class CryptoMetricsServiceTests extends OpenSearchTestCase {

    private MetricsRegistry mockMetricsRegistry;
    private Histogram mockPoolHistogram;
    private Histogram mockCacheHistogram;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        resetSingleton();

        mockMetricsRegistry = mock(MetricsRegistry.class);
        mockPoolHistogram = mock(Histogram.class);
        mockCacheHistogram = mock(Histogram.class);

        when(mockMetricsRegistry.createCounter(any(), any(), any())).thenReturn(mock(Counter.class));
        when(mockMetricsRegistry.createHistogram(eq("crypto.pool.stats"), any(), any())).thenReturn(mockPoolHistogram);
        when(mockMetricsRegistry.createHistogram(eq("crypto.cache.stats"), any(), any())).thenReturn(mockCacheHistogram);
    }

    @After
    public void tearDown() throws Exception {
        resetSingleton();
        super.tearDown();
    }

    private void resetSingleton() throws Exception {
        Field instanceField = CryptoMetricsService.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        instanceField.set(null, null);
    }

    public void testRecordPoolStats() {
        CryptoMetricsService.initialize(mockMetricsRegistry);
        CryptoMetricsService service = CryptoMetricsService.getInstance();

        service.recordPoolStats(SegmentType.PRIMARY, 100, 80, 20, 80.0, 75.0);

        verify(mockPoolHistogram, times(5)).record(any(Double.class), any(Tags.class));
    }

    public void testRecordCacheStats() {
        CryptoMetricsService.initialize(mockMetricsRegistry);
        CryptoMetricsService service = CryptoMetricsService.getInstance();

        service.recordCacheStats(1000L, 800L, 200L, 80.0, 1000L, 50L, 15.5);

        verify(mockCacheHistogram, times(7)).record(any(Double.class), any(Tags.class));
    }
}

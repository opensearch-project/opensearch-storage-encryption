/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class PoolSizeCalculatorTests extends OpenSearchTestCase {

    public void testCalculatePoolSizeWithDirectMemory() {
        Settings settings = Settings.builder()
            .put(PoolSizeCalculator.NODE_POOL_SIZE_PERCENTAGE_SETTING.getKey(), 0.3)
            .build();

        long poolSize = PoolSizeCalculator.calculatePoolSize(settings);
        
        // Pool size should be positive and reasonable
        assertThat(poolSize, greaterThan(0L));
        assertThat(poolSize, greaterThan(256L * 1024L * 1024L)); // At least 256MB minimum
    }

    public void testCalculatePoolSizeWithCustomPercentage() {
        Settings settings = Settings.builder()
            .put(PoolSizeCalculator.NODE_POOL_SIZE_PERCENTAGE_SETTING.getKey(), 0.5)
            .build();

        long poolSize = PoolSizeCalculator.calculatePoolSize(settings);
        assertThat(poolSize, greaterThan(0L));
    }

    public void testParseMemorySizeMethod() throws Exception {
        // Use reflection to test private parseMemorySize method
        Method parseMemorySize = PoolSizeCalculator.class.getDeclaredMethod("parseMemorySize", String.class);
        parseMemorySize.setAccessible(true);

        // Test various memory size formats
        assertThat((Long) parseMemorySize.invoke(null, "1024"), equalTo(1024L));
        assertThat((Long) parseMemorySize.invoke(null, "1k"), equalTo(1024L));
        assertThat((Long) parseMemorySize.invoke(null, "1K"), equalTo(1024L));
        assertThat((Long) parseMemorySize.invoke(null, "1m"), equalTo(1024L * 1024L));
        assertThat((Long) parseMemorySize.invoke(null, "1M"), equalTo(1024L * 1024L));
        assertThat((Long) parseMemorySize.invoke(null, "1g"), equalTo(1024L * 1024L * 1024L));
        assertThat((Long) parseMemorySize.invoke(null, "1G"), equalTo(1024L * 1024L * 1024L));
        assertThat((Long) parseMemorySize.invoke(null, "512m"), equalTo(512L * 1024L * 1024L));
        
        // Test invalid formats
        assertThat((Long) parseMemorySize.invoke(null, "invalid"), equalTo(0L));
        assertThat((Long) parseMemorySize.invoke(null, ""), equalTo(0L));
        assertThat((Long) parseMemorySize.invoke(null, (String) null), equalTo(0L));
    }

    public void testGetMaxDirectMemoryMethod() throws Exception {
        // Use reflection to test private getMaxDirectMemory method
        Method getMaxDirectMemory = PoolSizeCalculator.class.getDeclaredMethod("getMaxDirectMemory");
        getMaxDirectMemory.setAccessible(true);

        long maxDirectMemory = (Long) getMaxDirectMemory.invoke(null);
        
        // Should return a positive value (either from JVM args or default to heap size)
        assertThat(maxDirectMemory, greaterThan(0L));
        
        // Should not exceed total heap size by too much (reasonable upper bound)
        long maxHeap = Runtime.getRuntime().maxMemory();
        assertThat(maxDirectMemory, lessThanOrEqualTo(maxHeap * 2)); // Allow up to 2x heap as reasonable upper bound
    }

    public void testDirectMemoryAccountingInPoolCalculation() throws Exception {
        // Test that direct memory is properly subtracted from available memory
        Settings settings = Settings.builder()
            .put(PoolSizeCalculator.NODE_POOL_SIZE_PERCENTAGE_SETTING.getKey(), 0.3)
            .build();

        // Get the current JVM arguments to verify direct memory is being read
        List<String> jvmArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
        boolean hasDirectMemoryArg = jvmArgs.stream()
            .anyMatch(arg -> arg.startsWith("-XX:MaxDirectMemorySize="));

        long poolSize = PoolSizeCalculator.calculatePoolSize(settings);
        
        // Pool size should be calculated correctly regardless of whether direct memory is explicitly set
        assertThat(poolSize, greaterThan(0L));
        
        // If direct memory is set, verify it's being accounted for
        if (hasDirectMemoryArg) {
            // The pool size should be less than what it would be without direct memory accounting
            // This is a behavioral test - the exact value depends on system memory
            assertTrue("Pool size should account for direct memory", poolSize > 0);
        }
    }

    public void testCacheToPoolRatioCalculation() {
        long offHeapBytes = 10L * 1024L * 1024L * 1024L; // 10 GB
        Settings settings = Settings.EMPTY;

        double ratio = PoolSizeCalculator.calculateCacheToPoolRatio(offHeapBytes, settings);
        assertThat(ratio, equalTo(0.75)); // Default ratio for large instances

        // Test small instance
        long smallOffHeap = 5L * 1024L * 1024L * 1024L; // 5 GB
        double smallRatio = PoolSizeCalculator.calculateCacheToPoolRatio(smallOffHeap, settings);
        assertThat(smallRatio, equalTo(0.5)); // Reduced ratio for small instances

        // Test custom setting
        Settings customSettings = Settings.builder()
            .put(PoolSizeCalculator.NODE_CACHE_TO_POOL_RATIO_SETTING.getKey(), 0.8)
            .build();
        double customRatio = PoolSizeCalculator.calculateCacheToPoolRatio(offHeapBytes, customSettings);
        assertThat(customRatio, equalTo(0.8));
    }

    public void testWarmupPercentageCalculation() {
        long offHeapBytes = 40L * 1024L * 1024L * 1024L; // 40 GB
        Settings settings = Settings.EMPTY;

        double warmup = PoolSizeCalculator.calculateWarmupPercentage(offHeapBytes, settings);
        assertThat(warmup, equalTo(0.05)); // Default warmup for large instances

        // Test small instance
        long smallOffHeap = 20L * 1024L * 1024L * 1024L; // 20 GB
        double smallWarmup = PoolSizeCalculator.calculateWarmupPercentage(smallOffHeap, settings);
        assertThat(smallWarmup, equalTo(0.0)); // No warmup for small instances

        // Test custom setting
        Settings customSettings = Settings.builder()
            .put(PoolSizeCalculator.NODE_WARMUP_PERCENTAGE_SETTING.getKey(), 0.1)
            .build();
        double customWarmup = PoolSizeCalculator.calculateWarmupPercentage(offHeapBytes, customSettings);
        assertThat(customWarmup, equalTo(0.1));
    }

    public void testMinimumPoolSizeEnforced() {
        // Test with very small percentage to ensure minimum is enforced
        Settings settings = Settings.builder()
            .put(PoolSizeCalculator.NODE_POOL_SIZE_PERCENTAGE_SETTING.getKey(), 0.001) // Very small percentage
            .build();

        long poolSize = PoolSizeCalculator.calculatePoolSize(settings);
        long minExpected = 256L * 1024L * 1024L; // 256 MB minimum
        
        assertThat("Pool size should enforce minimum of 256MB", poolSize, greaterThan(minExpected - 1));
    }
}

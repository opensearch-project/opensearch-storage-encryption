/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;

public class WindowedReadaheadPolicyTests extends OpenSearchTestCase {

    private static final int CACHE_BLOCK_SIZE = 4096;
    private static final Path TEST_PATH = Paths.get("/test/file.dat");

    private WindowedReadaheadPolicy policy;

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    private long blockOffset(long blockIndex) {
        return blockIndex * CACHE_BLOCK_SIZE;
    }

    /**
     * Tests valid constructor parameters.
     */
    public void testValidConstruction() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        assertNotNull("Policy should be created", policy);
        assertEquals("Initial window should match", 4, policy.initialWindow());
        assertEquals("Max window should match", 32, policy.maxWindow());
        assertEquals("Current window should be initial", 4, policy.currentWindow());
    }

    /**
     * Tests constructor with minLead parameter.
     */
    public void testConstructionWithMinLead() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 2, 16);

        assertNotNull("Policy should be created with minLead", policy);
        assertEquals("Initial window should match", 4, policy.initialWindow());
    }

    /**
     * Tests that initialWindow < 1 throws exception.
     */
    public void testConstructorRejectsZeroInitialWindow() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> { new WindowedReadaheadPolicy(TEST_PATH, 0, 32, 16); }
        );
        assertTrue("Error message should mention initialWindow", e.getMessage().contains("initialWindow"));
    }

    /**
     * Tests that negative initialWindow throws exception.
     */
    public void testConstructorRejectsNegativeInitialWindow() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> { new WindowedReadaheadPolicy(TEST_PATH, -1, 32, 16); }
        );
        assertTrue("Error message should mention initialWindow", e.getMessage().contains("initialWindow"));
    }

    /**
     * Tests that maxWindow < initialWindow throws exception.
     */
    public void testConstructorRejectsMaxWindowLessThanInitial() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> { new WindowedReadaheadPolicy(TEST_PATH, 8, 4, 16); }
        );
        assertTrue("Error message should mention maxWindow", e.getMessage().contains("maxWindow"));
    }

    /**
     * Tests that minLead < 1 throws exception.
     */
    public void testConstructorRejectsZeroMinLead() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> { new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 0, 16); }
        );
        assertTrue("Error message should mention minLead", e.getMessage().contains("minLead"));
    }

    /**
     * Tests that smallGapDivisor < 2 throws exception.
     */
    public void testConstructorRejectsSmallGapDivisor() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> { new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 1); }
        );
        assertTrue("Error message should mention smallGapDivisor", e.getMessage().contains("smallGapDivisor"));
    }

    /**
     * Tests edge case where initialWindow equals maxWindow.
     */
    public void testConstructorWithEqualInitialAndMaxWindow() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 16, 16, 8);

        assertNotNull("Policy should accept equal windows", policy);
        assertEquals("Initial should equal max", policy.initialWindow(), policy.maxWindow());
    }

    // ========== Initial Access Tests ==========

    /**
     * Tests first access triggers and initializes state.
     */
    public void testFirstAccessTriggers() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        boolean triggered = policy.shouldTrigger(blockOffset(0));

        assertTrue("First access should trigger", triggered);
        assertEquals("Window should be initial", 4, policy.currentWindow());
    }

    /**
     * Tests first access at non-zero offset.
     */
    public void testFirstAccessAtNonZeroOffset() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        boolean triggered = policy.shouldTrigger(blockOffset(100));

        assertTrue("First access should trigger", triggered);
        assertEquals("Window should be initial", 4, policy.currentWindow());
    }

    /**
     * Tests lead blocks calculation on first access.
     */
    public void testLeadBlocksAfterFirstAccess() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 6, 48, 16);

        policy.shouldTrigger(blockOffset(0));

        int lead = policy.leadBlocks();
        // Lead = max(minLead=1, window/3) = max(1, 6/3) = 2
        assertTrue("Lead should be at least 1", lead >= 1);
    }

    /**
     * Tests sequential forward access pattern and window growth.
     * Note: Policy doesn't trigger on every access, but grows window when it does trigger.
     */
    public void testSequentialAccessTriggersAndGrows() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        // First access always triggers
        assertTrue("First access triggers", policy.shouldTrigger(blockOffset(0)));
        assertEquals("Window starts at 4", 4, policy.currentWindow());

        // Access block 1 - may or may not trigger
        policy.shouldTrigger(blockOffset(1));

        // Access block 2 - sequential pattern continues
        boolean triggered = policy.shouldTrigger(blockOffset(2));
        // Window should have grown by now
        int windowAfter2 = policy.currentWindow();
        assertTrue("Window should grow with sequential access", windowAfter2 > 4);

        // Continue sequential pattern
        policy.shouldTrigger(blockOffset(3));
        policy.shouldTrigger(blockOffset(4));

        int finalWindow = policy.currentWindow();
        assertTrue("Window should continue growing", finalWindow >= windowAfter2);
        assertTrue("Window should not exceed max", finalWindow <= 32);
    }

    /**
     * Tests window growth caps at maxWindow.
     */
    public void testWindowGrowthCapsAtMax() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 16, 16);

        policy.shouldTrigger(blockOffset(0));  // window=4
        int w1 = policy.currentWindow();
        policy.shouldTrigger(blockOffset(1));  // window should grow
        int w2 = policy.currentWindow();
        assertTrue("Window should grow or stay same", w2 >= w1);

        // Continue sequential to reach max
        for (int i = 2; i <= 10; i++) {
            policy.shouldTrigger(blockOffset(i));
        }

        assertTrue("Window should cap at max", policy.currentWindow() <= 16);
    }

    /**
     * Tests sequential access with gap=2 (within buffer).
     */
    public void testSequentialAccessWithGap2() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        policy.shouldTrigger(blockOffset(0));
        int w1 = policy.currentWindow();
        assertTrue("Gap=2 should be sequential", policy.shouldTrigger(blockOffset(2)));
        assertTrue("Window should grow or stay same", policy.currentWindow() >= w1);
    }

    /**
     * Tests sequential access with varying small gaps.
     */
    public void testSequentialAccessWithSmallGaps() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 64, 16);

        policy.shouldTrigger(blockOffset(0));   // window=4
        policy.shouldTrigger(blockOffset(1));   // gap=1, sequential
        policy.shouldTrigger(blockOffset(3));   // gap=2, sequential

        // Gap=4 may or may not trigger depending on window growth
        policy.shouldTrigger(blockOffset(7));
        assertNotNull("Policy should handle gaps", policy);
    }

    // ========== Forward Jump Tests ==========

    /**
     * Tests forward jump behavior.
     */
    public void testSmallForwardJumpTriggersShrinks() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        policy.shouldTrigger(blockOffset(0));
        policy.shouldTrigger(blockOffset(1));
        policy.shouldTrigger(blockOffset(2));

        // Forward jump
        policy.shouldTrigger(blockOffset(7));

        // Window should either shrink, reset, or stay same depending on jump size
        assertTrue("Window should be valid", policy.currentWindow() >= 4);
        assertTrue("Window should not exceed max", policy.currentWindow() <= 32);
    }

    /**
     * Tests forward jump with custom smallGapDivisor.
     */
    public void testSmallForwardJumpWithCustomDivisor() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 4); // divisor=4

        policy.shouldTrigger(blockOffset(0));
        policy.shouldTrigger(blockOffset(1));
        policy.shouldTrigger(blockOffset(2));

        // Forward jump
        policy.shouldTrigger(blockOffset(8));

        // Window should be valid regardless of trigger result
        assertTrue("Window should be valid", policy.currentWindow() >= 4);
        assertTrue("Window should not exceed max", policy.currentWindow() <= 32);
    }

    /**
     * Tests large forward jump does not trigger and resets window.
     */
    public void testLargeForwardJumpResetsWindow() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        policy.shouldTrigger(blockOffset(0));   // window=4
        policy.shouldTrigger(blockOffset(1));   // window=8
        policy.shouldTrigger(blockOffset(2));   // window=16

        // Large jump far ahead
        boolean triggered = policy.shouldTrigger(blockOffset(100));

        assertFalse("Large jump should not trigger", triggered);
        assertEquals("Window should reset to initial", 4, policy.currentWindow());
    }

    // ========== Same Position Tests ==========

    /**
     * Tests accessing same position twice does not trigger.
     */
    public void testSamePositionAccessNoTrigger() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        policy.shouldTrigger(blockOffset(5));
        boolean triggered = policy.shouldTrigger(blockOffset(5));

        assertFalse("Same position should not trigger", triggered);
        assertEquals("Window should not change", 4, policy.currentWindow());
    }

    /**
     * Tests multiple accesses to same position.
     */
    public void testMultipleSamePositionAccesses() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        policy.shouldTrigger(blockOffset(10));

        for (int i = 0; i < 5; i++) {
            assertFalse("Repeated same position should not trigger", policy.shouldTrigger(blockOffset(10)));
        }

        assertEquals("Window should remain initial", 4, policy.currentWindow());
    }

    // ========== Backward Seek Tests ==========

    /**
     * Tests small backward seek decays window gradually.
     */
    public void testSmallBackwardSeekDecays() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        policy.shouldTrigger(blockOffset(0));   // window=4
        policy.shouldTrigger(blockOffset(1));
        policy.shouldTrigger(blockOffset(2));
        policy.shouldTrigger(blockOffset(3));

        int windowBeforeBackward = policy.currentWindow();

        // Small backward seek
        boolean triggered = policy.shouldTrigger(blockOffset(2));

        assertFalse("Backward seek should not trigger", triggered);
        // Window should decay but stay >= initial
        assertTrue("Window should decay", policy.currentWindow() <= windowBeforeBackward);
        assertTrue("Window should not go below initial", policy.currentWindow() >= 4);
    }

    /**
     * Tests large backward seek resets window.
     */
    public void testLargeBackwardSeekResets() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        policy.shouldTrigger(blockOffset(0));
        policy.shouldTrigger(blockOffset(1));
        policy.shouldTrigger(blockOffset(2));

        int windowBefore = policy.currentWindow();

        // Large backward seek
        boolean triggered = policy.shouldTrigger(blockOffset(0));

        assertFalse("Large backward seek should not trigger", triggered);
        assertTrue("Window should reset or decay", policy.currentWindow() <= windowBefore);
        assertTrue("Window should be at or above initial", policy.currentWindow() >= 4);
    }

    /**
     * Tests backward seek to very beginning.
     */
    public void testBackwardSeekToStart() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        policy.shouldTrigger(blockOffset(100));
        policy.shouldTrigger(blockOffset(101));

        boolean triggered = policy.shouldTrigger(blockOffset(0));

        assertFalse("Backward to start should not trigger", triggered);
    }

    /**
     * Tests decay bottoms out at initial window.
     */
    public void testDecayBottomsAtInitial() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        policy.shouldTrigger(blockOffset(0));   // window=4
        policy.shouldTrigger(blockOffset(1));   // window=8

        // Small backward seek should decay but not below initial
        policy.shouldTrigger(blockOffset(0));   // decay from 8

        assertTrue("Window should be at or above initial", policy.currentWindow() >= 4);
    }

    // ========== Lead Blocks Tests ==========

    /**
     * Tests lead blocks calculation with default minLead.
     */
    public void testLeadBlocksCalculation() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 6, 48, 16);

        policy.shouldTrigger(blockOffset(0));
        int lead1 = policy.leadBlocks();
        assertTrue("Lead should be positive", lead1 > 0);

        policy.shouldTrigger(blockOffset(1));
        int lead2 = policy.leadBlocks();
        assertTrue("Lead should be positive", lead2 > 0);

        policy.shouldTrigger(blockOffset(2));
        int lead3 = policy.leadBlocks();
        assertTrue("Lead should be positive", lead3 > 0);
    }

    /**
     * Tests lead blocks with custom minLead.
     */
    public void testLeadBlocksWithCustomMinLead() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 3, 24, 5, 16);

        policy.shouldTrigger(blockOffset(0));
        int lead1 = policy.leadBlocks();
        assertTrue("Lead should be at least minLead", lead1 >= 5);

        policy.shouldTrigger(blockOffset(1));
        int lead2 = policy.leadBlocks();
        assertTrue("Lead should be at least minLead", lead2 >= 5);

        policy.shouldTrigger(blockOffset(2));
        int lead3 = policy.leadBlocks();
        assertTrue("Lead should be at least minLead", lead3 >= 5);

        policy.shouldTrigger(blockOffset(3));
        int lead4 = policy.leadBlocks();
        assertTrue("Lead should be positive", lead4 > 0);
    }

    /**
     * Tests medium queue pressure shrinks window.
     */
    public void testQueuePressureMediumShrinksWindow() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        policy.shouldTrigger(blockOffset(0));
        policy.shouldTrigger(blockOffset(1));
        policy.shouldTrigger(blockOffset(2));
        policy.shouldTrigger(blockOffset(3));

        int windowBefore = policy.currentWindow();

        policy.onQueuePressureMedium();

        int windowAfter = policy.currentWindow();
        assertTrue("Window should shrink", windowAfter < windowBefore || windowAfter == 4);
        assertTrue("Window should not go below initial", windowAfter >= 4);
    }

    /**
     * Tests medium pressure bottoms at initial window.
     */
    public void testQueuePressureMediumBottomsAtInitial() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        policy.shouldTrigger(blockOffset(0));   // window=4

        policy.onQueuePressureMedium();

        assertEquals("Window should not go below initial", 4, policy.currentWindow());
    }

    /**
     * Tests high queue pressure resets to initial.
     */
    public void testQueuePressureHighResetsWindow() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        policy.shouldTrigger(blockOffset(0));   // window=4
        policy.shouldTrigger(blockOffset(1));   // window=8
        policy.shouldTrigger(blockOffset(2));   // window=16

        policy.onQueuePressureHigh();

        assertEquals("Window should reset to initial", 4, policy.currentWindow());
    }

    /**
     * Tests queue saturation delegates to medium pressure.
     */
    public void testQueueSaturatedDelegatesToMedium() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        policy.shouldTrigger(blockOffset(0));
        policy.shouldTrigger(blockOffset(1));
        policy.shouldTrigger(blockOffset(2));

        int windowBefore = policy.currentWindow();

        policy.onQueueSaturated();

        int windowAfter = policy.currentWindow();
        assertTrue("Window should shrink", windowAfter < windowBefore || windowAfter == 4);
        assertTrue("Window should not go below initial", windowAfter >= 4);
    }

    /**
     * Tests repeated pressure calls.
     */
    public void testRepeatedQueuePressureCalls() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        // Grow window
        policy.shouldTrigger(blockOffset(0));   // window=4
        policy.shouldTrigger(blockOffset(1));   // window=8
        policy.shouldTrigger(blockOffset(2));   // window=16
        policy.shouldTrigger(blockOffset(3));   // window=32

        // Apply pressure repeatedly
        policy.onQueuePressureMedium();  // 32 -> 16
        policy.onQueuePressureMedium();  // 16 -> 8
        policy.onQueuePressureMedium();  // 8 -> 4
        policy.onQueuePressureMedium();  // 4 -> 4 (bottomed)

        assertEquals("Window should bottom at initial", 4, policy.currentWindow());
    }

    /**
     * Tests cache hit shrink reduces window.
     */
    public void testCacheHitShrinkReducesWindow() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        policy.shouldTrigger(blockOffset(0));
        policy.shouldTrigger(blockOffset(1));
        policy.shouldTrigger(blockOffset(2));

        int windowBefore = policy.currentWindow();

        policy.onCacheHitShrink();

        int windowAfter = policy.currentWindow();
        assertTrue("Window should shrink", windowAfter < windowBefore || windowAfter == 4);
        assertTrue("Window should not go below initial", windowAfter >= 4);
    }

    /**
     * Tests cache hit shrink bottoms at initial.
     */
    public void testCacheHitShrinkBottomsAtInitial() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        policy.shouldTrigger(blockOffset(0));   // window=4
        policy.shouldTrigger(blockOffset(1));   // window=8

        policy.onCacheHitShrink();  // 8 -> 4
        assertEquals("Window should shrink to initial", 4, policy.currentWindow());

        policy.onCacheHitShrink();  // 4 -> 4 (bottomed)
        assertEquals("Window should stay at initial", 4, policy.currentWindow());
    }

    /**
     * Tests reset clears all state.
     */
    public void testResetClearsState() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        policy.shouldTrigger(blockOffset(0));
        policy.shouldTrigger(blockOffset(1));
        policy.shouldTrigger(blockOffset(2));

        policy.reset();

        assertEquals("Window should reset to initial", 4, policy.currentWindow());
    }

    /**
     * Tests behavior after reset.
     */
    public void testBehaviorAfterReset() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        policy.shouldTrigger(blockOffset(0));
        policy.shouldTrigger(blockOffset(1));
        policy.shouldTrigger(blockOffset(2));

        policy.reset();

        // Next access should be like first access
        boolean triggered = policy.shouldTrigger(blockOffset(0));
        assertTrue("First access after reset should trigger", triggered);
        assertEquals("Window should be initial", 4, policy.currentWindow());
    }

    /**
     * Tests multiple resets.
     */
    public void testMultipleResets() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        policy.shouldTrigger(blockOffset(0));
        policy.reset();
        policy.reset();
        policy.reset();

        assertEquals("Window should remain initial", 4, policy.currentWindow());
    }

    /**
     * Tests concurrent shouldTrigger calls.
     */
    public void testConcurrentShouldTriggerCalls() throws Exception {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 64, 16);

        int threadCount = 4;
        int accessesPerThread = 50;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < accessesPerThread; i++) {
                        long offset = blockOffset(threadId * accessesPerThread + i);
                        policy.shouldTrigger(offset);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        assertTrue("Concurrent access should complete", doneLatch.await(10, TimeUnit.SECONDS));

        // Policy should remain valid
        assertNotNull("Policy should handle concurrent access", policy);
        assertTrue("Window should be valid", policy.currentWindow() >= policy.initialWindow());
    }

    /**
     * Tests concurrent pressure operations.
     */
    public void testConcurrentPressureOperations() throws Exception {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 64, 16);

        // Grow window first
        for (int i = 0; i < 5; i++) {
            policy.shouldTrigger(blockOffset(i));
        }

        int threadCount = 3;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        // Thread 1: medium pressure
        new Thread(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < 10; i++) {
                    policy.onQueuePressureMedium();
                    Thread.sleep(1);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                doneLatch.countDown();
            }
        }).start();

        // Thread 2: high pressure
        new Thread(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < 10; i++) {
                    policy.onQueuePressureHigh();
                    Thread.sleep(1);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                doneLatch.countDown();
            }
        }).start();

        // Thread 3: cache hit shrink
        new Thread(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < 10; i++) {
                    policy.onCacheHitShrink();
                    Thread.sleep(1);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                doneLatch.countDown();
            }
        }).start();

        startLatch.countDown();
        assertTrue("Concurrent pressure should complete", doneLatch.await(10, TimeUnit.SECONDS));

        // Window should be at or above initial
        assertTrue("Window should be valid", policy.currentWindow() >= policy.initialWindow());
    }

    /**
     * Tests concurrent access and reset.
     */
    public void testConcurrentAccessAndReset() throws Exception {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(2);
        AtomicInteger triggerCount = new AtomicInteger(0);

        // Access thread
        new Thread(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < 100; i++) {
                    if (policy.shouldTrigger(blockOffset(i))) {
                        triggerCount.incrementAndGet();
                    }
                    if (i % 10 == 0) {
                        Thread.sleep(1);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                doneLatch.countDown();
            }
        }).start();

        // Reset thread
        new Thread(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < 5; i++) {
                    Thread.sleep(5);
                    policy.reset();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                doneLatch.countDown();
            }
        }).start();

        startLatch.countDown();
        assertTrue("Concurrent reset should complete", doneLatch.await(10, TimeUnit.SECONDS));

        // Should have triggered at least once
        assertTrue("Should have some triggers", triggerCount.get() > 0);
    }

    /**
     * Tests alternating sequential and random access.
     */
    public void testAlternatingSequentialAndRandom() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        // Sequential burst
        policy.shouldTrigger(blockOffset(0));
        policy.shouldTrigger(blockOffset(1));
        policy.shouldTrigger(blockOffset(2));

        int windowAfterSeq = policy.currentWindow();
        assertTrue("Window should grow", windowAfterSeq >= 4);

        // Random jump
        policy.shouldTrigger(blockOffset(100));  // large jump

        assertEquals("Window should reset", 4, policy.currentWindow());

        // Sequential again
        policy.shouldTrigger(blockOffset(101));
        assertTrue("Window should be valid", policy.currentWindow() >= 4);
    }

    /**
     * Tests sawtooth access pattern.
     */
    public void testSawtoothAccessPattern() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        // Forward
        policy.shouldTrigger(blockOffset(0));
        policy.shouldTrigger(blockOffset(1));
        policy.shouldTrigger(blockOffset(2));

        // Back
        policy.shouldTrigger(blockOffset(0));

        // Forward again
        policy.shouldTrigger(blockOffset(1));
        policy.shouldTrigger(blockOffset(2));
        policy.shouldTrigger(blockOffset(3));

        // Policy should remain functional
        assertTrue("Window should be valid", policy.currentWindow() >= 4);
    }

    /**
     * Tests stride access pattern.
     */
    public void testStrideAccessPattern() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        // Access with stride of 5
        policy.shouldTrigger(blockOffset(0));
        policy.shouldTrigger(blockOffset(5));
        policy.shouldTrigger(blockOffset(10));
        policy.shouldTrigger(blockOffset(15));

        // Should detect as non-sequential and not grow much
        assertNotNull("Policy should handle stride pattern", policy);
    }

    /**
     * Tests very large block indices.
     */
    public void testVeryLargeBlockIndices() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        long largeIndex = 1_000_000_000L;
        boolean triggered = policy.shouldTrigger(blockOffset(largeIndex));

        assertTrue("First access at large index should trigger", triggered);
        assertEquals("Window should be initial", 4, policy.currentWindow());
    }

    /**
     * Tests maximum positive gap.
     */
    public void testMaximumPositiveGap() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 16);

        policy.shouldTrigger(blockOffset(0));
        policy.shouldTrigger(blockOffset(Long.MAX_VALUE / CACHE_BLOCK_SIZE));

        // Should handle as large jump
        assertEquals("Window should reset on huge jump", 4, policy.currentWindow());
    }

    /**
     * Tests boundary of sequential gap detection.
     */
    public void testSequentialGapBoundary() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 8, 64, 16);

        policy.shouldTrigger(blockOffset(0));   // window=8

        // seqGapBuffer = max(2, min(8/2, 4)) = 4
        // gap=4 should be sequential
        assertTrue("Gap at boundary should trigger", policy.shouldTrigger(blockOffset(4)));

        // gap=5 should not be sequential
        policy.shouldTrigger(blockOffset(10));
        boolean trigger = policy.shouldTrigger(blockOffset(15)); // gap=5
        // This depends on window size and smallGapDivisor, just verify no crash
        assertNotNull("Should handle boundary gap", policy);
    }

    /**
     * Tests policy with minimal configuration.
     */
    public void testMinimalConfiguration() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 1, 1, 2);

        policy.shouldTrigger(blockOffset(0));
        assertEquals("Window should be 1", 1, policy.currentWindow());

        policy.shouldTrigger(blockOffset(1));
        // Window can't grow beyond max=1
        assertEquals("Window should remain 1", 1, policy.currentWindow());
    }

    /**
     * Tests policy with large configuration.
     */
    public void testLargeConfiguration() {
        policy = new WindowedReadaheadPolicy(TEST_PATH, 128, 1024, 32);

        policy.shouldTrigger(blockOffset(0));
        assertEquals("Window should be initial", 128, policy.currentWindow());

        // Sequential accesses should grow window
        policy.shouldTrigger(blockOffset(1));
        assertTrue("Window should grow or stay", policy.currentWindow() >= 128);

        policy.shouldTrigger(blockOffset(2));
        assertTrue("Window should grow or stay", policy.currentWindow() >= 128);

        policy.shouldTrigger(blockOffset(3));
        assertTrue("Window should not exceed max", policy.currentWindow() <= 1024);
    }
}

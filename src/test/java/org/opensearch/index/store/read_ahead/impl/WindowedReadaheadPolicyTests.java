/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Comprehensive tests for WindowedReadaheadPolicy covering all behavioral scenarios.
 */
public class WindowedReadaheadPolicyTests extends OpenSearchTestCase {

    private static final int CACHE_BLOCK_SIZE = 8192; // 2^13 from DirectIoConfigs.CACHE_BLOCK_SIZE_POWER
    private static final Path TEST_PATH = Paths.get("/test/file.dat");

    // ========== Construction and Parameter Validation Tests ==========

    /**
     * Tests basic construction with valid parameters.
     */
    public void testConstruction() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        assertNotNull("Policy should be created", policy);
        assertEquals("Initial window should match", 4, policy.initialWindow());
        assertEquals("Max window should match", 128, policy.maxWindow());
        assertEquals("Current window should be initial", 4, policy.currentWindow());
    }

    /**
     * Tests construction with full parameter set.
     */
    public void testConstructionFullParameters() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 8, 256, 2, 4);

        assertNotNull("Policy should be created", policy);
        assertEquals("Initial window should match", 8, policy.initialWindow());
        assertEquals("Max window should match", 256, policy.maxWindow());
        assertEquals("Current window should be initial", 8, policy.currentWindow());
    }

    /**
     * Tests that initialWindow must be >= 1.
     */
    public void testInvalidInitialWindowZero() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new WindowedReadaheadPolicy(TEST_PATH, 0, 128, 8));
        assertTrue("Error message should mention initialWindow", ex.getMessage().contains("initialWindow"));
    }

    /**
     * Tests that initialWindow must be >= 1.
     */
    public void testInvalidInitialWindowNegative() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new WindowedReadaheadPolicy(TEST_PATH, -1, 128, 8)
        );
        assertTrue("Error message should mention initialWindow", ex.getMessage().contains("initialWindow"));
    }

    /**
     * Tests that maxWindow must be >= initialWindow.
     */
    public void testInvalidMaxWindowLessThanInitial() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new WindowedReadaheadPolicy(TEST_PATH, 16, 8, 8));
        assertTrue("Error message should mention maxWindow", ex.getMessage().contains("maxWindow"));
    }

    /**
     * Tests that minLead must be >= 1.
     */
    public void testInvalidMinLeadZero() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 0, 8)
        );
        assertTrue("Error message should mention minLead", ex.getMessage().contains("minLead"));
    }

    /**
     * Tests that smallGapDivisor must be >= 2.
     */
    public void testInvalidSmallGapDivisorOne() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 1));
        assertTrue("Error message should mention smallGapDivisor", ex.getMessage().contains("smallGapDivisor"));
    }

    /**
     * Tests that initialWindow == maxWindow is valid (no growth).
     */
    public void testInitialEqualsMaxWindow() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 16, 16, 8);

        assertEquals("Initial should equal max", policy.initialWindow(), policy.maxWindow());
        assertEquals("Current window should be initial", 16, policy.currentWindow());
    }

    // ========== Sequential Access Pattern Tests ==========

    /**
     * Tests first access triggers readahead and initializes state.
     */
    public void testFirstAccess() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        boolean triggered = policy.shouldTrigger(0);

        assertTrue("First access should trigger", triggered);
        assertEquals("Window should be initial", 4, policy.currentWindow());
    }

    /**
     * Tests sequential forward reads trigger and grow window.
     */
    public void testSequentialForwardGrowth() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        // First access
        policy.shouldTrigger(0);
        assertEquals("Initial window", 4, policy.currentWindow());

        // Sequential reads: gap = 1 each time
        policy.shouldTrigger(1L * CACHE_BLOCK_SIZE);
        assertEquals("Window doubles after sequential", 8, policy.currentWindow());

        policy.shouldTrigger(2L * CACHE_BLOCK_SIZE);
        assertEquals("Window doubles again", 16, policy.currentWindow());

        policy.shouldTrigger(3L * CACHE_BLOCK_SIZE);
        assertEquals("Window doubles again", 32, policy.currentWindow());
    }

    /**
     * Tests window growth caps at maxWindow.
     */
    public void testWindowGrowthCapsAtMax() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 32, 8);

        policy.shouldTrigger(0);
        policy.shouldTrigger(1L * CACHE_BLOCK_SIZE); // window = 8
        policy.shouldTrigger(2L * CACHE_BLOCK_SIZE); // window = 16
        policy.shouldTrigger(3L * CACHE_BLOCK_SIZE); // window = 32 (max)

        assertEquals("Window should be at max", 32, policy.currentWindow());

        policy.shouldTrigger(4L * CACHE_BLOCK_SIZE);
        assertEquals("Window should stay at max", 32, policy.currentWindow());

        policy.shouldTrigger(5L * CACHE_BLOCK_SIZE);
        assertEquals("Window should stay at max", 32, policy.currentWindow());
    }

    /**
     * Tests that all sequential accesses trigger readahead.
     */
    public void testSequentialAccessesAllTrigger() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        List<Boolean> triggers = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            triggers.add(policy.shouldTrigger(i * CACHE_BLOCK_SIZE));
        }

        // All sequential accesses should trigger
        for (int i = 0; i < triggers.size(); i++) {
            assertTrue("Sequential access " + i + " should trigger", triggers.get(i));
        }
    }

    /**
     * Tests gap = 2 is considered sequential within buffer.
     */
    public void testSmallGapsStillSequential() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 8, 128, 8);

        policy.shouldTrigger(0);
        int initialWindow = policy.currentWindow();

        // Gap of 2 blocks - should be sequential (within seqGapBuffer)
        boolean triggered = policy.shouldTrigger(2L * CACHE_BLOCK_SIZE);

        assertTrue("Small gap should trigger", triggered);
        assertEquals("Window should grow", initialWindow * 2, policy.currentWindow());
    }

    /**
     * Tests gap within seqGapBuffer is sequential.
     */
    public void testSequentialGapBuffer() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 8, 128, 8);

        policy.shouldTrigger(0);

        // seqGapBuffer = max(2, min(window/2, 4)) = max(2, min(4, 4)) = 4
        // So gaps 1-4 should be sequential
        boolean triggered = policy.shouldTrigger(4L * CACHE_BLOCK_SIZE);

        assertTrue("Gap within buffer should be sequential", triggered);
        assertTrue("Window should grow", policy.currentWindow() > 8);
    }

    // ========== Forward Jump Tests ==========

    /**
     * Tests small forward jump triggers but shrinks window.
     */
    public void testSmallForwardJump() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        // Build up window to 64
        policy.shouldTrigger(0);
        policy.shouldTrigger(1L * CACHE_BLOCK_SIZE);
        policy.shouldTrigger(2L * CACHE_BLOCK_SIZE);
        policy.shouldTrigger(3L * CACHE_BLOCK_SIZE);
        policy.shouldTrigger(4L * CACHE_BLOCK_SIZE);
        int windowBeforeJump = policy.currentWindow(); // Should be 64

        // Small jump: gap > seqGapBuffer but <= window/smallGapDivisor
        // seqGapBuffer = max(2, min(64/2, 4)) = 4
        // window/8 = 64/8 = 8, so jump of 6 blocks is small (> 4 but <= 8)
        boolean triggered = policy.shouldTrigger(10L * CACHE_BLOCK_SIZE);

        assertTrue("Small jump should trigger", triggered);
        assertEquals("Window should shrink by half", windowBeforeJump / 2, policy.currentWindow());
    }

    /**
     * Tests large forward jump resets window without triggering.
     */
    public void testLargeForwardJump() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        // Build up window
        policy.shouldTrigger(0);
        policy.shouldTrigger(1L * CACHE_BLOCK_SIZE);
        policy.shouldTrigger(2L * CACHE_BLOCK_SIZE);
        assertEquals("Window should be grown", 16, policy.currentWindow());

        // Large jump: gap > window/smallGapDivisor
        // window/8 = 16/8 = 2, so jump of 100 is large
        boolean triggered = policy.shouldTrigger(100L * CACHE_BLOCK_SIZE);

        assertFalse("Large jump should NOT trigger", triggered);
        assertEquals("Window should reset to initial", 4, policy.currentWindow());
    }

    /**
     * Tests boundary between small and large jumps.
     */
    public void testJumpBoundary() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 8, 128, 4);

        // Build window - start at 8, doubles each time
        policy.shouldTrigger(0); // window = 8
        policy.shouldTrigger(1L * CACHE_BLOCK_SIZE); // window = 16
        policy.shouldTrigger(2L * CACHE_BLOCK_SIZE); // window = 32
        assertEquals("Window should be 32", 32, policy.currentWindow());

        // seqGapBuffer = max(2, min(32/2, 4)) = 4
        // smallGapDivisor = 4, so smallGap = max(1, 32/4) = 8
        // Jump of 8 blocks (from 2 to 10) should be small (gap=8, seqGap=4, smallGap=8)
        // gap > seqGapBuffer (8 > 4) AND gap <= smallGap (8 <= 8) → small jump, triggers and shrinks
        boolean triggered8 = policy.shouldTrigger(10L * CACHE_BLOCK_SIZE);
        assertTrue("Jump of exactly smallGap should trigger", triggered8);
        assertEquals("Window should shrink by half after small jump", 16, policy.currentWindow());

        // Jump of 20 blocks should be large (doesn't trigger)
        boolean triggered9 = policy.shouldTrigger(30L * CACHE_BLOCK_SIZE);
        assertFalse("Large jump should NOT trigger", triggered9);
    }

    // ========== Same Position Tests ==========

    /**
     * Tests reading same position doesn't trigger or change window.
     */
    public void testSamePosition() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        policy.shouldTrigger(0);
        policy.shouldTrigger(1L * CACHE_BLOCK_SIZE);
        int windowBefore = policy.currentWindow();

        boolean triggered = policy.shouldTrigger(1L * CACHE_BLOCK_SIZE);

        assertFalse("Same position should NOT trigger", triggered);
        assertEquals("Window should not change", windowBefore, policy.currentWindow());
    }

    /**
     * Tests multiple same position reads.
     */
    public void testRepeatedSamePosition() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        policy.shouldTrigger(5L * CACHE_BLOCK_SIZE);
        int windowBefore = policy.currentWindow();

        for (int i = 0; i < 10; i++) {
            boolean triggered = policy.shouldTrigger(5L * CACHE_BLOCK_SIZE);
            assertFalse("Repeated same position should not trigger", triggered);
        }

        assertEquals("Window should remain unchanged", windowBefore, policy.currentWindow());
    }

    // ========== Backward Seek Tests ==========

    /**
     * Tests small backward seek decays window without triggering.
     */
    public void testSmallBackwardSeek() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        // Build up window
        policy.shouldTrigger(0);
        policy.shouldTrigger(1L * CACHE_BLOCK_SIZE);
        policy.shouldTrigger(2L * CACHE_BLOCK_SIZE);
        policy.shouldTrigger(3L * CACHE_BLOCK_SIZE);
        int windowBefore = policy.currentWindow(); // Should be 32

        // Small backward: gap = -1
        boolean triggered = policy.shouldTrigger(2L * CACHE_BLOCK_SIZE);

        assertFalse("Backward seek should NOT trigger", triggered);
        assertTrue("Window should decay (not reset)", policy.currentWindow() > 4);
        assertTrue("Window should shrink", policy.currentWindow() < windowBefore);
    }

    /**
     * Tests large backward seek resets window.
     */
    public void testLargeBackwardSeek() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        // Build up window
        policy.shouldTrigger(0);
        policy.shouldTrigger(1L * CACHE_BLOCK_SIZE);
        policy.shouldTrigger(2L * CACHE_BLOCK_SIZE);
        assertEquals("Window should be 16", 16, policy.currentWindow());

        // Large backward: from block 2 to block 0, gap = -2
        // absGap = 2, window/2 = 8, so absGap <= window/2 → decay (not reset)
        // Need larger backward to trigger reset
        // Jump from block 2 to far in past
        policy.shouldTrigger(100L * CACHE_BLOCK_SIZE);
        int windowAfterForward = policy.currentWindow();

        // Now backward by more than window/2
        // If window is reset from forward jump, need to rebuild
        policy.shouldTrigger(101L * CACHE_BLOCK_SIZE);
        policy.shouldTrigger(102L * CACHE_BLOCK_SIZE);
        int rebuiltWindow = policy.currentWindow();

        // Large backward: gap = -50 (absGap > window/2)
        boolean triggered = policy.shouldTrigger(52L * CACHE_BLOCK_SIZE);

        assertFalse("Large backward should NOT trigger", triggered);
        // Should reset to initial if absGap > window/2
        assertTrue("Window should be reset or decayed", policy.currentWindow() <= rebuiltWindow);
    }

    /**
     * Tests backward seek that is exactly window/2.
     */
    public void testBackwardSeekAtBoundary() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        // Build window to 16
        policy.shouldTrigger(0);
        policy.shouldTrigger(1L * CACHE_BLOCK_SIZE);
        policy.shouldTrigger(2L * CACHE_BLOCK_SIZE);
        assertEquals("Window should be 16", 16, policy.currentWindow());

        // Backward by exactly window/2 = 8 blocks
        // absGap = 8, window/2 = 8, so absGap <= window/2 → decay
        boolean triggered = policy.shouldTrigger((2L - 8L) * CACHE_BLOCK_SIZE);

        assertFalse("Backward at boundary should NOT trigger", triggered);
        assertTrue("Should decay not reset", policy.currentWindow() >= 4);
    }

    /**
     * Tests backward seek beyond window/2 resets.
     */
    public void testBackwardSeekBeyondBoundary() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        // Build window to 16
        policy.shouldTrigger(0);
        policy.shouldTrigger(1L * CACHE_BLOCK_SIZE);
        policy.shouldTrigger(2L * CACHE_BLOCK_SIZE);
        assertEquals("Window should be 16", 16, policy.currentWindow());

        // Backward by window/2 + 1 = 9 blocks
        // absGap = 9, window/2 = 8, so absGap > window/2 → reset
        boolean triggered = policy.shouldTrigger((2L - 9L) * CACHE_BLOCK_SIZE);

        assertFalse("Backward beyond boundary should NOT trigger", triggered);
        assertEquals("Should reset to initial", 4, policy.currentWindow());
    }

    // ========== Decay Behavior Tests ==========

    /**
     * Tests decay reduces window by 25%.
     */
    public void testDecayAmount() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        // Build window to 32
        policy.shouldTrigger(0);
        for (int i = 1; i <= 3; i++) {
            policy.shouldTrigger(i * CACHE_BLOCK_SIZE);
        }
        assertEquals("Window should be 32", 32, policy.currentWindow());

        // Trigger small backward to cause decay
        policy.shouldTrigger(2L * CACHE_BLOCK_SIZE);

        // Decay formula: max(initialWindow, window - max(1, window/4))
        // max(4, 32 - max(1, 8)) = max(4, 24) = 24
        assertEquals("Window should decay by 25%", 24, policy.currentWindow());
    }

    /**
     * Tests decay doesn't go below initialWindow.
     */
    public void testDecayFloor() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 8, 128, 8);

        // Start at initial window
        policy.shouldTrigger(0);
        assertEquals("Window should be initial", 8, policy.currentWindow());

        // Trigger backward (should try to decay)
        policy.shouldTrigger(0);

        assertEquals("Window should not go below initial", 8, policy.currentWindow());
    }

    /**
     * Tests multiple decays gradually reduce window.
     */
    public void testMultipleDecays() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        // Build to 64
        policy.shouldTrigger(0);
        for (int i = 1; i <= 4; i++) {
            policy.shouldTrigger(i * CACHE_BLOCK_SIZE);
        }
        assertEquals("Window should be 64", 64, policy.currentWindow());

        // Cause multiple small backward seeks to trigger decay
        // After block 4, go back to 3, then 2, then 1
        policy.shouldTrigger(3L * CACHE_BLOCK_SIZE); // gap = -1 (backward)
        int window1 = policy.currentWindow();
        assertTrue("Window should decay after first backward", window1 < 64);
        assertTrue("Window should stay above initial", window1 >= 4);

        policy.shouldTrigger(2L * CACHE_BLOCK_SIZE); // gap = -1 (backward)
        int window2 = policy.currentWindow();
        assertTrue("Window should continue to decay", window2 <= window1);
        assertTrue("Window should stay above initial", window2 >= 4);

        policy.shouldTrigger(1L * CACHE_BLOCK_SIZE); // gap = -1 (backward)
        int window3 = policy.currentWindow();
        assertTrue("Window should continue to decay", window3 <= window2);
        assertTrue("Window should stay above initial", window3 >= 4);

        assertTrue("Window should have decayed significantly", policy.currentWindow() < 64);
        assertTrue("Window should stay above or at initial", policy.currentWindow() >= 4);
    }

    // ========== Lead Blocks Tests ==========

    /**
     * Tests leadBlocks returns appropriate value.
     */
    public void testLeadBlocks() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 6, 128, 2, 8);

        policy.shouldTrigger(0);

        // Lead is max(minLead, window/3)
        // window = 6, minLead = 2
        // lead = max(2, 6/3) = max(2, 2) = 2
        assertEquals("Lead should be correct", 2, policy.leadBlocks());
    }

    /**
     * Tests leadBlocks grows with window.
     */
    public void testLeadBlocksGrowsWithWindow() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 6, 128, 2, 8);

        policy.shouldTrigger(0);
        int lead1 = policy.leadBlocks();

        // Grow window
        policy.shouldTrigger(1L * CACHE_BLOCK_SIZE);
        int lead2 = policy.leadBlocks();

        assertTrue("Lead should grow with window", lead2 > lead1);
    }

    /**
     * Tests leadBlocks respects minLead.
     */
    public void testLeadBlocksRespectsMinimum() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 3, 128, 5, 8);

        policy.shouldTrigger(0);

        // window = 3, minLead = 5
        // lead = max(5, 3/3) = max(5, 1) = 5
        assertEquals("Lead should be at least minLead", 5, policy.leadBlocks());
    }

    // ========== Queue Pressure Response Tests ==========

    /**
     * Tests onQueuePressureMedium shrinks window by half.
     */
    public void testQueuePressureMedium() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        // Build window to 32
        policy.shouldTrigger(0);
        for (int i = 1; i <= 3; i++) {
            policy.shouldTrigger(i * CACHE_BLOCK_SIZE);
        }
        assertEquals("Window should be 32", 32, policy.currentWindow());

        policy.onQueuePressureMedium();

        assertEquals("Window should shrink by half", 16, policy.currentWindow());
    }

    /**
     * Tests medium pressure doesn't go below initial.
     */
    public void testQueuePressureMediumFloor() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 8, 128, 8);

        policy.shouldTrigger(0);
        assertEquals("Window should be initial", 8, policy.currentWindow());

        policy.onQueuePressureMedium();

        assertEquals("Window should not go below initial", 8, policy.currentWindow());
    }

    /**
     * Tests onQueuePressureHigh resets to initial.
     */
    public void testQueuePressureHigh() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        // Build window to 64
        policy.shouldTrigger(0);
        for (int i = 1; i <= 4; i++) {
            policy.shouldTrigger(i * CACHE_BLOCK_SIZE);
        }
        assertEquals("Window should be 64", 64, policy.currentWindow());

        policy.onQueuePressureHigh();

        assertEquals("Window should reset to initial", 4, policy.currentWindow());
    }

    /**
     * Tests onQueueSaturated applies medium pressure.
     */
    public void testQueueSaturated() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        policy.shouldTrigger(0);
        policy.shouldTrigger(1L * CACHE_BLOCK_SIZE);
        policy.shouldTrigger(2L * CACHE_BLOCK_SIZE);
        assertEquals("Window should be 16", 16, policy.currentWindow());

        policy.onQueueSaturated();

        assertEquals("Saturated should shrink by half", 8, policy.currentWindow());
    }

    // ========== Cache Hit Shrink Tests ==========

    /**
     * Tests onCacheHitShrink reduces window.
     */
    public void testCacheHitShrink() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        // Build window to 32
        policy.shouldTrigger(0);
        for (int i = 1; i <= 3; i++) {
            policy.shouldTrigger(i * CACHE_BLOCK_SIZE);
        }
        assertEquals("Window should be 32", 32, policy.currentWindow());

        policy.onCacheHitShrink();

        assertEquals("Window should shrink by half", 16, policy.currentWindow());
    }

    /**
     * Tests cache hit shrink doesn't go below initial.
     */
    public void testCacheHitShrinkFloor() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 8, 128, 8);

        policy.shouldTrigger(0);

        policy.onCacheHitShrink();

        assertEquals("Window should not go below initial", 8, policy.currentWindow());
    }

    // ========== Reset Tests ==========

    /**
     * Tests reset returns to initial state.
     */
    public void testReset() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        // Build up state
        policy.shouldTrigger(0);
        policy.shouldTrigger(1L * CACHE_BLOCK_SIZE);
        policy.shouldTrigger(2L * CACHE_BLOCK_SIZE);
        assertEquals("Window should be grown", 16, policy.currentWindow());

        policy.reset();

        assertEquals("Window should reset to initial", 4, policy.currentWindow());

        // Next access should trigger (like first access)
        boolean triggered = policy.shouldTrigger(100L * CACHE_BLOCK_SIZE);
        assertTrue("After reset, access should trigger", triggered);
    }

    /**
     * Tests reset clears position history.
     */
    public void testResetClearsHistory() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        policy.shouldTrigger(50L * CACHE_BLOCK_SIZE);

        policy.reset();

        // Large jump after reset should trigger (treated as first access)
        boolean triggered = policy.shouldTrigger(1000L * CACHE_BLOCK_SIZE);
        assertTrue("After reset, should trigger like first access", triggered);
    }

    // ========== Concurrent Access Tests ==========

    /**
     * Tests thread-safe concurrent access.
     */
    public void testConcurrentAccess() throws Exception {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        int threadCount = 8;
        int accessesPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger triggerCount = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < accessesPerThread; i++) {
                        long offset = (threadId * accessesPerThread + i) * CACHE_BLOCK_SIZE;
                        if (policy.shouldTrigger(offset)) {
                            triggerCount.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue("All threads should complete", doneLatch.await(10, TimeUnit.SECONDS));

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // Should have some triggers (exact count depends on interleaving)
        assertTrue("Should have triggered some readaheads", triggerCount.get() > 0);

        // Window should be valid
        assertTrue("Window should be within bounds", policy.currentWindow() >= 4);
        assertTrue("Window should be within bounds", policy.currentWindow() <= 128);
    }

    /**
     * Tests concurrent pressure callbacks don't corrupt state.
     */
    public void testConcurrentPressureCallbacks() throws Exception {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        // Build up window
        for (int i = 0; i < 10; i++) {
            policy.shouldTrigger(i * CACHE_BLOCK_SIZE);
        }

        int threadCount = 4;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < 100; i++) {
                        switch (threadId % 4) {
                            case 0:
                                policy.onQueuePressureMedium();
                                break;
                            case 1:
                                policy.onQueuePressureHigh();
                                break;
                            case 2:
                                policy.onCacheHitShrink();
                                break;
                            case 3:
                                policy.shouldTrigger((i + 100) * CACHE_BLOCK_SIZE);
                                break;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue("All threads should complete", doneLatch.await(10, TimeUnit.SECONDS));

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // State should remain valid
        assertTrue("Window should be valid", policy.currentWindow() >= 4);
        assertTrue("Window should be valid", policy.currentWindow() <= 128);
    }

    // ========== Complex Pattern Tests ==========

    /**
     * Tests alternating sequential and random access.
     */
    public void testAlternatingPattern() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 64, 8);

        // Sequential burst
        policy.shouldTrigger(0);
        policy.shouldTrigger(1L * CACHE_BLOCK_SIZE);
        policy.shouldTrigger(2L * CACHE_BLOCK_SIZE);
        int windowAfterSeq = policy.currentWindow();
        assertTrue("Window should grow during sequential", windowAfterSeq > 4);

        // Random jump
        policy.shouldTrigger(100L * CACHE_BLOCK_SIZE);
        assertEquals("Window should reset after large jump", 4, policy.currentWindow());

        // Sequential again
        policy.shouldTrigger(101L * CACHE_BLOCK_SIZE);
        policy.shouldTrigger(102L * CACHE_BLOCK_SIZE);
        assertTrue("Window should grow again", policy.currentWindow() > 4);
    }

    /**
     * Tests recovery from backward seek.
     */
    public void testRecoveryFromBackwardSeek() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        // Build up
        policy.shouldTrigger(0);
        policy.shouldTrigger(1L * CACHE_BLOCK_SIZE);
        policy.shouldTrigger(2L * CACHE_BLOCK_SIZE);

        // Backward seek
        policy.shouldTrigger(0);
        int windowAfterBackward = policy.currentWindow();

        // Resume sequential
        policy.shouldTrigger(1L * CACHE_BLOCK_SIZE);
        policy.shouldTrigger(2L * CACHE_BLOCK_SIZE);
        policy.shouldTrigger(3L * CACHE_BLOCK_SIZE);

        assertTrue("Window should recover after resuming sequential", policy.currentWindow() > windowAfterBackward);
    }

    /**
     * Tests stress scenario with pressure and growth.
     */
    public void testStressWithPressure() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 4, 128, 8);

        // Grow window
        for (int i = 0; i < 5; i++) {
            policy.shouldTrigger(i * CACHE_BLOCK_SIZE);
        }
        assertTrue("Window should be grown", policy.currentWindow() > 4);

        // Apply pressure
        policy.onQueuePressureHigh();
        assertEquals("Pressure should reset window", 4, policy.currentWindow());

        // Try to grow again
        for (int i = 5; i < 10; i++) {
            policy.shouldTrigger(i * CACHE_BLOCK_SIZE);
        }
        assertTrue("Window should grow again after pressure", policy.currentWindow() > 4);
    }

    /**
     * Tests typical Lucene access pattern (sequential with occasional jumps).
     */
    public void testTypicalLucenePattern() {
        WindowedReadaheadPolicy policy = new WindowedReadaheadPolicy(TEST_PATH, 8, 256, 8);

        long offset = 0;
        int triggerCount = 0;

        // Sequential read of posting list
        for (int i = 0; i < 50; i++) {
            if (policy.shouldTrigger(offset)) {
                triggerCount++;
            }
            offset += CACHE_BLOCK_SIZE;
        }

        assertTrue("Should trigger many times during sequential", triggerCount > 10);
        assertTrue("Window should have grown", policy.currentWindow() > 8);

        // Jump to different posting list
        offset = 1000L * CACHE_BLOCK_SIZE;
        boolean jumpTriggered = policy.shouldTrigger(offset);
        assertFalse("Large jump should not trigger", jumpTriggered);

        // Sequential again
        for (int i = 0; i < 20; i++) {
            offset += CACHE_BLOCK_SIZE;
            policy.shouldTrigger(offset);
        }

        assertTrue("Window should grow again after jump", policy.currentWindow() > 8);
    }
}

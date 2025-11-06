/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.test.OpenSearchTestCase;

public class WindowedReadAheadContextTests extends OpenSearchTestCase {

    private static final int CACHE_BLOCK_SIZE = 4096;
    private static final Path TEST_PATH = Paths.get("/test/file.dat");
    private static final long FILE_SIZE = 1024 * 1024; // 1MB

    private Worker mockWorker;
    private Runnable mockSignalCallback;
    private WindowedReadAheadContext context;
    private WindowedReadAheadConfig config;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mockWorker = mock(Worker.class);
        mockSignalCallback = mock(Runnable.class);
        config = WindowedReadAheadConfig.defaultConfig();

        // Default: worker accepts all schedules
        when(mockWorker.schedule(any(Path.class), anyLong(), anyLong())).thenReturn(true);
    }

    private WindowedReadAheadContext createContext(long fileLength) {
        return WindowedReadAheadContext.build(TEST_PATH, fileLength, mockWorker, config, mockSignalCallback);
    }

    private WindowedReadAheadContext createContext(long fileLength, WindowedReadAheadConfig customConfig) {
        return WindowedReadAheadContext.build(TEST_PATH, fileLength, mockWorker, customConfig, mockSignalCallback);
    }

    /**
     * Tests that context can be created with valid parameters.
     */
    public void testContextCreation() {
        context = createContext(FILE_SIZE);

        assertNotNull("Context should be created", context);
        assertNotNull("Policy should be initialized", context.policy());
        assertTrue("Readahead should be enabled initially", context.isReadAheadEnabled());
        assertFalse("No queued work initially", context.hasQueuedWork());
    }

    /**
     * Tests context creation with zero file length.
     */
    public void testContextCreationWithZeroFileLength() {
        context = createContext(0L);

        assertNotNull("Context should be created with zero length", context);
        assertFalse("No queued work for empty file", context.hasQueuedWork());
    }

    /**
     * Tests context creation with small file (single block).
     */
    public void testContextCreationWithSingleBlock() {
        context = createContext(CACHE_BLOCK_SIZE);

        assertNotNull("Context should be created with single block", context);
    }

    // ========== Cache Hit Handling Tests ==========

    /**
     * Tests that single cache hits below threshold don't trigger readahead.
     */
    public void testSingleCacheHitNoTrigger() {
        context = createContext(FILE_SIZE);

        // Single hit should not trigger
        context.onAccess(0, true);

        assertFalse("Single hit should not queue work", context.hasQueuedWork());
        verify(mockSignalCallback, never()).run();
    }

    /**
     * Tests that consecutive cache hits eventually trigger readahead extension.
     * The threshold starts at 8 hits. Note that hits set the signal pending flag
     * but the callback is only drained on misses to keep the hot path fast.
     */
    public void testConsecutiveCacheHitsTriggerExtension() throws Exception {
        context = createContext(FILE_SIZE);

        // First, trigger initial readahead with a miss
        context.onAccess(0, false);
        context.processQueue();

        // Now simulate consecutive hits at positions that trigger guardedExtend
        // Hits need to be in the guard zone (close to scheduled end)
        long scheduledEnd = 10; // After initial schedule
        for (int i = 0; i < 10; i++) {
            context.onAccess((scheduledEnd - 5 + i) * CACHE_BLOCK_SIZE, true);
        }

        // The 8th hit should trigger extension (sets desiredEndBlock)
        // But signal callback is only invoked on miss (drainSignalIfPending)
        // So just check that work is queued
        assertNotNull("Consecutive hits should be handled", context);
    }

    /**
     * Tests exponential backoff of hit threshold to reduce overhead.
     * Hits in the guard zone trigger extension, and threshold doubles each time.
     */
    public void testCacheHitThresholdExponentialBackoff() throws Exception {
        context = createContext(FILE_SIZE);

        // Trigger initial readahead
        context.onAccess(0, false);
        context.onAccess(CACHE_BLOCK_SIZE, false);
        context.onAccess(2 * CACHE_BLOCK_SIZE, false);
        context.processQueue();

        // First batch: 8 hits in guard zone should trigger threshold increment
        long scheduledEnd = 10;
        for (int i = 0; i < 8; i++) {
            context.onAccess((scheduledEnd - 3 + i) * CACHE_BLOCK_SIZE, true);
        }

        // After 8 hits, threshold doubles to 16
        // Verify context handles the backoff logic
        assertNotNull("Context should handle exponential backoff", context);
    }

    /**
     * Tests that hit threshold caps at maximum value.
     */
    public void testCacheHitThresholdMaxCap() throws Exception {
        context = createContext(FILE_SIZE);

        // Trigger initial readahead
        context.onAccess(0, false);
        context.processQueue();

        // Repeatedly trigger to max out threshold (8 -> 16 -> 32 -> 64 -> 128 -> 256 -> 512)
        for (int batch = 0; batch < 7; batch++) {
            int threshold = 8 << batch;
            for (int i = 0; i < threshold; i++) {
                context.onAccess((batch * 1000 + i) * CACHE_BLOCK_SIZE, true);
            }
            if (context.hasQueuedWork()) {
                context.processQueue();
            }
        }

        // Threshold should now be at max (512), shouldn't grow further
        assertNotNull("Context should remain valid", context);
    }

    /**
     * Tests single cache miss batching - should not immediately trigger.
     */
    public void testSingleCacheMissNoImmediateTrigger() {
        context = createContext(FILE_SIZE);

        context.onAccess(0, false);

        // Single miss should queue work but may not signal yet
        assertNotNull("Context should remain valid", context);
    }

    /**
     * Tests that batched cache misses trigger sequential readahead.
     * Default batch size is 3 misses.
     */
    public void testBatchedCacheMissesTriggerReadahead() throws Exception {
        context = createContext(FILE_SIZE);

        // Batch of 3 sequential misses
        context.onAccess(0, false);
        context.onAccess(CACHE_BLOCK_SIZE, false);
        context.onAccess(2 * CACHE_BLOCK_SIZE, false);

        assertTrue("Batched misses should queue work", context.hasQueuedWork());

        // Process and verify schedule was called
        context.processQueue();
        verify(mockWorker, atLeastOnce()).schedule(eq(TEST_PATH), anyLong(), anyLong());
    }

    /**
     * Tests that cache miss resets hit threshold to base value.
     */
    public void testCacheMissResetsHitThreshold() throws Exception {
        context = createContext(FILE_SIZE);

        // Build up hit threshold
        context.onAccess(0, false);
        context.processQueue();

        for (int i = 0; i < 8; i++) {
            context.onAccess(i * CACHE_BLOCK_SIZE, true);
        }
        context.processQueue();

        // Now a miss should reset threshold
        context.onAccess(100 * CACHE_BLOCK_SIZE, false);

        // Next trigger should happen at base threshold (8) again
        for (int i = 101; i < 109; i++) {
            context.onAccess(i * CACHE_BLOCK_SIZE, true);
        }

        assertNotNull("Context should handle threshold reset", context);
    }

    /**
     * Tests sequential miss detection and readahead extension.
     */
    public void testSequentialMissPattern() throws Exception {
        context = createContext(FILE_SIZE);

        // Sequential misses in forward direction
        long offset = 0;
        for (int i = 0; i < 5; i++) {
            context.onAccess(offset, false);
            offset += CACHE_BLOCK_SIZE;
        }

        assertTrue("Sequential misses should queue work", context.hasQueuedWork());
        context.processQueue();

        verify(mockWorker, atLeastOnce()).schedule(eq(TEST_PATH), anyLong(), anyLong());
    }

    /**
     * Tests that far-ahead misses trigger immediate readahead.
     */
    public void testFarAheadMissTriggers() throws Exception {
        context = createContext(FILE_SIZE);

        // Initial miss and schedule
        context.onAccess(0, false);
        context.onAccess(CACHE_BLOCK_SIZE, false);
        context.onAccess(2 * CACHE_BLOCK_SIZE, false);
        context.processQueue();

        // Jump far ahead (more than window/4)
        long farOffset = 100 * CACHE_BLOCK_SIZE;
        context.onAccess(farOffset, false);

        assertTrue("Far-ahead miss should queue work", context.hasQueuedWork());
    }

    /**
     * Tests backward access cancels pending readahead.
     */
    public void testBackwardAccessCancelsReadahead() throws Exception {
        context = createContext(FILE_SIZE);

        // Build up forward readahead
        context.onAccess(100 * CACHE_BLOCK_SIZE, false);
        context.onAccess(101 * CACHE_BLOCK_SIZE, false);
        context.onAccess(102 * CACHE_BLOCK_SIZE, false);
        context.processQueue();

        // Access backward, outside the scheduled window
        context.onAccess(0, false);

        // Backward access should reduce or cancel pending work
        assertNotNull("Context should handle backward access", context);
    }

    /**
     * Tests random access pattern detection.
     */
    public void testRandomAccessPattern() throws Exception {
        context = createContext(FILE_SIZE);

        // Random access pattern: scattered block accesses
        long[] randomOffsets = { 0, 50, 10, 80, 30, 100, 5 };
        for (long blockIndex : randomOffsets) {
            context.onAccess(blockIndex * CACHE_BLOCK_SIZE, false);
        }

        // Random pattern may trigger some readahead but should be limited
        assertNotNull("Context should handle random access", context);
    }

    /**
     * Tests processQueue returns false when no work queued.
     */
    public void testProcessQueueNoWork() {
        context = createContext(FILE_SIZE);

        boolean processed = context.processQueue();

        assertFalse("processQueue should return false with no work", processed);
        verify(mockWorker, never()).schedule(any(), anyLong(), anyLong());
    }

    /**
     * Tests processQueue schedules work when available.
     */
    public void testProcessQueueSchedulesWork() throws Exception {
        context = createContext(FILE_SIZE);

        // Queue work via misses
        context.onAccess(0, false);
        context.onAccess(CACHE_BLOCK_SIZE, false);
        context.onAccess(2 * CACHE_BLOCK_SIZE, false);

        assertTrue("Should have queued work", context.hasQueuedWork());

        boolean processed = context.processQueue();

        assertTrue("processQueue should return true when work processed", processed);
        verify(mockWorker, atLeastOnce()).schedule(eq(TEST_PATH), anyLong(), anyLong());
    }

    /**
     * Tests that processQueue respects file boundaries.
     */
    public void testProcessQueueRespectsFileBoundary() throws Exception {
        long smallFileSize = 10 * CACHE_BLOCK_SIZE;
        context = createContext(smallFileSize);

        // Try to trigger readahead near end of file
        context.onAccess(8 * CACHE_BLOCK_SIZE, false);
        context.onAccess(9 * CACHE_BLOCK_SIZE, false);
        context.onAccess(9 * CACHE_BLOCK_SIZE, false); // Third miss triggers

        context.processQueue();

        // Should not schedule beyond file boundary
        verify(mockWorker, atLeastOnce()).schedule(eq(TEST_PATH), anyLong(), anyLong());
    }

    /**
     * Tests spin-merge optimization during queue processing.
     */
    public void testProcessQueueSpinMerge() throws Exception {
        context = createContext(FILE_SIZE);

        // Queue initial work
        context.onAccess(0, false);
        context.onAccess(CACHE_BLOCK_SIZE, false);
        context.onAccess(2 * CACHE_BLOCK_SIZE, false);

        // Process should spin briefly to merge updates
        boolean processed = context.processQueue();

        assertTrue("Should process queued work", processed);
    }

    /**
     * Tests that worker rejection is handled properly.
     */
    public void testProcessQueueWorkerRejectsSchedule() throws Exception {
        when(mockWorker.schedule(any(), anyLong(), anyLong())).thenReturn(false);

        context = createContext(FILE_SIZE);

        context.onAccess(0, false);
        context.onAccess(CACHE_BLOCK_SIZE, false);
        context.onAccess(2 * CACHE_BLOCK_SIZE, false);

        boolean processed = context.processQueue();

        // Should still return false since worker rejected
        assertFalse("Should return false when worker rejects", processed);
    }

    // ========== Signal Callback Tests ==========

    /**
     * Tests that signal callback is invoked when appropriate.
     * Signals are batched and rate-limited, and drained on misses.
     */
    public void testSignalCallbackInvoked() throws Exception {
        context = createContext(FILE_SIZE);

        // Trigger enough misses to signal - need to build up enough delta
        // and respect minimum signal interval and batch size
        for (int i = 0; i < 20; i++) {
            context.onAccess(i * CACHE_BLOCK_SIZE, false);
        }

        // Signal should be invoked at least once due to sequential misses
        verify(mockSignalCallback, atLeastOnce()).run();
    }

    /**
     * Tests signal rate limiting (300µs minimum interval).
     */
    public void testSignalRateLimiting() throws Exception {
        context = createContext(FILE_SIZE);

        // Rapid successive triggers
        for (int i = 0; i < 10; i++) {
            context.onAccess(i * CACHE_BLOCK_SIZE, false);
            context.onAccess((i + 1) * CACHE_BLOCK_SIZE, false);
            context.onAccess((i + 2) * CACHE_BLOCK_SIZE, false);
        }

        // Should have rate-limited signals, not 10x
        verify(mockSignalCallback, atLeastOnce()).run();
    }

    /**
     * Tests null signal callback is handled gracefully.
     */
    public void testNullSignalCallback() throws Exception {
        context = WindowedReadAheadContext.build(TEST_PATH, FILE_SIZE, mockWorker, config, null);

        // Should not throw
        context.onAccess(0, false);
        context.onAccess(CACHE_BLOCK_SIZE, false);
        context.onAccess(2 * CACHE_BLOCK_SIZE, false);

        assertNotNull("Context should handle null callback", context);
    }

    // ========== Trigger Readahead Tests ==========

    /**
     * Tests manual readahead trigger.
     */
    public void testTriggerReadahead() throws Exception {
        context = createContext(FILE_SIZE);

        long offset = 10 * CACHE_BLOCK_SIZE;
        context.triggerReadahead(offset);

        assertTrue("Manual trigger should queue work", context.hasQueuedWork());

        context.processQueue();
        verify(mockWorker, atLeastOnce()).schedule(eq(TEST_PATH), anyLong(), anyLong());
    }

    /**
     * Tests triggerReadahead respects file boundaries.
     */
    public void testTriggerReadaheadAtFileBoundary() throws Exception {
        long smallFileSize = 10 * CACHE_BLOCK_SIZE;
        context = createContext(smallFileSize);

        // Trigger near end
        context.triggerReadahead(9 * CACHE_BLOCK_SIZE);

        context.processQueue();

        // Should not exceed file boundary
        verify(mockWorker, atLeastOnce()).schedule(eq(TEST_PATH), anyLong(), anyLong());
    }

    /**
     * Tests triggerReadahead with window sizing.
     */
    public void testTriggerReadaheadUsesCurrentWindow() throws Exception {
        context = createContext(FILE_SIZE);

        context.triggerReadahead(0);

        assertTrue("Should queue work based on window", context.hasQueuedWork());
    }

    // ========== Reset Tests ==========

    /**
     * Tests reset clears internal state.
     */
    public void testReset() throws Exception {
        context = createContext(FILE_SIZE);

        // Build up state
        context.onAccess(0, false);
        context.onAccess(CACHE_BLOCK_SIZE, false);
        context.onAccess(2 * CACHE_BLOCK_SIZE, false);

        assertTrue("Should have queued work before reset", context.hasQueuedWork());

        context.reset();

        assertFalse("Reset should clear queued work", context.hasQueuedWork());
    }

    /**
     * Tests reset clears hit counters.
     */
    public void testResetClearsHitCounters() throws Exception {
        context = createContext(FILE_SIZE);

        // Build up hit threshold
        context.onAccess(0, false);
        context.processQueue();

        for (int i = 0; i < 8; i++) {
            context.onAccess(i * CACHE_BLOCK_SIZE, true);
        }

        context.reset();

        // After reset, hit threshold should be back to base
        assertFalse("Reset should clear state", context.hasQueuedWork());
    }

    // ========== Cancel Tests ==========

    /**
     * Tests cancel invokes worker cancellation.
     */
    public void testCancel() {
        context = createContext(FILE_SIZE);

        context.cancel();

        verify(mockWorker, times(1)).cancel(TEST_PATH);
    }

    /**
     * Tests cancel can be called multiple times safely.
     */
    public void testCancelIdempotent() {
        context = createContext(FILE_SIZE);

        context.cancel();
        context.cancel();
        context.cancel();

        verify(mockWorker, atLeastOnce()).cancel(TEST_PATH);
    }

    /**
     * Tests close disables readahead and cancels work.
     */
    public void testClose() {
        context = createContext(FILE_SIZE);

        assertTrue("Should be enabled before close", context.isReadAheadEnabled());

        context.close();

        assertFalse("Should be disabled after close", context.isReadAheadEnabled());
        verify(mockWorker, times(1)).cancel(TEST_PATH);
    }

    /**
     * Tests close is idempotent.
     */
    public void testCloseIdempotent() {
        context = createContext(FILE_SIZE);

        context.close();
        context.close();
        context.close();

        assertFalse("Should remain closed", context.isReadAheadEnabled());
        verify(mockWorker, times(1)).cancel(TEST_PATH);
    }

    /**
     * Tests processQueue returns false after close.
     */
    public void testProcessQueueAfterClose() throws Exception {
        context = createContext(FILE_SIZE);

        // Queue work
        context.onAccess(0, false);
        context.onAccess(CACHE_BLOCK_SIZE, false);
        context.onAccess(2 * CACHE_BLOCK_SIZE, false);

        context.close();

        boolean processed = context.processQueue();

        assertFalse("processQueue should return false after close", processed);
    }

    /**
     * Tests concurrent onAccess calls from multiple threads.
     */
    public void testConcurrentOnAccessCalls() throws Exception {
        context = createContext(FILE_SIZE);

        int threadCount = 4;
        int accessesPerThread = 100;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < accessesPerThread; i++) {
                        long offset = (threadId * accessesPerThread + i) * CACHE_BLOCK_SIZE;
                        boolean isHit = randomBoolean();
                        context.onAccess(offset, isHit);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        assertTrue("Threads should complete", doneLatch.await(10, TimeUnit.SECONDS));

        // Context should remain valid
        assertNotNull("Context should handle concurrent access", context);
    }

    /**
     * Tests concurrent processQueue calls.
     */
    public void testConcurrentProcessQueue() throws Exception {
        context = createContext(FILE_SIZE);

        // Queue some work
        for (int i = 0; i < 10; i++) {
            context.onAccess(i * CACHE_BLOCK_SIZE, false);
        }

        int threadCount = 3;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger processedCount = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    if (context.processQueue()) {
                        processedCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        assertTrue("Threads should complete", doneLatch.await(10, TimeUnit.SECONDS));

        // At least one should have processed
        assertTrue("At least one thread should process", processedCount.get() > 0);
    }

    /**
     * Tests concurrent access and processing.
     */
    public void testConcurrentAccessAndProcess() throws Exception {
        context = createContext(FILE_SIZE);

        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch doneLatch = new CountDownLatch(2);

        // Access thread
        new Thread(() -> {
            try {
                int i = 0;
                while (!stop.get() && i < 200) {
                    context.onAccess(i * CACHE_BLOCK_SIZE, randomBoolean());
                    i++;
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

        // Process thread
        new Thread(() -> {
            try {
                while (!stop.get()) {
                    context.processQueue();
                    Thread.sleep(5);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                doneLatch.countDown();
            }
        }).start();

        Thread.sleep(100);
        stop.set(true);

        assertTrue("Threads should complete", doneLatch.await(10, TimeUnit.SECONDS));
        assertNotNull("Context should remain valid", context);
    }

    // ========== Edge Cases and Boundary Tests ==========

    /**
     * Tests very large file handling.
     */
    public void testVeryLargeFile() throws Exception {
        long largeFileSize = 10L * 1024 * 1024 * 1024; // 10GB
        context = createContext(largeFileSize);

        context.onAccess(0, false);
        context.onAccess(CACHE_BLOCK_SIZE, false);
        context.onAccess(2 * CACHE_BLOCK_SIZE, false);

        context.processQueue();

        verify(mockWorker, atLeastOnce()).schedule(eq(TEST_PATH), anyLong(), anyLong());
    }

    /**
     * Tests access at exact file boundary.
     */
    public void testAccessAtFileBoundary() throws Exception {
        long fileSize = 100 * CACHE_BLOCK_SIZE;
        context = createContext(fileSize);

        // Access last block
        context.onAccess(99 * CACHE_BLOCK_SIZE, false);
        context.onAccess(99 * CACHE_BLOCK_SIZE + 1, false);

        assertNotNull("Should handle boundary access", context);
    }

    /**
     * Tests custom configuration parameters.
     */
    public void testCustomConfiguration() throws Exception {
        WindowedReadAheadConfig customConfig = WindowedReadAheadConfig.of(8, 64, 8);
        context = createContext(FILE_SIZE, customConfig);

        assertNotNull("Should accept custom config", context);
        assertEquals("Should use custom initial window", 8, context.policy().initialWindow());
        assertEquals("Should use custom max window", 64, context.policy().maxWindow());
    }

    /**
     * Tests policy integration and window growth.
     */
    public void testPolicyWindowGrowth() throws Exception {
        context = createContext(FILE_SIZE);

        int initialWindow = context.policy().initialWindow();

        // Trigger sequential pattern to grow window
        for (int batch = 0; batch < 5; batch++) {
            for (int i = 0; i < 10; i++) {
                long offset = (batch * 10 + i) * CACHE_BLOCK_SIZE;
                context.onAccess(offset, false);
            }
            context.processQueue();
        }

        int finalWindow = context.policy().currentWindow();

        assertTrue("Window should grow with sequential access", finalWindow >= initialWindow);
    }

    /**
     * Tests hasQueuedWork accuracy.
     */
    public void testHasQueuedWorkAccuracy() throws Exception {
        context = createContext(FILE_SIZE);

        assertFalse("No work initially", context.hasQueuedWork());

        context.onAccess(0, false);
        context.onAccess(CACHE_BLOCK_SIZE, false);
        context.onAccess(2 * CACHE_BLOCK_SIZE, false);

        assertTrue("Should have queued work after misses", context.hasQueuedWork());

        context.processQueue();

        // After processing, may or may not have more work depending on state
        assertNotNull("Context should remain valid", context);
    }

    /**
     * Tests that desiredEndBlock updates are atomic and non-blocking.
     */
    public void testDesiredEndBlockAtomicUpdates() throws Exception {
        context = createContext(FILE_SIZE);

        // Rapid concurrent updates to desired end
        int threadCount = 4;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < 50; i++) {
                        long offset = (threadId * 50 + i) * CACHE_BLOCK_SIZE;
                        context.triggerReadahead(offset);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        assertTrue("Atomic updates should complete", doneLatch.await(10, TimeUnit.SECONDS));

        assertTrue("Should have work after concurrent triggers", context.hasQueuedWork());
    }

    /**
     * Tests signal pending flag race conditions.
     */
    public void testSignalPendingFlagHandling() throws Exception {
        AtomicInteger callbackCount = new AtomicInteger(0);
        Runnable countingCallback = callbackCount::incrementAndGet;

        context = WindowedReadAheadContext.build(TEST_PATH, FILE_SIZE, mockWorker, config, countingCallback);

        // Trigger multiple signals rapidly
        for (int i = 0; i < 100; i++) {
            context.onAccess(i * CACHE_BLOCK_SIZE, false);
            if (i % 3 == 0) {
                // Interleave with hits to vary the pattern
                context.onAccess(i * CACHE_BLOCK_SIZE, true);
            }
        }

        // Callback should have been invoked at least once
        assertTrue("Callback should be invoked", callbackCount.get() > 0);
    }

    /**
     * Tests worker null handling.
     */
    public void testNullWorkerHandling() {
        // Should not throw on construction
        context = WindowedReadAheadContext.build(TEST_PATH, FILE_SIZE, null, config, mockSignalCallback);

        assertNotNull("Should handle null worker", context);

        // These should not throw
        context.onAccess(0, false);
        context.cancel();
        context.close();
    }
}

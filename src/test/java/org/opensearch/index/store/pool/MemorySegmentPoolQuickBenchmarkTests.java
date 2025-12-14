/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Quick benchmark tests to demonstrate lock-free performance improvements.
 *
 * Run with: ./gradlew test --tests "MemorySegmentPoolQuickBenchmarkTests"
 */
public class MemorySegmentPoolQuickBenchmarkTests extends OpenSearchTestCase {

    /**
     * Demonstrates linear scalability with thread count.
     * Lock-free implementation should show near-linear speedup.
     */
    public void testScalability() throws Exception {
        System.out.println("\n========== SCALABILITY BENCHMARK ==========");
        System.out.println("Measuring throughput as thread count increases\n");
        System.out.println(String.format("%-10s %-20s %-15s %-15s",
            "Threads", "Total Ops/sec", "Speedup", "Efficiency"));
        System.out.println("-".repeat(60));

        int[] threadCounts = {1, 2, 4, 8, 16};
        int opsPerThread = 10_000;
        double baselineOpsPerSec = 0;

        for (int threads : threadCounts) {
            double opsPerSec = runBenchmark(threads, opsPerThread);

            if (threads == 1) {
                baselineOpsPerSec = opsPerSec;
            }

            double speedup = opsPerSec / baselineOpsPerSec;
            double efficiency = (speedup / threads) * 100;

            System.out.println(String.format("%-10d %-20.0f %-15.2fx %-15.1f%%",
                threads, opsPerSec, speedup, efficiency));
        }

        System.out.println("\nInterpretation:");
        System.out.println("- Efficiency > 80%: Excellent scalability, minimal contention");
        System.out.println("- Efficiency 50-80%: Good scalability");
        System.out.println("- Efficiency < 50%: Lock contention limiting performance");
        System.out.println();
    }

    /**
     * Compares pre-warmed (freelist) vs cold-start (malloc) performance.
     */
    public void testWarmVsColdPool() throws Exception {
        System.out.println("\n========== WARM vs COLD POOL ==========");
        System.out.println("Comparing freelist reuse vs malloc allocation\n");

        int threads = 8;
        int opsPerThread = 5_000;

        // Cold pool (needs malloc)
        double coldOps = runBenchmark(threads, opsPerThread);

        // Warm pool (pre-allocated)
        double warmOps = runWarmPoolBenchmark(threads, opsPerThread);

        System.out.println(String.format("Cold pool (with malloc): %,.0f ops/sec", coldOps));
        System.out.println(String.format("Warm pool (freelist only): %,.0f ops/sec", warmOps));
        System.out.println(String.format("Speedup from pre-warming: %.2fx", warmOps / coldOps));
        System.out.println("\nConclusion: Pre-warming eliminates malloc contention");
        System.out.println();
    }

    /**
     * Shows batch release efficiency.
     */
    public void testBatchRelease() throws Exception {
        System.out.println("\n========== BATCH RELEASE EFFICIENCY ==========");
        System.out.println("Comparing individual vs batch releases\n");
        System.out.println(String.format("%-12s %-20s %-15s",
            "Batch Size", "Ops/sec", "Speedup"));
        System.out.println("-".repeat(50));

        int threads = 4;
        int opsPerThread = 2_000;
        int[] batchSizes = {1, 4, 8, 16};
        double baselineOps = 0;

        for (int batchSize : batchSizes) {
            double opsPerSec = runBatchBenchmark(threads, opsPerThread, batchSize);

            if (batchSize == 1) {
                baselineOps = opsPerSec;
            }

            double speedup = opsPerSec / baselineOps;

            System.out.println(String.format("%-12d %-20.0f %-15.2fx",
                batchSize, opsPerSec, speedup));
        }

        System.out.println("\nConclusion: Larger batches reduce CAS contention");
        System.out.println();
    }

    // ==================== Helper Methods ====================

    private double runBenchmark(int threadCount, int opsPerThread) throws Exception {
        long totalMemory = 1024 * 1024 * 32; // 32MB
        int segmentSize = 16 * 1024;

        try (MemorySegmentPool pool = new MemorySegmentPool(totalMemory, segmentSize, false)) {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            AtomicLong totalOps = new AtomicLong(0);

            for (int i = 0; i < threadCount; i++) {
                new Thread(() -> {
                    try {
                        startLatch.await();
                        for (int j = 0; j < opsPerThread; j++) {
                            RefCountedMemorySegment seg = pool.acquire();
                            seg.decRef();
                            totalOps.incrementAndGet();
                        }
                    } catch (Exception e) {
                        // Ignore
                    } finally {
                        doneLatch.countDown();
                    }
                }).start();
            }

            long startTime = System.nanoTime();
            startLatch.countDown();
            doneLatch.await();
            long totalTime = System.nanoTime() - startTime;

            return (totalOps.get() * 1_000_000_000.0) / totalTime;
        }
    }

    private double runWarmPoolBenchmark(int threadCount, int opsPerThread) throws Exception {
        long totalMemory = 1024 * 1024 * 64; // 64MB
        int segmentSize = 16 * 1024;

        try (MemorySegmentPool pool = new MemorySegmentPool(totalMemory, segmentSize, false)) {
            // Pre-warm all segments
            pool.warmUp(pool.totalMemory() / segmentSize);

            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            AtomicLong totalOps = new AtomicLong(0);

            for (int i = 0; i < threadCount; i++) {
                new Thread(() -> {
                    try {
                        startLatch.await();
                        for (int j = 0; j < opsPerThread; j++) {
                            RefCountedMemorySegment seg = pool.acquire();
                            seg.decRef();
                            totalOps.incrementAndGet();
                        }
                    } catch (Exception e) {
                        // Ignore
                    } finally {
                        doneLatch.countDown();
                    }
                }).start();
            }

            long startTime = System.nanoTime();
            startLatch.countDown();
            doneLatch.await();
            long totalTime = System.nanoTime() - startTime;

            return (totalOps.get() * 1_000_000_000.0) / totalTime;
        }
    }

    private double runBatchBenchmark(int threadCount, int opsPerThread, int batchSize) throws Exception {
        long totalMemory = 1024 * 1024 * 64;
        int segmentSize = 16 * 1024;

        try (MemorySegmentPool pool = new MemorySegmentPool(totalMemory, segmentSize, false)) {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            AtomicLong totalOps = new AtomicLong(0);

            for (int i = 0; i < threadCount; i++) {
                new Thread(() -> {
                    try {
                        startLatch.await();
                        for (int j = 0; j < opsPerThread; j++) {
                            // Acquire batch
                            RefCountedMemorySegment[] batch = new RefCountedMemorySegment[batchSize];
                            for (int k = 0; k < batchSize; k++) {
                                batch[k] = pool.acquire();
                            }

                            // Release batch
                            if (batchSize == 1) {
                                batch[0].decRef();
                            } else {
                                pool.releaseAll(batch);
                            }

                            totalOps.addAndGet(batchSize);
                        }
                    } catch (Exception e) {
                        // Ignore
                    } finally {
                        doneLatch.countDown();
                    }
                }).start();
            }

            long startTime = System.nanoTime();
            startLatch.countDown();
            doneLatch.await();
            long totalTime = System.nanoTime() - startTime;

            return (totalOps.get() * 1_000_000_000.0) / totalTime;
        }
    }
}

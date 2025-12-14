/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Benchmark tests to measure contention and throughput of MemorySegmentPool
 * under various concurrent access patterns.
 *
 * Run with: ./gradlew test --tests "MemorySegmentPoolConcurrencyBenchmark" -Dtests.output=always
 *
 * These tests help evaluate:
 * 1. Lock contention on acquire path
 * 2. Lock contention on release path
 * 3. Cache line bouncing effects
 * 4. Throughput scalability with thread count
 * 5. Latency distribution (avg, p50, p99)
 */
public class MemorySegmentPoolConcurrencyBenchmarkTests extends OpenSearchTestCase {

    private static final int WARMUP_ITERATIONS = 3;
    private static final int MEASUREMENT_ITERATIONS = 5;
    private static final boolean PRINT_OUTPUT = true;

    /**
     * Benchmark: Pure acquire/release pairs to measure lock contention.
     * This simulates cache miss scenario where threads need to acquire from pool.
     */
    public void testBenchmarkAcquireReleaseContention() throws Exception {
        int[] threadCounts = {1, 2, 4, 8, 16};
        int operationsPerThread = 50_000;

        printHeader("Acquire/Release Contention Benchmark");
        System.out.println("Operations per thread: " + operationsPerThread);
        System.out.println(String.format("%-10s %-15s %-15s %-15s %-15s %-15s",
            "Threads", "Ops/sec", "Avg(ns)", "P50(ns)", "P99(ns)", "Speedup"));
        System.out.println("=".repeat(100));

        double baselineOpsPerSec = 0;

        for (int threadCount : threadCounts) {
            long totalMemory = 1024 * 1024 * 32; // 32MB
            int segmentSize = 16 * 1024; // 16KB

            // Warmup
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                runAcquireReleaseBenchmark(threadCount, operationsPerThread / 10, totalMemory, segmentSize);
            }

            // Measurement
            BenchmarkResult result = new BenchmarkResult();
            for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
                BenchmarkResult iterResult = runAcquireReleaseBenchmark(
                    threadCount, operationsPerThread, totalMemory, segmentSize);
                result.merge(iterResult);
            }

            double opsPerSec = result.getOpsPerSecond();
            if (threadCount == 1) {
                baselineOpsPerSec = opsPerSec;
            }
            double speedup = baselineOpsPerSec > 0 ? opsPerSec / baselineOpsPerSec : 1.0;

            System.out.println(String.format("%-10d %-15.0f %-15d %-15d %-15d %-15.2fx",
                threadCount,
                opsPerSec,
                result.getAvgLatencyNs(),
                result.getP50LatencyNs(),
                result.getP99LatencyNs(),
                speedup));
        }
        System.out.println();
    }

    /**
     * Benchmark: Acquire with pre-warmed pool (freelist hits only).
     * Measures pure freelist contention without malloc overhead.
     */
    public void testBenchmarkFreelistContention() throws Exception {
        int[] threadCounts = {1, 2, 4, 8, 16};
        int operationsPerThread = 100_000;

        printHeader("Freelist-Only Contention Benchmark (Pre-warmed Pool)");
        System.out.println("Operations per thread: " + operationsPerThread);
        System.out.println(String.format("%-10s %-15s %-15s %-15s %-15s",
            "Threads", "Ops/sec", "Avg(ns)", "P99(ns)", "Speedup"));
        System.out.println("=".repeat(80));

        double baselineOpsPerSec = 0;

        for (int threadCount : threadCounts) {
            long totalMemory = 1024 * 1024 * 64; // 64MB - plenty for all threads
            int segmentSize = 16 * 1024;

            try (MemorySegmentPool pool = new MemorySegmentPool(totalMemory, segmentSize, false)) {
                // Pre-warm: allocate all segments
                pool.warmUp(pool.totalMemory() / segmentSize);

                // Warmup
                for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                    runFreelistBenchmark(pool, threadCount, operationsPerThread / 10);
                }

                // Measurement
                BenchmarkResult result = new BenchmarkResult();
                for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
                    BenchmarkResult iterResult = runFreelistBenchmark(pool, threadCount, operationsPerThread);
                    result.merge(iterResult);
                }

                double opsPerSec = result.getOpsPerSecond();
                if (threadCount == 1) {
                    baselineOpsPerSec = opsPerSec;
                }
                double speedup = baselineOpsPerSec > 0 ? opsPerSec / baselineOpsPerSec : 1.0;

                System.out.println(String.format("%-10d %-15.0f %-15d %-15d %-15.2fx",
                    threadCount,
                    opsPerSec,
                    result.getAvgLatencyNs(),
                    result.getP99LatencyNs(),
                    speedup));
            }
        }
        System.out.println();
    }

    /**
     * Benchmark: Mixed acquire/release pattern simulating real workload.
     * Some threads mostly acquire (readers), some mostly release (writers).
     */
    public void testBenchmarkMixedWorkload() throws Exception {
        int[] totalThreadCounts = {4, 8, 16};
        int operationsPerThread = 50_000;

        printHeader("Mixed Workload Benchmark (75% Acquirers, 25% Releasers)");
        System.out.println("Operations per thread: " + operationsPerThread);
        System.out.println(String.format("%-15s %-20s %-20s",
            "Total Threads", "Acquire Ops/sec", "Release Ops/sec"));
        System.out.println("=".repeat(60));

        for (int totalThreads : totalThreadCounts) {
            int acquireThreads = (int)(totalThreads * 0.75);
            int releaseThreads = totalThreads - acquireThreads;

            long totalMemory = 1024 * 1024 * 32;
            int segmentSize = 16 * 1024;

            // Warmup
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                runMixedWorkloadBenchmark(acquireThreads, releaseThreads,
                    operationsPerThread / 10, totalMemory, segmentSize);
            }

            // Measurement
            MixedWorkloadResult result = new MixedWorkloadResult();
            for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
                MixedWorkloadResult iterResult = runMixedWorkloadBenchmark(
                    acquireThreads, releaseThreads, operationsPerThread, totalMemory, segmentSize);
                result.merge(iterResult);
            }

            System.out.println(String.format("%-15d %-20.0f %-20.0f",
                totalThreads,
                result.acquireResult.getOpsPerSecond(),
                result.releaseResult.getOpsPerSecond()));
        }
        System.out.println();
    }

    /**
     * Benchmark: Batch release (releaseAll) vs individual releases.
     * Measures efficiency of batch operations.
     */
    public void testBenchmarkBatchRelease() throws Exception {
        int threadCount = 8;
        int[] batchSizes = {1, 2, 4, 8, 16, 32};
        int operationsPerThread = 10_000;

        printHeader("Batch Release Benchmark");
        System.out.println("Threads: " + threadCount + ", Operations per thread: " + operationsPerThread);
        System.out.println(String.format("%-12s %-15s %-15s %-15s %-15s",
            "Batch Size", "Ops/sec", "Avg(ns)", "P99(ns)", "Speedup"));
        System.out.println("=".repeat(80));

        double baselineOpsPerSec = 0;

        for (int batchSize : batchSizes) {
            long totalMemory = 1024 * 1024 * 64;
            int segmentSize = 16 * 1024;

            // Warmup
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                runBatchReleaseBenchmark(threadCount, operationsPerThread / 10,
                    batchSize, totalMemory, segmentSize);
            }

            // Measurement
            BenchmarkResult result = new BenchmarkResult();
            for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
                BenchmarkResult iterResult = runBatchReleaseBenchmark(
                    threadCount, operationsPerThread, batchSize, totalMemory, segmentSize);
                result.merge(iterResult);
            }

            double opsPerSec = result.getOpsPerSecond();
            if (batchSize == 1) {
                baselineOpsPerSec = opsPerSec;
            }
            double speedup = baselineOpsPerSec > 0 ? opsPerSec / baselineOpsPerSec : 1.0;

            System.out.println(String.format("%-12d %-15.0f %-15d %-15d %-15.2fx",
                batchSize,
                opsPerSec,
                result.getAvgLatencyNs(),
                result.getP99LatencyNs(),
                speedup));
        }
        System.out.println();
    }

    /**
     * Benchmark: Latency variance as proxy for cache line bouncing.
     * Higher coefficient of variation indicates more cache contention.
     */
    public void testBenchmarkLatencyVariance() throws Exception {
        int threadCount = 16;
        int operationsPerThread = 100_000;

        printHeader("Latency Variance Benchmark (Cache Line Bouncing Indicator)");
        System.out.println("Threads: " + threadCount + ", Operations per thread: " + operationsPerThread);
        System.out.println("Note: Higher Coeff of Variation indicates more cache line bouncing");
        System.out.println(String.format("%-25s %-15s %-15s %-15s %-15s",
            "Test", "Avg(ns)", "Std Dev(ns)", "Coeff Var", "Min/Max(ns)"));
        System.out.println("=".repeat(90));

        long totalMemory = 1024 * 1024 * 32;
        int segmentSize = 16 * 1024;

        // Test 1: Acquire-only (all threads contend on acquire path)
        BenchmarkResult acquireOnly = new BenchmarkResult();
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            BenchmarkResult iterResult = runAcquireOnlyBenchmark(
                threadCount, operationsPerThread, totalMemory, segmentSize);
            acquireOnly.merge(iterResult);
        }

        System.out.println(String.format("%-25s %-15d %-15d %-15.2f%% %-15s",
            "Acquire-only",
            acquireOnly.getAvgLatencyNs(),
            acquireOnly.getStdDevLatencyNs(),
            acquireOnly.getCoefficientOfVariation() * 100,
            acquireOnly.getMinLatencyNs() + "/" + acquireOnly.getMaxLatencyNs()));

        // Test 2: Alternating acquire/release (cache line ping-pong)
        BenchmarkResult alternating = new BenchmarkResult();
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            BenchmarkResult iterResult = runAcquireReleaseBenchmark(
                threadCount, operationsPerThread, totalMemory, segmentSize);
            alternating.merge(iterResult);
        }

        System.out.println(String.format("%-25s %-15d %-15d %-15.2f%% %-15s",
            "Acquire/Release",
            alternating.getAvgLatencyNs(),
            alternating.getStdDevLatencyNs(),
            alternating.getCoefficientOfVariation() * 100,
            alternating.getMinLatencyNs() + "/" + alternating.getMaxLatencyNs()));

        System.out.println();
    }

    /**
     * Benchmark: Scalability test showing how throughput changes with thread count.
     */
    public void testBenchmarkScalability() throws Exception {
        int[] threadCounts = {1, 2, 4, 6, 8, 12, 16, 24, 32};
        int operationsPerThread = 50_000;

        printHeader("Scalability Benchmark");
        System.out.println("Operations per thread: " + operationsPerThread);
        System.out.println(String.format("%-10s %-15s %-15s %-15s %-15s",
            "Threads", "Total Ops/sec", "Per-Thread Ops", "Speedup", "Efficiency"));
        System.out.println("=".repeat(80));

        long totalMemory = 1024 * 1024 * 64;
        int segmentSize = 16 * 1024;

        double baselineOpsPerSec = 0;

        for (int threadCount : threadCounts) {
            // Warmup
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                runAcquireReleaseBenchmark(threadCount, operationsPerThread / 10, totalMemory, segmentSize);
            }

            // Measurement
            BenchmarkResult result = new BenchmarkResult();
            for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
                BenchmarkResult iterResult = runAcquireReleaseBenchmark(
                    threadCount, operationsPerThread, totalMemory, segmentSize);
                result.merge(iterResult);
            }

            double totalOpsPerSec = result.getOpsPerSecond();
            double perThreadOps = totalOpsPerSec / threadCount;

            if (threadCount == 1) {
                baselineOpsPerSec = totalOpsPerSec;
            }
            double speedup = baselineOpsPerSec > 0 ? totalOpsPerSec / baselineOpsPerSec : 1.0;
            double efficiency = speedup / threadCount * 100;

            System.out.println(String.format("%-10d %-15.0f %-15.0f %-15.2fx %-15.1f%%",
                threadCount,
                totalOpsPerSec,
                perThreadOps,
                speedup,
                efficiency));
        }
        System.out.println();
    }

    // ==================== Helper Methods ====================

    private BenchmarkResult runAcquireReleaseBenchmark(
            int threadCount, int opsPerThread, long totalMemory, int segmentSize) throws Exception {

        try (MemorySegmentPool pool = new MemorySegmentPool(totalMemory, segmentSize, false)) {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);

            ConcurrentLinkedQueue<Long> allLatencies = new ConcurrentLinkedQueue<>();
            AtomicLong totalOps = new AtomicLong(0);

            for (int i = 0; i < threadCount; i++) {
                Thread t = new Thread(() -> {
                    List<Long> latencies = new ArrayList<>(opsPerThread);
                    try {
                        startLatch.await();
                        for (int j = 0; j < opsPerThread; j++) {
                            long start = System.nanoTime();
                            RefCountedMemorySegment seg = pool.acquire();
                            seg.decRef();
                            long latency = System.nanoTime() - start;
                            latencies.add(latency);
                            totalOps.incrementAndGet();
                        }
                        allLatencies.addAll(latencies);
                    } catch (Exception e) {
                        // Ignore for benchmark
                    } finally {
                        doneLatch.countDown();
                    }
                });
                t.start();
            }

            long startTime = System.nanoTime();
            startLatch.countDown();
            doneLatch.await();
            long totalTime = System.nanoTime() - startTime;

            return new BenchmarkResult(new ArrayList<>(allLatencies), totalOps.get(), totalTime);
        }
    }

    private BenchmarkResult runFreelistBenchmark(
            MemorySegmentPool pool, int threadCount, int opsPerThread) throws Exception {

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        ConcurrentLinkedQueue<Long> allLatencies = new ConcurrentLinkedQueue<>();
        AtomicLong totalOps = new AtomicLong(0);

        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(() -> {
                List<Long> latencies = new ArrayList<>(opsPerThread);
                try {
                    startLatch.await();
                    for (int j = 0; j < opsPerThread; j++) {
                        long start = System.nanoTime();
                        RefCountedMemorySegment seg = pool.acquire();
                        long latency = System.nanoTime() - start;
                        latencies.add(latency);
                        seg.decRef();
                        totalOps.incrementAndGet();
                    }
                    allLatencies.addAll(latencies);
                } catch (Exception e) {
                    // Ignore
                } finally {
                    doneLatch.countDown();
                }
            });
            t.start();
        }

        long startTime = System.nanoTime();
        startLatch.countDown();
        doneLatch.await();
        long totalTime = System.nanoTime() - startTime;

        return new BenchmarkResult(new ArrayList<>(allLatencies), totalOps.get(), totalTime);
    }

    private MixedWorkloadResult runMixedWorkloadBenchmark(
            int acquireThreads, int releaseThreads, int opsPerThread,
            long totalMemory, int segmentSize) throws Exception {

        try (MemorySegmentPool pool = new MemorySegmentPool(totalMemory, segmentSize, false)) {
            // Pre-populate with some segments
            pool.warmUp(acquireThreads);

            BlockingQueue<RefCountedMemorySegment> sharedQueue = new LinkedBlockingQueue<>();
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(acquireThreads + releaseThreads);

            ConcurrentLinkedQueue<Long> acquireLatencies = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<Long> releaseLatencies = new ConcurrentLinkedQueue<>();
            AtomicLong acquireOps = new AtomicLong(0);
            AtomicLong releaseOps = new AtomicLong(0);

            // Acquire threads
            for (int i = 0; i < acquireThreads; i++) {
                Thread t = new Thread(() -> {
                    try {
                        startLatch.await();
                        for (int j = 0; j < opsPerThread; j++) {
                            long start = System.nanoTime();
                            RefCountedMemorySegment seg = pool.acquire();
                            long latency = System.nanoTime() - start;
                            acquireLatencies.add(latency);
                            acquireOps.incrementAndGet();
                            sharedQueue.offer(seg);
                        }
                    } catch (Exception e) {
                        // Ignore
                    } finally {
                        doneLatch.countDown();
                    }
                });
                t.start();
            }

            // Release threads
            for (int i = 0; i < releaseThreads; i++) {
                Thread t = new Thread(() -> {
                    try {
                        startLatch.await();
                        int expectedOps = (acquireThreads * opsPerThread) / releaseThreads;
                        for (int j = 0; j < expectedOps; j++) {
                            RefCountedMemorySegment seg = sharedQueue.poll(1, TimeUnit.SECONDS);
                            if (seg != null) {
                                long start = System.nanoTime();
                                seg.decRef();
                                long latency = System.nanoTime() - start;
                                releaseLatencies.add(latency);
                                releaseOps.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        // Ignore
                    } finally {
                        doneLatch.countDown();
                    }
                });
                t.start();
            }

            long startTime = System.nanoTime();
            startLatch.countDown();
            doneLatch.await(30, TimeUnit.SECONDS);
            long totalTime = System.nanoTime() - startTime;

            return new MixedWorkloadResult(
                new BenchmarkResult(new ArrayList<>(acquireLatencies), acquireOps.get(), totalTime),
                new BenchmarkResult(new ArrayList<>(releaseLatencies), releaseOps.get(), totalTime)
            );
        }
    }

    private BenchmarkResult runBatchReleaseBenchmark(
            int threadCount, int opsPerThread, int batchSize,
            long totalMemory, int segmentSize) throws Exception {

        try (MemorySegmentPool pool = new MemorySegmentPool(totalMemory, segmentSize, false)) {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);

            ConcurrentLinkedQueue<Long> allLatencies = new ConcurrentLinkedQueue<>();
            AtomicLong totalOps = new AtomicLong(0);

            for (int i = 0; i < threadCount; i++) {
                Thread t = new Thread(() -> {
                    List<Long> latencies = new ArrayList<>(opsPerThread);
                    try {
                        startLatch.await();
                        for (int j = 0; j < opsPerThread; j++) {
                            // Acquire batch
                            RefCountedMemorySegment[] batch = new RefCountedMemorySegment[batchSize];
                            for (int k = 0; k < batchSize; k++) {
                                batch[k] = pool.acquire();
                            }

                            // Release batch
                            long start = System.nanoTime();
                            if (batchSize == 1) {
                                batch[0].decRef();
                            } else {
                                pool.releaseAll(batch);
                            }
                            long latency = System.nanoTime() - start;
                            latencies.add(latency);
                            totalOps.addAndGet(batchSize);
                        }
                        allLatencies.addAll(latencies);
                    } catch (Exception e) {
                        // Ignore
                    } finally {
                        doneLatch.countDown();
                    }
                });
                t.start();
            }

            long startTime = System.nanoTime();
            startLatch.countDown();
            doneLatch.await();
            long totalTime = System.nanoTime() - startTime;

            return new BenchmarkResult(new ArrayList<>(allLatencies), totalOps.get(), totalTime);
        }
    }

    private BenchmarkResult runAcquireOnlyBenchmark(
            int threadCount, int opsPerThread, long totalMemory, int segmentSize) throws Exception {

        try (MemorySegmentPool pool = new MemorySegmentPool(totalMemory, segmentSize, false)) {
            // Pre-warm pool
            pool.warmUp(threadCount * 10);

            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);

            ConcurrentLinkedQueue<Long> allLatencies = new ConcurrentLinkedQueue<>();
            AtomicLong totalOps = new AtomicLong(0);

            for (int i = 0; i < threadCount; i++) {
                Thread t = new Thread(() -> {
                    List<Long> latencies = new ArrayList<>(opsPerThread);
                    List<RefCountedMemorySegment> held = new ArrayList<>();
                    try {
                        startLatch.await();
                        for (int j = 0; j < opsPerThread; j++) {
                            long start = System.nanoTime();
                            RefCountedMemorySegment seg = pool.acquire();
                            long latency = System.nanoTime() - start;
                            latencies.add(latency);
                            held.add(seg);
                            totalOps.incrementAndGet();
                        }
                        allLatencies.addAll(latencies);
                        // Release all at end
                        for (RefCountedMemorySegment seg : held) {
                            seg.decRef();
                        }
                    } catch (Exception e) {
                        // Ignore
                    } finally {
                        doneLatch.countDown();
                    }
                });
                t.start();
            }

            long startTime = System.nanoTime();
            startLatch.countDown();
            doneLatch.await();
            long totalTime = System.nanoTime() - startTime;

            return new BenchmarkResult(new ArrayList<>(allLatencies), totalOps.get(), totalTime);
        }
    }

    private void printHeader(String title) {
        System.out.println();
        System.out.println("=".repeat(100));
        System.out.println(title);
        System.out.println("=".repeat(100));
    }

    // ==================== Result Classes ====================

    private static class BenchmarkResult {
        private final List<Long> latencies;
        private final long totalOps;
        private final long totalTimeNs;

        BenchmarkResult() {
            this.latencies = new ArrayList<>();
            this.totalOps = 0;
            this.totalTimeNs = 0;
        }

        BenchmarkResult(List<Long> latencies, long totalOps, long totalTimeNs) {
            this.latencies = new ArrayList<>(latencies);
            this.totalOps = totalOps;
            this.totalTimeNs = totalTimeNs;
        }

        void merge(BenchmarkResult other) {
            this.latencies.addAll(other.latencies);
        }

        double getOpsPerSecond() {
            if (totalTimeNs == 0) return 0;
            return (totalOps * 1_000_000_000.0) / totalTimeNs;
        }

        long getAvgLatencyNs() {
            if (latencies.isEmpty()) return 0;
            return (long) latencies.stream().mapToLong(l -> l).average().orElse(0);
        }

        long getP50LatencyNs() {
            return getPercentile(0.50);
        }

        long getP99LatencyNs() {
            return getPercentile(0.99);
        }

        long getMinLatencyNs() {
            if (latencies.isEmpty()) return 0;
            return latencies.stream().mapToLong(l -> l).min().orElse(0);
        }

        long getMaxLatencyNs() {
            if (latencies.isEmpty()) return 0;
            return latencies.stream().mapToLong(l -> l).max().orElse(0);
        }

        private long getPercentile(double percentile) {
            if (latencies.isEmpty()) return 0;
            List<Long> sorted = new ArrayList<>(latencies);
            sorted.sort(Long::compareTo);
            int index = (int) (sorted.size() * percentile);
            return sorted.get(Math.min(index, sorted.size() - 1));
        }

        long getStdDevLatencyNs() {
            if (latencies.isEmpty()) return 0;
            double avg = getAvgLatencyNs();
            double variance = latencies.stream()
                .mapToDouble(l -> Math.pow(l - avg, 2))
                .average()
                .orElse(0);
            return (long) Math.sqrt(variance);
        }

        double getCoefficientOfVariation() {
            long avg = getAvgLatencyNs();
            if (avg == 0) return 0;
            return (double) getStdDevLatencyNs() / avg;
        }
    }

    private static class MixedWorkloadResult {
        final BenchmarkResult acquireResult;
        final BenchmarkResult releaseResult;

        MixedWorkloadResult() {
            this.acquireResult = new BenchmarkResult();
            this.releaseResult = new BenchmarkResult();
        }

        MixedWorkloadResult(BenchmarkResult acquireResult, BenchmarkResult releaseResult) {
            this.acquireResult = acquireResult;
            this.releaseResult = releaseResult;
        }

        void merge(MixedWorkloadResult other) {
            this.acquireResult.merge(other.acquireResult);
            this.releaseResult.merge(other.releaseResult);
        }
    }
}

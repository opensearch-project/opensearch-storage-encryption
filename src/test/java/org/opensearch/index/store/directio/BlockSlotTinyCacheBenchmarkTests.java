/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;

/**
 * Benchmark for BlockSlotTinyCache comparing different synchronization strategies:
 * 1. ThreadLocal (current implementation)
 * 2. VarHandle with acquire/release
 * 3. Volatile
 * 4. No synchronization (baseline - unsafe)
 *
 * Run with: ./gradlew test --tests BlockSlotTinyCacheBenchmarkTests
 */
public class BlockSlotTinyCacheBenchmarkTests {

    private static final int WARMUP_ITERATIONS = 5;
    private static final int MEASURED_ITERATIONS = 10;
    private static final int OPS_PER_ITERATION = 1_000_000;
    private static final int THREAD_COUNTS[] = { 1, 2, 4, 8, 16 };
    private static final int BLOCK_SIZE = 8192;

    /**
     * Mock BlockCache for testing
     */
    static class MockBlockCache implements BlockCache<RefCountedMemorySegment> {
        private final ConcurrentHashMap<Long, BlockCacheValue<RefCountedMemorySegment>> cache = new ConcurrentHashMap<>();
        private final Arena arena = Arena.ofShared();
        private final AtomicInteger generation = new AtomicInteger(0);

        @Override
        public BlockCacheValue<RefCountedMemorySegment> get(org.opensearch.index.store.block_cache.BlockCacheKey key) {
            FileBlockCacheKey fileKey = (FileBlockCacheKey) key;
            return cache.computeIfAbsent(fileKey.fileOffset(), offset -> {
                MemorySegment segment = arena.allocate(BLOCK_SIZE);
                // Create with no-op releaser for benchmark
                RefCountedMemorySegment refCounted = new RefCountedMemorySegment(segment, BLOCK_SIZE, seg -> {});
                return new MockBlockCacheValue(refCounted);
            });
        }

        @Override
        public BlockCacheValue<RefCountedMemorySegment> getOrLoad(org.opensearch.index.store.block_cache.BlockCacheKey key)
            throws IOException {
            return get(key);
        }

        @Override
        public void prefetch(org.opensearch.index.store.block_cache.BlockCacheKey key) {
            // No-op for benchmark
        }

        @Override
        public void put(org.opensearch.index.store.block_cache.BlockCacheKey key, BlockCacheValue<RefCountedMemorySegment> value) {
            FileBlockCacheKey fileKey = (FileBlockCacheKey) key;
            cache.put(fileKey.fileOffset(), value);
        }

        @Override
        public void invalidate(org.opensearch.index.store.block_cache.BlockCacheKey key) {
            FileBlockCacheKey fileKey = (FileBlockCacheKey) key;
            cache.remove(fileKey.fileOffset());
        }

        @Override
        public void invalidate(Path normalizedFilePath) {
            cache.clear();
        }

        @Override
        public void clear() {
            cache.clear();
        }

        @Override
        public Map<org.opensearch.index.store.block_cache.BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> loadBulk(
            Path filePath,
            long startOffset,
            long blockCount
        ) throws IOException {
            Map<org.opensearch.index.store.block_cache.BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> result =
                new ConcurrentHashMap<>();
            for (long i = 0; i < blockCount; i++) {
                long offset = startOffset + (i * BLOCK_SIZE);
                FileBlockCacheKey key = new FileBlockCacheKey(filePath, offset);
                BlockCacheValue<RefCountedMemorySegment> value = getOrLoad(key);
                result.put(key, value);
            }
            return result;
        }

        @Override
        public String cacheStats() {
            return "MockCache[size=" + cache.size() + "]";
        }

        @Override
        public void recordStats() {
            // No-op for benchmark
        }

        public void close() {
            arena.close();
        }
    }

    /**
     * Mock BlockCacheValue for testing - delegates to RefCountedMemorySegment
     */
    static class MockBlockCacheValue implements BlockCacheValue<RefCountedMemorySegment> {
        private final RefCountedMemorySegment segment;

        MockBlockCacheValue(RefCountedMemorySegment segment) {
            this.segment = segment;
        }

        @Override
        public RefCountedMemorySegment value() {
            return segment;
        }

        @Override
        public boolean tryPin() {
            return segment.tryPin();
        }

        @Override
        public void unpin() {
            segment.unpin();
        }

        @Override
        public int length() {
            return BLOCK_SIZE;
        }

        @Override
        public void close() {
            segment.close();
        }

        @Override
        public void decRef() {
            segment.decRef();
        }
    }

    /**
     * Access pattern for benchmark
     */
    enum AccessPattern {
        SEQUENTIAL,      // Sequential access (best case for ThreadLocal)
        RANDOM,          // Random access
        STRIDED,         // Strided access (stride of 8 blocks)
        HOT_BLOCKS       // 80/20 rule - 20% of blocks accessed 80% of time
    }

    /**
     * Benchmark scenario
     */
    static class BenchmarkScenario {
        final String name;
        final int threadCount;
        final AccessPattern pattern;
        final int uniqueBlocks;

        BenchmarkScenario(String name, int threadCount, AccessPattern pattern, int uniqueBlocks) {
            this.name = name;
            this.threadCount = threadCount;
            this.pattern = pattern;
            this.uniqueBlocks = uniqueBlocks;
        }
    }

    /**
     * Generates block offsets based on access pattern
     */
    static class OffsetGenerator {
        private final AccessPattern pattern;
        private final int uniqueBlocks;
        private final Random random;
        private int sequentialIndex = 0;

        OffsetGenerator(AccessPattern pattern, int uniqueBlocks, long seed) {
            this.pattern = pattern;
            this.uniqueBlocks = uniqueBlocks;
            this.random = new Random(seed);
        }

        long nextOffset() {
            return switch (pattern) {
                case SEQUENTIAL -> {
                    long offset = (sequentialIndex % uniqueBlocks) * BLOCK_SIZE;
                    sequentialIndex++;
                    yield offset;
                }
                case RANDOM -> random.nextInt(uniqueBlocks) * BLOCK_SIZE;
                case STRIDED -> {
                    long offset = ((sequentialIndex * 8) % uniqueBlocks) * BLOCK_SIZE;
                    sequentialIndex++;
                    yield offset;
                }
                case HOT_BLOCKS -> {
                    // 80% of accesses hit 20% of blocks
                    int blockIdx = random.nextDouble() < 0.8
                        ? random.nextInt(uniqueBlocks / 5)  // Hot 20%
                        : uniqueBlocks / 5 + random.nextInt(uniqueBlocks * 4 / 5);  // Cold 80%
                    yield blockIdx * BLOCK_SIZE;
                }
            };
        }
    }

    /**
     * Run benchmark for a specific scenario
     */
    static double runBenchmark(BenchmarkScenario scenario) throws Exception {
        MockBlockCache cache = new MockBlockCache();
        Path path = Paths.get("/test/file");
        BlockSlotTinyCache tinyCache = new BlockSlotTinyCache(cache, path, scenario.uniqueBlocks * BLOCK_SIZE);

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            runIteration(tinyCache, scenario, OPS_PER_ITERATION);
        }

        // Measured runs
        List<Long> times = new ArrayList<>();
        for (int i = 0; i < MEASURED_ITERATIONS; i++) {
            long time = runIteration(tinyCache, scenario, OPS_PER_ITERATION);
            times.add(time);
        }

        cache.close();

        // Calculate median
        times.sort(Long::compareTo);
        long medianNs = times.get(times.size() / 2);
        return OPS_PER_ITERATION / (medianNs / 1_000_000_000.0); // ops/sec
    }

    /**
     * Run a single iteration
     */
    static long runIteration(BlockSlotTinyCache cache, BenchmarkScenario scenario, int operations) throws Exception {
        int opsPerThread = operations / scenario.threadCount;
        ExecutorService executor = Executors.newFixedThreadPool(scenario.threadCount);
        CountDownLatch latch = new CountDownLatch(scenario.threadCount);

        long startNs = System.nanoTime();

        for (int t = 0; t < scenario.threadCount; t++) {
            final long threadSeed = t;
            executor.submit(() -> {
                try {
                    OffsetGenerator generator = new OffsetGenerator(scenario.pattern, scenario.uniqueBlocks, threadSeed);
                    for (int i = 0; i < opsPerThread; i++) {
                        long offset = generator.nextOffset();
                        BlockCacheValue<RefCountedMemorySegment> val = cache.acquireRefCountedValue(offset);
                        val.unpin();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        long endNs = System.nanoTime();

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        return endNs - startNs;
    }

    /**
     * JUnit test entry point
     */
    @org.junit.Test
    public void testBenchmark() throws Exception {
        runBenchmarks();
    }

    /**
     * Main benchmark runner
     */
    public static void main(String[] args) throws Exception {
        runBenchmarks();
    }

    private static void runBenchmarks() throws Exception {
        System.out.println("BlockSlotTinyCache Benchmark");
        System.out.println("============================");
        System.out.println("Implementation: ThreadLocal");
        System.out.println();

        List<BenchmarkScenario> scenarios = new ArrayList<>();

        // Test different thread counts with sequential access
        for (int threads : THREAD_COUNTS) {
            scenarios.add(new BenchmarkScenario("Sequential/" + threads + "T", threads, AccessPattern.SEQUENTIAL, 1000));
        }

        // Test different access patterns with 8 threads
        scenarios.add(new BenchmarkScenario("Random/8T", 8, AccessPattern.RANDOM, 1000));
        scenarios.add(new BenchmarkScenario("Strided/8T", 8, AccessPattern.STRIDED, 1000));
        scenarios.add(new BenchmarkScenario("HotBlocks/8T", 8, AccessPattern.HOT_BLOCKS, 1000));

        System.out.printf("%-25s %15s %15s%n", "Scenario", "Ops/sec", "Ops/sec/thread");
        System.out.println("-".repeat(60));

        for (BenchmarkScenario scenario : scenarios) {
            double opsPerSec = runBenchmark(scenario);
            double opsPerSecPerThread = opsPerSec / scenario.threadCount;
            System.out.printf("%-25s %,15.0f %,15.0f%n", scenario.name, opsPerSec, opsPerSecPerThread);
        }

        System.out.println();
        System.out.println("Notes:");
        System.out.println("- Higher ops/sec is better");
        System.out.println("- Sequential: Best case for ThreadLocal (no contention)");
        System.out.println("- HotBlocks: Realistic workload (80/20 distribution)");
        System.out.println("- Ops/sec/thread shows scaling efficiency");
    }
}

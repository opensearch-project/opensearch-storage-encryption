/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;
import org.opensearch.index.store.bufferpoolfs.RadixBlockTable;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Microbenchmark comparing pure read throughput of three L1/L2 cache strategies:
 * <ul>
 *   <li><b>radixL1</b> — RadixBlockTable.get(): two plain array loads, no fences</li>
 *   <li><b>caffeineL2</b> — CaffeineBlockCache.get(): ConcurrentHashMap lookup</li>
 *   <li><b>stampGatedL1</b> — BlockSlotTinyCache-style stamp-gated acquire/release lookup</li>
 * </ul>
 *
 * Pre-populates all caches with {@code numBlocks} entries, then measures
 * pure lookup throughput under JMH-managed thread concurrency.
 *
 * <p>Thread count is controlled via JMH's {@code -t} flag (e.g., {@code -t 16})
 * or the {@code @Threads} annotation. JMH manages thread lifecycle directly,
 * eliminating ExecutorService scheduling noise and providing accurate
 * per-operation throughput measurement.
 *
 * <p>Run examples:
 * <pre>
 *   # Single-threaded (default)
 *   java ... -jar storage-encryption-jmh.jar RadixBlockTable -t 1
 *
 *   # 16 threads
 *   java ... -jar storage-encryption-jmh.jar RadixBlockTable -t 16
 *
 *   # Max threads (all available processors)
 *   java ... -jar storage-encryption-jmh.jar RadixBlockTable -t max
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1, jvmArgsAppend = { "--enable-native-access=ALL-UNNAMED", "--enable-preview" })
@Threads(Threads.MAX)
public class RadixBlockTableBenchmark {

    // ========================================================================
    // Shared state — one instance per benchmark, shared across all threads
    // ========================================================================

    @State(Scope.Benchmark)
    public static class SharedState {

        /** Number of cache blocks to populate. */
        @Param({ "128", "8192" })
        public int numBlocks;

        /**
         * Percentage of lookups that target populated keys (0-100).
         * 100 = all hits, 50 = 50% hits / 50% misses.
         */
        @Param({ "100" })
        public int hitPct;

        // ---- caches under test ----
        RadixBlockTable<ByteBuffer> radixTable;
        RadixBlockTable<ByteBuffer> sparseRadixTable;
        CaffeineBlockCache<ByteBuffer, Void> caffeineCache;
        StampGatedCache stampGatedCache;

        /**
         * Stride between sparse blockIds. With PAGE_SIZE=1024, a stride of 2048
         * means every put lands in a different outer directory slot, forcing
         * directory growth well beyond DEFAULT_OUTER_SLOTS (256).
         */
        private static final int SPARSE_STRIDE = 2048;

        // ---- shared data ----
        ByteBuffer[] byteBuffers;
        Path tempDir;
        Path dummyFile;

        // Pre-computed lookup keys for Caffeine
        FileBlockCacheKey[] hitKeys;
        FileBlockCacheKey[] missKeys;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            tempDir = Files.createTempDirectory("radix-bench");
            dummyFile = tempDir.resolve("dummy.dat");

            // Write file large enough for all blocks
            byte[] fileData = new byte[numBlocks * CACHE_BLOCK_SIZE];
            Files.write(dummyFile, fileData);

            // Allocate backing ByteBuffers
            byteBuffers = new ByteBuffer[numBlocks];
            for (int i = 0; i < numBlocks; i++) {
                byteBuffers[i] = ByteBuffer.allocateDirect(CACHE_BLOCK_SIZE);
            }

            // Pre-compute Caffeine lookup keys (FileBlockCacheKey uses Path + offset)
            hitKeys = new FileBlockCacheKey[numBlocks];
            missKeys = new FileBlockCacheKey[numBlocks];
            for (int i = 0; i < numBlocks; i++) {
                hitKeys[i] = new FileBlockCacheKey(dummyFile, (long) i * CACHE_BLOCK_SIZE);
                missKeys[i] = new FileBlockCacheKey(dummyFile, (long) (numBlocks + i) * CACHE_BLOCK_SIZE);
            }

            // ---- Populate RadixBlockTable ----
            radixTable = new RadixBlockTable<>(numBlocks / RadixBlockTable.PAGE_SIZE + 1);
            for (int i = 0; i < numBlocks; i++) {
                radixTable.put(i, byteBuffers[i]);
            }

            // ---- Populate CaffeineBlockCache (mainline L2 cache) ----
            Cache<BlockCacheKey, BlockCacheValue<ByteBuffer>> rawCache = Caffeine
                .newBuilder()
                .initialCapacity(numBlocks)
                .maximumSize(numBlocks * 2L)
                .recordStats()
                .build();
            caffeineCache = new CaffeineBlockCache<>(rawCache, null, numBlocks * 2L);
            for (int i = 0; i < numBlocks; i++) {
                caffeineCache.put(hitKeys[i], new StubBlockCacheValue(byteBuffers[i]));
            }

            // ---- Populate StampGatedCache (simplified BlockSlotTinyCache) ----
            stampGatedCache = new StampGatedCache();
            for (int i = 0; i < numBlocks; i++) {
                stampGatedCache.put(i, byteBuffers[i]);
            }

            // ---- Populate sparse RadixBlockTable (forces directory growth) ----
            sparseRadixTable = new RadixBlockTable<>();
            for (int i = 0; i < numBlocks; i++) {
                sparseRadixTable.put((long) i * SPARSE_STRIDE, byteBuffers[i]);
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            caffeineCache.clear();
            deleteRecursively(tempDir);
        }
    }

    // ========================================================================
    // Per-thread state — each JMH thread gets its own seed for hit/miss
    // ========================================================================

    @State(Scope.Thread)
    public static class ThreadState {
        int seed;

        @Setup(Level.Trial)
        public void setup() {
            seed = (int) Thread.currentThread().threadId();
        }
    }

    // ========================================================================
    // Benchmarks
    // ========================================================================

    /**
     * RadixBlockTable.get() — L1 cache.
     * Two plain array loads, no fences, no synchronization.
     */
    @Benchmark
    public void radixL1Get(SharedState s, ThreadState ts, Blackhole bh) {
        for (int i = 0; i < s.numBlocks; i++) {
            long blockId = nextBlockId(i, ts.seed, s.numBlocks, s.hitPct);
            bh.consume(s.radixTable.get(blockId));
        }
    }

    /**
     * CaffeineBlockCache.get() — L2 cache.
     * ConcurrentHashMap lookup with hashing.
     */
    @Benchmark
    public void caffeineL2Get(SharedState s, ThreadState ts, Blackhole bh) {
        for (int i = 0; i < s.numBlocks; i++) {
            FileBlockCacheKey key = isHit(i, ts.seed, s.hitPct) ? s.hitKeys[i % s.numBlocks] : s.missKeys[i % s.numBlocks];
            bh.consume(s.caffeineCache.get(key));
        }
    }

    /**
     * Simplified BlockSlotTinyCache-style stamp-gated acquire/release lookup.
     * Isolates the L1 cache mechanism: VarHandle getAcquire on stamp, then
     * plain loads of blockIdx and value arrays — mirrors the real
     * BlockSlotTinyCache read path without the full RefCounted/pin/unpin lifecycle.
     */
    @Benchmark
    public void stampGatedL1Get(SharedState s, ThreadState ts, Blackhole bh) {
        for (int i = 0; i < s.numBlocks; i++) {
            long blockId = nextBlockId(i, ts.seed, s.numBlocks, s.hitPct);
            bh.consume(s.stampGatedCache.get(blockId));
        }
    }

    /**
     * Measures memory overhead of RadixBlockTable: directory array + inner arrays.
     * Reports bytes per entry as a throughput metric.
     */
    @Benchmark
    public void radixMemoryOverhead(SharedState s, Blackhole bh) {
        bh.consume(s.radixTable.memoryOverheadBytes());
    }

    /**
     * Realistic read path: RadixBlockTable L1 with Caffeine L2 fallback.
     * On L1 miss, falls through to Caffeine L2 — this is the actual
     * production read path for the radix-based design.
     */
    @Benchmark
    public void radixL1WithCaffeineL2Fallback(SharedState s, ThreadState ts, Blackhole bh) {
        for (int i = 0; i < s.numBlocks; i++) {
            long blockId = nextBlockId(i, ts.seed, s.numBlocks, s.hitPct);
            ByteBuffer val = s.radixTable.get(blockId);
            if (val == null) {
                // L1 miss → fall through to L2
                int idx = (int) (blockId % s.numBlocks);
                FileBlockCacheKey key = isHit(i, ts.seed, s.hitPct) ? s.hitKeys[idx] : s.missKeys[idx];
                bh.consume(s.caffeineCache.get(key));
            } else {
                bh.consume(val);
            }
        }
    }

    /**
     * Realistic read path: StampGated L1 with Caffeine L2 fallback.
     * On L1 miss (due to 32-slot collision eviction), falls through to
     * Caffeine L2. With 128+ blocks and only 32 slots, most lookups
     * will miss L1 and hit L2, showing the true cost of collision pressure.
     */
    @Benchmark
    public void stampGatedL1WithCaffeineL2Fallback(SharedState s, ThreadState ts, Blackhole bh) {
        for (int i = 0; i < s.numBlocks; i++) {
            long blockId = nextBlockId(i, ts.seed, s.numBlocks, s.hitPct);
            ByteBuffer val = s.stampGatedCache.get(blockId);
            if (val == null) {
                // L1 miss → fall through to L2
                int idx = (int) (blockId % s.numBlocks);
                FileBlockCacheKey key = isHit(i, ts.seed, s.hitPct) ? s.hitKeys[idx] : s.missKeys[idx];
                bh.consume(s.caffeineCache.get(key));
            } else {
                bh.consume(val);
            }
        }
    }

    /**
     * RadixBlockTable lookup with sparse blockIds that force directory growth.
     * Populates blocks at blockId offsets of {@code SPARSE_STRIDE * i}, causing
     * the directory to grow well beyond DEFAULT_OUTER_SLOTS. Measures get()
     * throughput on a grown directory to verify no performance cliff.
     */
    @Benchmark
    public void radixL1GetSparseGrown(SharedState s, Blackhole bh) {
        for (int i = 0; i < s.numBlocks; i++) {
            long blockId = (long) i * SharedState.SPARSE_STRIDE;
            bh.consume(s.sparseRadixTable.get(blockId));
        }
    }

    // ========================================================================
    // Hit/miss helpers (static — no instance state needed)
    // ========================================================================

    private static boolean isHit(int i, int seed, int hitPct) {
        int hash = (i * 1103515245 + seed) & 0x7FFFFFFF;
        return (hash % 100) < hitPct;
    }

    private static long nextBlockId(int i, int seed, int numBlocks, int hitPct) {
        if (isHit(i, seed, hitPct)) {
            return i % numBlocks;
        } else {
            return (long) numBlocks + (i % numBlocks);
        }
    }

    // ========================================================================
    // Simplified stamp-gated cache (mirrors BlockSlotTinyCache read path)
    // ========================================================================

    /**
     * A simplified version of BlockSlotTinyCache that isolates the stamp-gated
     * acquire/release lookup pattern for benchmarking. Uses the same VarHandle
     * getAcquire/setRelease mechanism as the real BlockSlotTinyCache but without
     * the RefCounted/pin/unpin lifecycle, allowing a fair comparison of the raw
     * L1 lookup mechanism.
     *
     * <p>32 direct-mapped slots, hash-indexed. Each slot stores:
     * <ul>
     *   <li>slotBlockId[i] — which blockId is cached here</li>
     *   <li>slotValue[i] — the cached ByteBuffer</li>
     *   <li>slotStamp[i] — packed hash acting as a publication gate (acquire/release)</li>
     * </ul>
     */
    static final class StampGatedCache {
        private static final int SLOT_COUNT = 32;
        private static final int SLOT_MASK = SLOT_COUNT - 1;
        private static final VarHandle STAMP_ARR = MethodHandles.arrayElementVarHandle(long[].class);

        private final long[] slotBlockId = new long[SLOT_COUNT];
        private final ByteBuffer[] slotValue = new ByteBuffer[SLOT_COUNT];
        private final long[] slotStamp = new long[SLOT_COUNT];

        StampGatedCache() {
            for (int i = 0; i < SLOT_COUNT; i++) {
                slotBlockId[i] = -1;
            }
        }

        ByteBuffer get(long blockId) {
            int hash = hashBlockId(blockId);
            int slotIdx = (int) ((blockId ^ (blockId >>> 17)) & SLOT_MASK);

            long stamp = (long) STAMP_ARR.getAcquire(slotStamp, slotIdx);
            if (stamp != 0L) {
                int gotHash = (int) stamp;
                if (gotHash == hash) {
                    if (slotBlockId[slotIdx] == blockId) {
                        return slotValue[slotIdx];
                    }
                }
            }
            return null;
        }

        void put(long blockId, ByteBuffer buffer) {
            int hash = hashBlockId(blockId);
            int slotIdx = (int) ((blockId ^ (blockId >>> 17)) & SLOT_MASK);

            slotBlockId[slotIdx] = blockId;
            slotValue[slotIdx] = buffer;
            long stamp = hash & 0xFFFF_FFFFL;
            STAMP_ARR.setRelease(slotStamp, slotIdx, stamp);
        }

        private static int hashBlockId(long blockId) {
            return (int) (blockId ^ (blockId >>> 32));
        }
    }

    // ========================================================================
    // Stub BlockCacheValue for benchmark (no ref-counting needed)
    // ========================================================================

    /**
     * Minimal BlockCacheValue implementation for benchmarking.
     * No ref-counting or lifecycle management — just wraps a ByteBuffer.
     */
    static final class StubBlockCacheValue implements BlockCacheValue<ByteBuffer> {
        private final ByteBuffer buffer;

        StubBlockCacheValue(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public boolean tryPin() {
            return true;
        }

        @Override
        public void unpin() {}

        @Override
        public ByteBuffer value() {
            return buffer;
        }

        @Override
        public int length() {
            return buffer.capacity();
        }

        @Override
        public void close() {}

        @Override
        public void decRef() {}

        @Override
        public int getGeneration() {
            return 0;
        }
    }

    // ========================================================================
    // Utility
    // ========================================================================

    private static void deleteRecursively(Path path) throws Exception {
        if (path == null)
            return;
        if (Files.isDirectory(path)) {
            try (var entries = Files.list(path)) {
                for (Path entry : entries.toList()) {
                    deleteRecursively(entry);
                }
            }
        }
        Files.deleteIfExists(path);
    }
}

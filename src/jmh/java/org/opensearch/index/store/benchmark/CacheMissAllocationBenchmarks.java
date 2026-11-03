/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.nio.ByteBuffer;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.index.store.block.RefCountedByteBuffer;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.pool.ByteBufferPool;
import org.opensearch.index.store.pool.MemorySegmentPool;

/**
 * Benchmarks comparing cache-miss allocation paths:
 * <ul>
 *   <li>MemorySegmentPool: freelist acquire (lock + deque poll) or malloc via Panama</li>
 *   <li>ByteBufferPool: ByteBuffer.allocateDirect() + Cleaner registration</li>
 *   <li>Raw ByteBuffer.allocateDirect() (no pool tracking)</li>
 * </ul>
 *
 * <p>Each benchmark simulates a cache miss: acquire buffer, write data into it
 * (simulating MemorySegment.copy from Direct I/O read), then read back
 * (simulating CachedIndexInput reads). This is the full cost of a cache miss.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(value = 2, jvmArgsAppend = { "--enable-native-access=ALL-UNNAMED", "--enable-preview" })
@State(Scope.Benchmark)
public class CacheMissAllocationBenchmarks {

    @Param({ "8192" })
    public int blockSize;

    private MemorySegmentPool memorySegmentPool;
    private ByteBufferPool byteBufferPool;

    // Large pool so we don't exhaust during benchmark
    private static final long POOL_SIZE_BYTES = 1024L * 1024 * 1024; // 1GB

    @Setup(Level.Trial)
    public void setup() {
        long aligned = (POOL_SIZE_BYTES / blockSize) * blockSize;
        memorySegmentPool = new MemorySegmentPool(aligned, blockSize);
        byteBufferPool = new ByteBufferPool(aligned, blockSize);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (memorySegmentPool != null)
            memorySegmentPool.close();
        if (byteBufferPool != null)
            byteBufferPool.close();
    }

    // ======================== Cache miss: acquire + write + read ========================

    /**
     * MemorySegmentPool cache miss path:
     * pool.tryAcquire() → write data into MemorySegment → read back.
     * First iteration allocates via malloc, subsequent ones reuse from freelist.
     */
    @Benchmark
    public void memorySegmentPoolCacheMiss(Blackhole bh) throws Exception {
        RefCountedMemorySegment handle = memorySegmentPool.tryAcquire(5, TimeUnit.SECONDS);
        java.lang.foreign.MemorySegment seg = handle.segment();

        // Simulate writing decrypted data (like MemorySegment.copy from Direct I/O read)
        for (int i = 0; i < blockSize; i += 8) {
            seg.set(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, i, (long) i);
        }

        // Simulate reading (like CachedMemorySegmentIndexInput.readLong)
        long sum = 0;
        for (int i = 0; i < blockSize; i += 8) {
            sum += seg.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, i);
        }

        bh.consume(sum);
        // Release back to pool (simulates cache eviction returning to freelist)
        memorySegmentPool.release(handle);
    }

    /**
     * ByteBufferPool cache miss path:
     * pool.acquire() → ByteBuffer.allocateDirect() + Cleaner → write → read.
     * Every call does a fresh malloc (no freelist).
     */
    @Benchmark
    public void byteBufferPoolCacheMiss(Blackhole bh) throws Exception {
        RefCountedByteBuffer buf = byteBufferPool.acquire();
        ByteBuffer direct = buf.buffer();
        direct.order(java.nio.ByteOrder.LITTLE_ENDIAN);

        // Simulate writing decrypted data
        for (int i = 0; i < blockSize; i += 8) {
            direct.putLong(i, (long) i);
        }

        // Simulate reading (like CachedByteBufferIndexInput.readLong)
        long sum = 0;
        for (int i = 0; i < blockSize; i += 8) {
            sum += direct.getLong(i);
        }

        bh.consume(sum);
        bh.consume(buf);
        // Don't release — let GC handle it (simulates real usage where buffer goes to cache)
    }

    /**
     * Raw ByteBuffer.allocateDirect() with no pool tracking.
     * Baseline: pure malloc + write + read cost.
     */
    @Benchmark
    public void rawAllocateDirectCacheMiss(Blackhole bh) {
        ByteBuffer direct = ByteBuffer.allocateDirect(blockSize).order(java.nio.ByteOrder.LITTLE_ENDIAN);

        for (int i = 0; i < blockSize; i += 8) {
            direct.putLong(i, (long) i);
        }

        long sum = 0;
        for (int i = 0; i < blockSize; i += 8) {
            sum += direct.getLong(i);
        }

        bh.consume(sum);
        bh.consume(direct);
    }

    // ======================== Allocation only (no data) ========================

    /**
     * MemorySegmentPool acquire + release only (no data write/read).
     * Measures pure pool overhead: lock + freelist + refcount.
     */
    @Benchmark
    public void memorySegmentPoolAcquireRelease(Blackhole bh) throws Exception {
        RefCountedMemorySegment handle = memorySegmentPool.tryAcquire(5, TimeUnit.SECONDS);
        bh.consume(handle);
        memorySegmentPool.release(handle);
    }

    /**
     * ByteBufferPool acquire only (no release — GC managed).
     * Measures: allocateDirect() + Cleaner registration.
     */
    @Benchmark
    public void byteBufferPoolAcquireOnly(Blackhole bh) throws Exception {
        RefCountedByteBuffer buf = byteBufferPool.acquire();
        bh.consume(buf);
    }

    /**
     * Raw ByteBuffer.allocateDirect() only.
     * Baseline: pure malloc cost.
     */
    @Benchmark
    public void rawAllocateDirectOnly(Blackhole bh) {
        ByteBuffer direct = ByteBuffer.allocateDirect(blockSize);
        bh.consume(direct);
    }
}

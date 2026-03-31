/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.store.IndexInput;
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
import org.opensearch.common.util.concurrent.IndexInputScope;
import org.opensearch.index.store.bufferpoolfs.StaticConfigs;

/**
 * Simulates sequential posting list reads on encrypted (bufferpool) vs mmap files.
 *
 * Posting lists are read sequentially — VInt-encoded doc IDs, term frequencies,
 * and positions. Each posting list is a contiguous run of small reads (1-5 bytes
 * per VInt) within a few blocks. The reader seeks to the posting list start, then
 * reads sequentially through it.
 *
 * This is the best case for the fast path — consecutive reads within the same
 * block hit currentBlockOffset match and avoid L1/L2 lookup entirely. Block
 * transitions only happen every 8KB.
 *
 * Each invocation simulates multiple posting list reads at random starting
 * positions to exercise both the fast path (within block) and slow path
 * (block transitions during long posting lists).
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(value = 1, jvmArgsAppend = { "--enable-native-access=ALL-UNNAMED", "--enable-preview" })
public class SequentialPostingListBenchmark {

    private static final int POSTING_LISTS_PER_OP = 8;
    private static final int BLOCK_SIZE = StaticConfigs.CACHE_BLOCK_SIZE;

    @State(Scope.Benchmark)
    public static class SharedState extends ReadBenchmarkBase {

        @Param({ "bufferpool", "mmap" })
        public String directoryType;

        @Param({ "32" })
        public int fileSizeMB;

        @Param({ "1" })
        public int numFilesToRead;

        /** Bytes per posting list — controls how many block transitions occur */
        @Param({ "128", "1024", "8192", "32768" })
        public int postingListBytes;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            super.setupTrial();
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            super.closeInputs();
            super.tearDownTrial();
        }
    }

    @State(Scope.Thread)
    public static class ThreadState {
        IndexInput input;
        Random rng;
        long maxStartOffset;

        @Setup(Level.Iteration)
        public void setup(SharedState shared) {
            input = shared.indexInputs[0].clone();
            rng = new Random(Thread.currentThread().threadId() * 13 + 5);
            maxStartOffset = input.length() - shared.postingListBytes - 1;
        }

        @TearDown(Level.Iteration)
        public void tearDown() throws IOException {
            if (input != null) input.close();
        }
    }

    @Benchmark
    @Threads(1)
    public void sequentialPosting_1thread(SharedState shared, ThreadState ts, Blackhole bh) throws IOException {
        doReads(shared, ts, bh);
    }

    @Benchmark
    @Threads(4)
    public void sequentialPosting_4threads(SharedState shared, ThreadState ts, Blackhole bh) throws IOException {
        doReads(shared, ts, bh);
    }

    @Benchmark
    @Threads(12)
    public void sequentialPosting_12threads(SharedState shared, ThreadState ts, Blackhole bh) throws IOException {
        doReads(shared, ts, bh);
    }

    private void doReads(SharedState shared, ThreadState ts, Blackhole bh) throws IOException {
        final IndexInputScope scope = new IndexInputScope();
        ScopedValue.where(IndexInputScope.SCOPE, scope).run(() -> {
            try {
                doReadsInner(shared, ts, bh);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                scope.closeAll();
            }
        });
    }

    private void doReadsInner(SharedState shared, ThreadState ts, Blackhole bh) throws IOException {
        IndexInput in = ts.input;
        Random rng = ts.rng;
        int postingBytes = shared.postingListBytes;
        long sum = 0;

        for (int p = 0; p < POSTING_LISTS_PER_OP; p++) {
            // Seek to random posting list start
            long startPos = rng.nextLong(ts.maxStartOffset);
            in.seek(startPos);

            // Read sequentially — simulating VInt decoding of doc IDs
            // Mix of readByte (VInt first byte) and readInt (skip data)
            int remaining = postingBytes;
            while (remaining > 0) {
                if (remaining >= 4) {
                    sum += in.readInt();
                    remaining -= 4;
                } else {
                    sum += in.readByte();
                    remaining -= 1;
                }
            }
        }

        bh.consume(sum);
    }
}

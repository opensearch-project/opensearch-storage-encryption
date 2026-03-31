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

/**
 * Simulates random doc values lookups on encrypted (bufferpool) vs mmap files.
 *
 * Doc values are stored as fixed-width arrays — each document's value is at a
 * known offset (docId * bytesPerValue). Random doc values lookups (e.g., for
 * sorting or aggregations like cardinality-agg-high) seek to scattered positions
 * across the file, reading 8 bytes (one long) per doc.
 *
 * This is the worst case for pin/unpin overhead: every read is 8 bytes but each
 * hits a different 8KB block, so the CAS cost is never amortized by sequential
 * reads within the same block.
 *
 * Uses ReadBenchmarkBase which writes raw encrypted files via BufferPoolDirectory
 * and identical plaintext files via MMapDirectory.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(value = 1, jvmArgsAppend = { "--enable-native-access=ALL-UNNAMED", "--enable-preview" })
public class RandomDocValuesLookupBenchmark {

    private static final int LOOKUPS_PER_OP = 64; // doc value reads per invocation
    private static final int BYTES_PER_VALUE = 8;  // one long per doc value

    @State(Scope.Benchmark)
    public static class SharedState extends ReadBenchmarkBase {

        @Param({ "bufferpool", "mmap" })
        public String directoryType;

        @Param({ "32" })
        public int fileSizeMB;

        @Param({ "1" })
        public int numFilesToRead;

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
        long maxOffset;

        @Setup(Level.Iteration)
        public void setup(SharedState shared) {
            input = shared.indexInputs[0].clone();
            rng = new Random(Thread.currentThread().threadId() * 17 + 3);
            maxOffset = input.length() - BYTES_PER_VALUE;
        }

        @TearDown(Level.Iteration)
        public void tearDown() throws IOException {
            if (input != null) input.close();
        }
    }

    @Benchmark
    @Threads(1)
    public void randomDocValues_1thread(ThreadState ts, Blackhole bh) throws IOException {
        doLookups(ts, bh);
    }

    @Benchmark
    @Threads(4)
    public void randomDocValues_4threads(ThreadState ts, Blackhole bh) throws IOException {
        doLookups(ts, bh);
    }

    @Benchmark
    @Threads(12)
    public void randomDocValues_12threads(ThreadState ts, Blackhole bh) throws IOException {
        doLookups(ts, bh);
    }

    private void doLookups(ThreadState ts, Blackhole bh) throws IOException {
        final IndexInputScope scope = new IndexInputScope();
        ScopedValue.where(IndexInputScope.SCOPE, scope).run(() -> {
            try {
                doLookupsInner(ts, bh);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                scope.closeAll();
            }
        });
    }

    private void doLookupsInner(ThreadState ts, Blackhole bh) throws IOException {
        IndexInput in = ts.input;
        Random rng = ts.rng;
        long maxOffset = ts.maxOffset;
        long sum = 0;

        for (int i = 0; i < LOOKUPS_PER_OP; i++) {
            // Random doc ID → random offset in the doc values file
            // Each lookup hits a different block (8 bytes out of 8KB block)
            long offset = rng.nextLong(maxOffset);
            in.seek(offset);
            sum += in.readLong();
        }

        bh.consume(sum);
    }
}

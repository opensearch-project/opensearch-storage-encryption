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
 * Simulates random concurrent term lookups on encrypted (bufferpool) vs mmap files.
 *
 * Each "term lookup" is modeled as:
 * 1. Seek to a random position (simulating FST traversal to find term in dictionary)
 * 2. Read a small number of bytes (simulating reading the term entry)
 * 3. Seek >1MB away (simulating jump to posting list)
 * 4. Read posting data (simulating reading doc IDs from posting list)
 *
 * This exercises the pin/unpin hot path on every block transition, which is the
 * core overhead measured by this benchmark. With high cardinality (unique seeks),
 * every lookup hits a different block — no fast-path reuse.
 *
 * Uses ReadBenchmarkBase which writes raw encrypted files via BufferPoolDirectory
 * and identical plaintext files via MMapDirectory.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(value = 1, jvmArgsAppend = { "--enable-native-access=ALL-UNNAMED", "--enable-preview" })
public class RandomTermLookupBenchmark {

    private static final int MIN_SEEK_DISTANCE = 1024 * 1024; // 1MB minimum between seeks
    private static final int TERM_ENTRY_BYTES = 32;  // bytes read at FST position
    private static final int POSTING_BYTES = 64;     // bytes read at posting list position
    private static final int LOOKUPS_PER_OP = 16;    // term lookups per benchmark invocation

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

        @Setup(Level.Iteration)
        public void setup(SharedState shared) {
            input = shared.indexInputs[0].clone();
            rng = new Random(Thread.currentThread().threadId() * 31 + 7);
        }

        @TearDown(Level.Iteration)
        public void tearDown() throws IOException {
            if (input != null) input.close();
        }
    }

    @Benchmark
    @Threads(1)
    public void randomTermLookup_1thread(ThreadState ts, Blackhole bh) throws IOException {
        doLookups(ts, bh);
    }

    @Benchmark
    @Threads(4)
    public void randomTermLookup_4threads(ThreadState ts, Blackhole bh) throws IOException {
        doLookups(ts, bh);
    }

    @Benchmark
    @Threads(12)
    public void randomTermLookup_12threads(ThreadState ts, Blackhole bh) throws IOException {
        doLookups(ts, bh);
    }

    private void doLookups(ThreadState ts, Blackhole bh) throws IOException {
        // Wrap in ScopedValue so slices register with scope and get
        // deterministic unpin at task end — same as OpenSearchThreadPoolExecutor.
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
        long fileLen = in.length();
        long sum = 0;

        for (int i = 0; i < LOOKUPS_PER_OP; i++) {
            long fstPos = rng.nextLong(fileLen - MIN_SEEK_DISTANCE - POSTING_BYTES);
            long postingPos = fstPos + MIN_SEEK_DISTANCE
                + rng.nextLong(fileLen - fstPos - MIN_SEEK_DISTANCE - POSTING_BYTES);

            // Step 1: Seek to FST position and read term entry
            in.seek(fstPos);
            for (int b = 0; b < TERM_ENTRY_BYTES; b++) {
                sum += in.readByte();
            }

            // Step 2: Seek >1MB away to posting list and read posting data
            in.seek(postingPos);
            for (int b = 0; b < POSTING_BYTES / Long.BYTES; b++) {
                sum += in.readLong();
            }
        }

        bh.consume(sum);
    }
}

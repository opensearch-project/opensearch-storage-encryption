/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/** Scans and point lookups on a file whose blocks are not cached; each invocation starts cold. */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1, jvmArgsAppend = { "--enable-native-access=ALL-UNNAMED" })
@Warmup(iterations = 5)
@Measurement(iterations = 20)
public class ColdScanBenchmarks {

    @State(Scope.Benchmark)
    public static class S extends ReadBenchmarkBase {
        IndexInput in;
        final byte[] buf = new byte[16 * 1024];

        @Setup(Level.Trial)
        public void setup() throws Exception {
            setupTrial();
            closeInputs();
        }

        @Setup(Level.Invocation)
        public void cold() throws Exception {
            Thread.sleep(50); // let read-ahead from the previous invocation finish
            poolResources.getBlockCache().clear();
            in = bufferPoolDirectory.openInput(fileNames[0], IOContext.DEFAULT);
        }

        @TearDown(Level.Invocation)
        public void close() throws IOException {
            in.close();
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            tearDownTrial();
        }

        long scan(IndexInput input, long from, long to, Blackhole bh) throws IOException {
            input.seek(from);
            long n = 0;
            while (input.getFilePointer() < to) {
                int len = (int) Math.min(buf.length, to - input.getFilePointer());
                input.readBytes(buf, 0, len);
                bh.consume(buf[len - 1]);
                n += len;
            }
            return n;
        }
    }

    @Benchmark
    public long scanFromStart(S s, Blackhole bh) throws IOException {
        return s.scan(s.in, 0, s.in.length(), bh);
    }

    @Benchmark
    public long scanSecondHalf(S s, Blackhole bh) throws IOException {
        return s.scan(s.in, s.in.length() / 2, s.in.length(), bh);
    }

    @Benchmark
    public long footerThenScan(S s, Blackhole bh) throws IOException {
        s.in.seek(s.in.length() - 16);
        bh.consume(s.in.readLong());
        return s.scan(s.in, 0, s.in.length(), bh);
    }

    @Benchmark
    public long twoSlicesInterleaved(S s, Blackhole bh) throws IOException {
        long half = s.in.length() / 2;
        IndexInput a = s.in.slice("a", 0, half);
        IndexInput b = s.in.slice("b", half, s.in.length() - half);
        long n = 0;
        while (a.getFilePointer() < a.length() || b.getFilePointer() < b.length()) {
            for (IndexInput x : new IndexInput[] { a, b }) {
                long left = x.length() - x.getFilePointer();
                if (left > 0) {
                    int len = (int) Math.min(s.buf.length, left);
                    x.readBytes(s.buf, 0, len);
                    bh.consume(s.buf[len - 1]);
                    n += len;
                }
            }
        }
        return n;
    }

    /** 2000 point lookups, each through a new clone, reading 16 bytes that sometimes cross into the next block. */
    @Benchmark
    public long randomLookupsThroughClones(S s, Blackhole bh) throws IOException {
        Random random = new Random(42);
        long n = 0;
        for (int i = 0; i < 2_000; i++) {
            IndexInput clone = s.in.clone();
            clone.seek(random.nextLong(s.in.length() - 16));
            clone.readBytes(s.buf, 0, 16);
            bh.consume(s.buf[15]);
            n += 16;
        }
        return n;
    }
}

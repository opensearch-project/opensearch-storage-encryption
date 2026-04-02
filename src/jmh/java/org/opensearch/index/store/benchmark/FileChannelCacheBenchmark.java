/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;
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
import org.opensearch.index.store.block_loader.DirectIOReaderUtil;
import org.opensearch.index.store.block_loader.FileChannelCache;
import org.opensearch.index.store.block_loader.RefCountedChannel;
import org.opensearch.index.store.bufferpoolfs.StaticConfigs;

/**
 * JMH benchmark comparing I/O latency for small reads (8 KB) with and without
 * the FileChannelCache, using O_DIRECT to bypass the OS page cache.
 *
 * <p>Three strategies are compared:
 * <ul>
 *   <li><b>openEveryTime</b>: open(O_DIRECT) + read() + close() per I/O — the baseline
 *       without FD caching.</li>
 *   <li><b>cachedChannel</b>: acquire() from FileChannelCache + read() + release()
 *       — the FD cache approach. open() is amortized across many reads.</li>
 *   <li><b>singleChannel</b>: one pre-opened O_DIRECT FileChannel, read() only —
 *       theoretical lower bound (no open/close, no cache lookup).</li>
 * </ul>
 *
 * <p>All reads use sector-aligned buffers allocated via {@link Arena#allocate(long, long)}
 * to satisfy O_DIRECT alignment requirements.
 *
 * <p>Run:
 * <pre>
 *   ./gradlew jmhJar
 *   java --enable-native-access=ALL-UNNAMED --enable-preview \
 *        -jar build/libs/*-jmh.jar FileChannelCacheBenchmark \
 *        -t 1 -f 2 -wi 3 -i 5
 *
 *   # Multi-threaded (8 threads, simulating concurrent shard reads):
 *   java --enable-native-access=ALL-UNNAMED --enable-preview \
 *        -jar build/libs/*-jmh.jar FileChannelCacheBenchmark \
 *        -t 8 -f 2 -wi 3 -i 5
 * </pre>
 */
@BenchmarkMode({ Mode.AverageTime, Mode.Throughput })
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 2)
@Fork(value = 1, jvmArgsAppend = { "--enable-native-access=ALL-UNNAMED", "--enable-preview" })
@Threads(1)
@SuppressWarnings("preview")
public class FileChannelCacheBenchmark {

    // ========================================================================
    // Shared state — one per benchmark trial. Creates test files on disk.
    // ========================================================================

    @State(Scope.Benchmark)
    public static class SharedState {

        @Param({ "8192" })
        public int readSizeBytes;

        @Param({ "1", "64" })
        public int numFiles;

        private Path tempDir;
        private String[] filePaths;
        private FileChannelCache fdCache;
        private OpenOption directOption;
        private int alignment;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            tempDir = Files.createTempDirectory("fcc-bench");
            filePaths = new String[numFiles];

            // Create test files — 1 MB each, filled with deterministic pattern
            byte[] data = BenchmarkConfig.buildDeterministicPattern(1024 * 1024);
            for (int i = 0; i < numFiles; i++) {
                Path f = tempDir.resolve("seg_" + i + ".dat");
                Files.write(f, data);
                filePaths[i] = f.toAbsolutePath().normalize().toString();
            }

            // O_DIRECT setup
            directOption = DirectIOReaderUtil.getDirectOpenOption();
            alignment = StaticConfigs.getDirectIOAlignment(tempDir);

            // FD cache sized to hold all files (no eviction during benchmark)
            fdCache = new FileChannelCache(Math.max(numFiles, 256), directOption);
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            if (fdCache != null) {
                fdCache.close();
            }
            BenchmarkConfig.deleteRecursively(tempDir);
        }
    }

    // ========================================================================
    // Per-thread state — each JMH thread gets its own aligned read buffer and RNG
    // ========================================================================

    @State(Scope.Thread)
    public static class ThreadState {
        Arena arena;
        ByteBuffer readBuf;
        Random rng;
        int fileIndex;

        @Setup(Level.Iteration)
        public void setup(SharedState shared) {
            arena = Arena.ofConfined();
            // Allocate sector-aligned buffer for O_DIRECT reads
            MemorySegment segment = arena.allocate(shared.readSizeBytes, shared.alignment);
            readBuf = segment.asByteBuffer();
            rng = new Random(Thread.currentThread().threadId());
            fileIndex = 0;
        }

        @TearDown(Level.Iteration)
        public void tearDown() {
            if (arena != null) {
                arena.close();
            }
        }

        /** Round-robin across files to simulate multi-shard access */
        String nextPath(SharedState shared) {
            String path = shared.filePaths[fileIndex];
            fileIndex = (fileIndex + 1) % shared.numFiles;
            return path;
        }

        /** Random offset within the file, aligned to read size */
        long randomOffset(SharedState shared) {
            int maxBlocks = (1024 * 1024) / shared.readSizeBytes;
            return (long) rng.nextInt(maxBlocks) * shared.readSizeBytes;
        }
    }

    // ========================================================================
    // Benchmark: open(O_DIRECT) + read() + close() per I/O — no FD caching
    // ========================================================================

    @Benchmark
    public void openEveryTime(SharedState shared, ThreadState ts, Blackhole bh) throws IOException {
        String path = ts.nextPath(shared);
        long offset = ts.randomOffset(shared);
        ts.readBuf.clear();

        try (FileChannel fc = FileChannel.open(Path.of(path), StandardOpenOption.READ, shared.directOption)) {
            int n = fc.read(ts.readBuf, offset);
            bh.consume(n);
        }
    }

    // ========================================================================
    // Benchmark: FileChannelCache acquire() + read() + release()
    // ========================================================================

    @Benchmark
    public void cachedChannel(SharedState shared, ThreadState ts, Blackhole bh) throws IOException {
        String path = ts.nextPath(shared);
        long offset = ts.randomOffset(shared);
        ts.readBuf.clear();

        try (RefCountedChannel ref = shared.fdCache.acquire(path)) {
            int n = ref.channel().read(ts.readBuf, offset);
            bh.consume(n);
        }
    }

    // ========================================================================
    // Benchmark: pre-opened O_DIRECT channel, read() only — theoretical lower bound
    // ========================================================================

    @State(Scope.Thread)
    public static class PreOpenedState {
        FileChannel[] channels;

        @Setup(Level.Trial)
        public void setup(SharedState shared) throws IOException {
            channels = new FileChannel[shared.numFiles];
            for (int i = 0; i < shared.numFiles; i++) {
                channels[i] = FileChannel.open(Path.of(shared.filePaths[i]), StandardOpenOption.READ, shared.directOption);
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() throws IOException {
            if (channels != null) {
                for (FileChannel fc : channels) {
                    if (fc != null)
                        fc.close();
                }
            }
        }
    }

    @Benchmark
    public void singleChannel(SharedState shared, ThreadState ts, PreOpenedState pre, Blackhole bh) throws IOException {
        long offset = ts.randomOffset(shared);
        ts.readBuf.clear();

        FileChannel fc = pre.channels[ts.fileIndex == 0 ? shared.numFiles - 1 : ts.fileIndex - 1];
        int n = fc.read(ts.readBuf, offset);
        bh.consume(n);
    }
}

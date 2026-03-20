/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.index.store.bufferpoolfs.StaticConfigs;

/**
 * Hot path benchmark: all data is already resident in block cache (BufferPool)
 * or page cache (MMap). Measures pure read throughput without I/O latency.
 *
 * <p>Thread count is controlled via JMH's {@code -t} flag (e.g., {@code -t 8}).
 * JMH manages thread lifecycle directly, eliminating ExecutorService scheduling
 * noise. Each JMH thread gets its own {@link ThreadState} with per-thread
 * IndexInput clones (Lucene IndexInputs are not thread-safe).
 *
 * <p>Run examples:
 * <pre>
 *   # Default threads (all available processors via @Threads(MAX))
 *   java ... -jar storage-encryption-jmh.jar HotPath
 *
 *   # Single-threaded
 *   java ... -jar storage-encryption-jmh.jar HotPath -t 1
 *
 *   # 16 threads
 *   java ... -jar storage-encryption-jmh.jar HotPath -t 16
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 10, time = 2)
@Fork(value = 2, jvmArgsAppend = { "--enable-native-access=ALL-UNNAMED", "--enable-preview" })
@Threads(Threads.MAX)
public class HotPathReadBenchmarks {

    // ========================================================================
    // Shared state — one instance per benchmark, shared across all threads.
    // Holds directories, file data, offset arrays, and the master IndexInputs.
    // ========================================================================

    @State(Scope.Benchmark)
    public static class SharedState extends ReadBenchmarkBase {

        @Setup(Level.Trial)
        public void setup() throws Exception {
            super.setupTrial();
            final int blockSize = StaticConfigs.CACHE_BLOCK_SIZE;
            byte[] buf = new byte[blockSize];
            // BufferPool: read all files to populate block cache
            if ("bufferpool".equals(directoryType)) {
                for (String fileName : fileNames) {
                    try (IndexInput in = bufferPoolDirectory.openInput(fileName, org.apache.lucene.store.IOContext.DEFAULT)) {
                        long remaining = in.length();
                        while (remaining > 0) {
                            int toRead = (int) Math.min(buf.length, remaining);
                            in.readBytes(buf, 0, toRead);
                            remaining -= toRead;
                        }
                    }
                }
            }
            // MMap: read all files to populate page cache
            if ("mmap".equals(directoryType) || "multisegment_mmap_impl".equals(directoryType)) {
                for (String fileName : fileNames) {
                    try (IndexInput in = mmapDirectory.openInput(fileName, org.apache.lucene.store.IOContext.DEFAULT)) {
                        long remaining = in.length();
                        while (remaining > 0) {
                            int toRead = (int) Math.min(buf.length, remaining);
                            in.readBytes(buf, 0, toRead);
                            remaining -= toRead;
                        }
                    }
                }
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            super.closeInputs();
            super.tearDownTrial();
        }
    }

    // ========================================================================
    // Per-thread state — each JMH thread gets its own IndexInput clones.
    // Lucene IndexInputs are NOT thread-safe, so each thread must clone.
    // ========================================================================

    @State(Scope.Thread)
    public static class ThreadState {
        IndexInput[] threadInputs;
        int numFilesToRead;
        int fileSize;
        long[] blockStartOffsets;
        long[] randomReadByteOffsets;
        int sequentialReadNumBytes;

        @Setup(Level.Iteration)
        public void setup(SharedState shared) {
            numFilesToRead = shared.numFilesToRead;
            fileSize = shared.fileSize;
            blockStartOffsets = shared.blockStartOffsets;
            randomReadByteOffsets = shared.randomReadByteOffsets;
            sequentialReadNumBytes = shared.sequentialReadNumBytes;
            threadInputs = new IndexInput[numFilesToRead];
            for (int i = 0; i < numFilesToRead; i++) {
                threadInputs[i] = shared.indexInputs[i].clone();
            }
        }

        @TearDown(Level.Iteration)
        public void tearDown() throws IOException {
            if (threadInputs != null) {
                for (IndexInput in : threadInputs) {
                    if (in != null)
                        in.close();
                }
                threadInputs = null;
            }
        }
    }

    // ========================================================================
    // Benchmarks — Random reads via clone (crossing block boundaries)
    // ========================================================================

    @Benchmark
    public void randomReadByteFromClone(ThreadState ts, Blackhole bh) throws IOException {
        for (int fileIdx = 0; fileIdx < ts.numFilesToRead; fileIdx++) {
            IndexInput fileInput = ts.threadInputs[fileIdx];
            RandomAccessInput in = (RandomAccessInput) fileInput;
            for (long offset : ts.randomReadByteOffsets) {
                bh.consume(in.readByte(offset));
            }
        }
    }

    @Benchmark
    public void sequentialReadBytesFromClone(ThreadState ts, Blackhole bh) throws IOException {
        for (int fileIdx = 0; fileIdx < ts.numFilesToRead; fileIdx++) {
            IndexInput fileInput = ts.threadInputs[fileIdx];
            for (long offset : ts.blockStartOffsets) {
                int dummyConsumer = 0;
                fileInput.seek(offset);
                long remaining = Math.min(ts.sequentialReadNumBytes, ts.fileSize - offset);
                for (long i = 0; i < remaining; i++) {
                    dummyConsumer += fileInput.readByte();
                }
                bh.consume(dummyConsumer);
            }
        }
    }

    // ======================== RandomAccessInput API benchmarks ========================

    @Benchmark
    public void randomReadShortFromClone(ThreadState ts, Blackhole bh) throws IOException {
        for (int fileIdx = 0; fileIdx < ts.numFilesToRead; fileIdx++) {
            IndexInput fileInput = ts.threadInputs[fileIdx];
            RandomAccessInput in = (RandomAccessInput) fileInput;
            for (long offset : ts.randomReadByteOffsets) {
                if (offset + Short.BYTES <= ts.fileSize) {
                    bh.consume(in.readShort(offset));
                }
            }
        }
    }

    @Benchmark
    public void randomReadIntFromClone(ThreadState ts, Blackhole bh) throws IOException {
        for (int fileIdx = 0; fileIdx < ts.numFilesToRead; fileIdx++) {
            IndexInput fileInput = ts.threadInputs[fileIdx];
            RandomAccessInput in = (RandomAccessInput) fileInput;
            for (long offset : ts.randomReadByteOffsets) {
                if (offset + Integer.BYTES <= ts.fileSize) {
                    bh.consume(in.readInt(offset));
                }
            }
        }
    }

    @Benchmark
    public void randomReadLongFromClone(ThreadState ts, Blackhole bh) throws IOException {
        for (int fileIdx = 0; fileIdx < ts.numFilesToRead; fileIdx++) {
            IndexInput fileInput = ts.threadInputs[fileIdx];
            RandomAccessInput in = (RandomAccessInput) fileInput;
            for (long offset : ts.randomReadByteOffsets) {
                if (offset + Long.BYTES <= ts.fileSize) {
                    bh.consume(in.readLong(offset));
                }
            }
        }
    }

    @Benchmark
    public void randomReadBulkBytesFromClone(ThreadState ts, Blackhole bh) throws IOException {
        byte[] readBuf = new byte[StaticConfigs.CACHE_BLOCK_SIZE];
        for (int fileIdx = 0; fileIdx < ts.numFilesToRead; fileIdx++) {
            IndexInput fileInput = ts.threadInputs[fileIdx];
            RandomAccessInput in = (RandomAccessInput) fileInput;
            for (long offset : ts.randomReadByteOffsets) {
                int readable = (int) Math.min(readBuf.length, ts.fileSize - offset);
                if (readable > 0) {
                    in.readBytes(offset, readBuf, 0, readable);
                    bh.consume(readBuf[0]);
                }
            }
        }
    }

    // ======================== IndexInput sequential API benchmarks ========================

    @Benchmark
    public void sequentialReadBulkBytesFromClone(ThreadState ts, Blackhole bh) throws IOException {
        byte[] readBuf = new byte[StaticConfigs.CACHE_BLOCK_SIZE];
        for (int fileIdx = 0; fileIdx < ts.numFilesToRead; fileIdx++) {
            IndexInput fileInput = ts.threadInputs[fileIdx];
            for (long offset : ts.blockStartOffsets) {
                fileInput.seek(offset);
                int readable = (int) Math.min(readBuf.length, ts.fileSize - offset);
                if (readable > 0) {
                    fileInput.readBytes(readBuf, 0, readable);
                    bh.consume(readBuf[0]);
                }
            }
        }
    }

    @Benchmark
    public void sequentialReadShortFromClone(ThreadState ts, Blackhole bh) throws IOException {
        for (int fileIdx = 0; fileIdx < ts.numFilesToRead; fileIdx++) {
            IndexInput fileInput = ts.threadInputs[fileIdx];
            for (long offset : ts.blockStartOffsets) {
                if (offset + Short.BYTES <= ts.fileSize) {
                    fileInput.seek(offset);
                    bh.consume(fileInput.readShort());
                }
            }
        }
    }

    @Benchmark
    public void sequentialReadIntFromClone(ThreadState ts, Blackhole bh) throws IOException {
        for (int fileIdx = 0; fileIdx < ts.numFilesToRead; fileIdx++) {
            IndexInput fileInput = ts.threadInputs[fileIdx];
            for (long offset : ts.blockStartOffsets) {
                if (offset + Integer.BYTES <= ts.fileSize) {
                    fileInput.seek(offset);
                    bh.consume(fileInput.readInt());
                }
            }
        }
    }

    @Benchmark
    public void sequentialReadLongFromClone(ThreadState ts, Blackhole bh) throws IOException {
        for (int fileIdx = 0; fileIdx < ts.numFilesToRead; fileIdx++) {
            IndexInput fileInput = ts.threadInputs[fileIdx];
            for (long offset : ts.blockStartOffsets) {
                if (offset + Long.BYTES <= ts.fileSize) {
                    fileInput.seek(offset);
                    bh.consume(fileInput.readLong());
                }
            }
        }
    }

    @Benchmark
    public void sequentialReadIntsFromClone(ThreadState ts, Blackhole bh) throws IOException {
        int numInts = StaticConfigs.CACHE_BLOCK_SIZE / Integer.BYTES;
        int[] intBuf = new int[numInts];
        for (int fileIdx = 0; fileIdx < ts.numFilesToRead; fileIdx++) {
            IndexInput fileInput = ts.threadInputs[fileIdx];
            for (long offset : ts.blockStartOffsets) {
                long remaining = ts.fileSize - offset;
                int readable = (int) Math.min(numInts, remaining / Integer.BYTES);
                if (readable > 0) {
                    fileInput.seek(offset);
                    fileInput.readInts(intBuf, 0, readable);
                    bh.consume(intBuf[0]);
                }
            }
        }
    }

    @Benchmark
    public void sequentialReadLongsFromClone(ThreadState ts, Blackhole bh) throws IOException {
        int numLongs = StaticConfigs.CACHE_BLOCK_SIZE / Long.BYTES;
        long[] longBuf = new long[numLongs];
        for (int fileIdx = 0; fileIdx < ts.numFilesToRead; fileIdx++) {
            IndexInput fileInput = ts.threadInputs[fileIdx];
            for (long offset : ts.blockStartOffsets) {
                long remaining = ts.fileSize - offset;
                int readable = (int) Math.min(numLongs, remaining / Long.BYTES);
                if (readable > 0) {
                    fileInput.seek(offset);
                    fileInput.readLongs(longBuf, 0, readable);
                    bh.consume(longBuf[0]);
                }
            }
        }
    }

    @Benchmark
    public void sequentialReadFloatsFromClone(ThreadState ts, Blackhole bh) throws IOException {
        int numFloats = StaticConfigs.CACHE_BLOCK_SIZE / Float.BYTES;
        float[] floatBuf = new float[numFloats];
        for (int fileIdx = 0; fileIdx < ts.numFilesToRead; fileIdx++) {
            IndexInput fileInput = ts.threadInputs[fileIdx];
            for (long offset : ts.blockStartOffsets) {
                long remaining = ts.fileSize - offset;
                int readable = (int) Math.min(numFloats, remaining / Float.BYTES);
                if (readable > 0) {
                    fileInput.seek(offset);
                    fileInput.readFloats(floatBuf, 0, readable);
                    bh.consume(floatBuf[0]);
                }
            }
        }
    }

    @Benchmark
    public void sliceReadBytesFromClone(ThreadState ts, Blackhole bh) throws IOException {
        byte[] readBuf = new byte[StaticConfigs.CACHE_BLOCK_SIZE];
        for (int fileIdx = 0; fileIdx < ts.numFilesToRead; fileIdx++) {
            IndexInput fileInput = ts.threadInputs[fileIdx];
            for (long offset : ts.blockStartOffsets) {
                long sliceLen = Math.min(StaticConfigs.CACHE_BLOCK_SIZE, ts.fileSize - offset);
                if (sliceLen > 0) {
                    try (IndexInput slice = fileInput.slice("bench-slice", offset, sliceLen)) {
                        slice.readBytes(readBuf, 0, (int) sliceLen);
                        bh.consume(readBuf[0]);
                    }
                }
            }
        }
    }

    @Benchmark
    public void skipBytesFromClone(ThreadState ts, Blackhole bh) throws IOException {
        for (int fileIdx = 0; fileIdx < ts.numFilesToRead; fileIdx++) {
            IndexInput fileInput = ts.threadInputs[fileIdx];
            fileInput.seek(0);
            long pos = 0;
            while (pos + StaticConfigs.CACHE_BLOCK_SIZE < ts.fileSize) {
                fileInput.skipBytes(StaticConfigs.CACHE_BLOCK_SIZE);
                pos += StaticConfigs.CACHE_BLOCK_SIZE;
                if (pos + 1 <= ts.fileSize) {
                    bh.consume(fileInput.readByte());
                    pos++;
                }
            }
        }
    }

    // Mixed workload: randomly exercises all read APIs in a single benchmark
    @Benchmark
    public void mixedReadWorkload(ThreadState ts, Blackhole bh) throws IOException {
        long[] offsets = ts.randomReadByteOffsets;
        byte[] readBuf = new byte[StaticConfigs.CACHE_BLOCK_SIZE];
        int numInts = StaticConfigs.CACHE_BLOCK_SIZE / Integer.BYTES;
        int[] intBuf = new int[numInts];
        int numLongs = StaticConfigs.CACHE_BLOCK_SIZE / Long.BYTES;
        long[] longBuf = new long[numLongs];
        int numFloats = StaticConfigs.CACHE_BLOCK_SIZE / Float.BYTES;
        float[] floatBuf = new float[numFloats];

        for (int fileIdx = 0; fileIdx < ts.numFilesToRead; fileIdx++) {
            IndexInput fileInput = ts.threadInputs[fileIdx];
            RandomAccessInput rai = (RandomAccessInput) fileInput;
            for (int i = 0; i < offsets.length; i++) {
                long offset = offsets[i];
                int op = ThreadLocalRandom.current().nextInt(12);
                switch (op) {
                    case 0 -> bh.consume(rai.readByte(offset));
                    case 1 -> {
                        if (offset + Short.BYTES <= ts.fileSize)
                            bh.consume(rai.readShort(offset));
                    }
                    case 2 -> {
                        if (offset + Integer.BYTES <= ts.fileSize)
                            bh.consume(rai.readInt(offset));
                    }
                    case 3 -> {
                        if (offset + Long.BYTES <= ts.fileSize)
                            bh.consume(rai.readLong(offset));
                    }
                    case 4 -> {
                        int readable = (int) Math.min(readBuf.length, ts.fileSize - offset);
                        if (readable > 0) {
                            rai.readBytes(offset, readBuf, 0, readable);
                            bh.consume(readBuf[0]);
                        }
                    }
                    case 5 -> {
                        fileInput.seek(offset);
                        bh.consume(fileInput.readByte());
                    }
                    case 6 -> {
                        fileInput.seek(offset);
                        int readable = (int) Math.min(readBuf.length, ts.fileSize - offset);
                        if (readable > 0) {
                            fileInput.readBytes(readBuf, 0, readable);
                            bh.consume(readBuf[0]);
                        }
                    }
                    case 7 -> {
                        fileInput.seek(offset);
                        long remaining = ts.fileSize - offset;
                        int readable = (int) Math.min(numInts, remaining / Integer.BYTES);
                        if (readable > 0) {
                            fileInput.readInts(intBuf, 0, readable);
                            bh.consume(intBuf[0]);
                        }
                    }
                    case 8 -> {
                        fileInput.seek(offset);
                        long remaining = ts.fileSize - offset;
                        int readable = (int) Math.min(numLongs, remaining / Long.BYTES);
                        if (readable > 0) {
                            fileInput.readLongs(longBuf, 0, readable);
                            bh.consume(longBuf[0]);
                        }
                    }
                    case 9 -> {
                        fileInput.seek(offset);
                        long remaining = ts.fileSize - offset;
                        int readable = (int) Math.min(numFloats, remaining / Float.BYTES);
                        if (readable > 0) {
                            fileInput.readFloats(floatBuf, 0, readable);
                            bh.consume(floatBuf[0]);
                        }
                    }
                    case 10 -> {
                        long sliceLen = Math.min(StaticConfigs.CACHE_BLOCK_SIZE, ts.fileSize - offset);
                        if (sliceLen > 0) {
                            try (IndexInput slice = fileInput.slice("bench-slice", offset, sliceLen)) {
                                slice.readBytes(readBuf, 0, (int) sliceLen);
                                bh.consume(readBuf[0]);
                            }
                        }
                    }
                    case 11 -> {
                        fileInput.seek(offset);
                        long skip = Math.min(StaticConfigs.CACHE_BLOCK_SIZE, ts.fileSize - offset);
                        if (skip > 0) {
                            fileInput.skipBytes(skip);
                            bh.consume(fileInput.getFilePointer());
                        }
                    }
                }
            }
        }
    }

    /**
     * Simulates DocValues aggregation: many readLong(pos) calls within the same 8KB block
     * before moving to the next block.
     * Pattern: for each block, read 512 longs (4KB of an 8KB block) via RandomAccessInput,
     * then move to the next block. This mirrors sorted numeric DocValues iteration where
     * hundreds of doc values live in the same block.
     */
    @Benchmark
    public void aggregationPatternReadLong(ThreadState ts, Blackhole bh) throws IOException {
        final int blockSize = StaticConfigs.CACHE_BLOCK_SIZE;
        final int readsPerBlock = 512;
        for (int fileIdx = 0; fileIdx < ts.numFilesToRead; fileIdx++) {
            IndexInput fileInput = ts.threadInputs[fileIdx];
            RandomAccessInput in = (RandomAccessInput) fileInput;
            for (long blockStart : ts.blockStartOffsets) {
                long blockEnd = Math.min(blockStart + blockSize, ts.fileSize);
                long usable = blockEnd - blockStart - Long.BYTES;
                if (usable <= 0) continue;
                for (int r = 0; r < readsPerBlock; r++) {
                    long pos = blockStart + ((long) r * Long.BYTES % usable);
                    bh.consume(in.readLong(pos));
                }
            }
        }
    }
}

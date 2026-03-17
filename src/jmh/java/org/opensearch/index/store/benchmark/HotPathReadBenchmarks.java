/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
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
import org.opensearch.index.store.bufferpoolfs.StaticConfigs;

/**
 * Hot path benchmark: all data is already resident in block cache (BufferPool)
 * or page cache (MMap). Measures pure read throughput without I/O latency.
 *
 * <p>Each benchmark method spawns {@code threadCount} concurrent reader threads,
 * each with its own IndexInput clone. This gives true concurrent read contention
 * as a JMH {@code @Param} without relying on {@code @Threads} annotation.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 10, time = 2)
@Fork(value = 2, jvmArgsAppend = { "--enable-native-access=ALL-UNNAMED", "--enable-preview" })
@State(Scope.Benchmark)
@Threads(1)
public class HotPathReadBenchmarks extends ReadBenchmarkBase {

    // Expand to "1", "4", "8", "16", "32" for full sweep
    @Param({ "8" })
    public int threadCount;

    private ExecutorService executor;

    /** Pre-warm caches by reading all files once. */
    @Setup(Level.Trial)
    public void setupHotPathTrial() throws Exception {
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

    @Setup(Level.Iteration)
    public void setupIteration() {
        AtomicInteger counter = new AtomicInteger();
        executor = Executors.newFixedThreadPool(threadCount, r -> {
            Thread t = new Thread(r, "jmh-reader-" + counter.getAndIncrement());
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Runs a task on {@code threadCount} threads concurrently.
     * Waits for all to complete and consumes results via Blackhole.
     */
    private void runConcurrent(ReaderTask task, Blackhole bh) throws Exception {
        List<Future<?>> futures = new ArrayList<>(threadCount);
        for (int t = 0; t < threadCount; t++) {
            futures.add(executor.submit(() -> {
                try {
                    task.run(bh);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        for (Future<?> f : futures) {
            f.get();
        }
    }

    @FunctionalInterface
    interface ReaderTask {
        void run(Blackhole bh) throws IOException;
    }

    // ---- Random reads via clone (crossing block boundaries) ----
    @Benchmark
    public void randomReadByteFromClone(Blackhole bh) throws Exception {
        runConcurrent((hole) -> {
            // Randomly read bytes from distinct blocks across all files
            long[] randomReadOffsets = getRandomReadByteOffsets();
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                RandomAccessInput in = (RandomAccessInput) fileInput;
                for (long offset : randomReadOffsets) {
                    byte b = in.readByte(offset);
                    hole.consume(b);
                }
            }
        }, bh);
    }

    // Sequentially read X bytes from each block (no block boundary crossing)
    @Benchmark
    public void sequentialReadBytesFromClone(Blackhole bh) throws Exception {
        runConcurrent((hole) -> {
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                for (long offset : blockStartOffsets) {
                    int dummyConsumer = 0;
                    fileInput.seek(offset);
                    long remaining = Math.min(sequentialReadNumBytes, fileSize - offset);
                    for (long i = 0; i < remaining; i++) {
                        byte b = fileInput.readByte();
                        dummyConsumer += b;
                    }
                    hole.consume(dummyConsumer);
                }
            }
        }, bh);
    }

    // ======================== RandomAccessInput API benchmarks ========================

    // RandomAccessInput.readShort(long pos) — random positions across blocks
    @Benchmark
    public void randomReadShortFromClone(Blackhole bh) throws Exception {
        runConcurrent((hole) -> {
            long[] offsets = getRandomReadByteOffsets();
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                RandomAccessInput in = (RandomAccessInput) fileInput;
                for (long offset : offsets) {
                    if (offset + Short.BYTES <= fileSize) {
                        hole.consume(in.readShort(offset));
                    }
                }
            }
        }, bh);
    }

    // RandomAccessInput.readInt(long pos) — random positions across blocks
    @Benchmark
    public void randomReadIntFromClone(Blackhole bh) throws Exception {
        runConcurrent((hole) -> {
            long[] offsets = getRandomReadByteOffsets();
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                RandomAccessInput in = (RandomAccessInput) fileInput;
                for (long offset : offsets) {
                    if (offset + Integer.BYTES <= fileSize) {
                        hole.consume(in.readInt(offset));
                    }
                }
            }
        }, bh);
    }

    // RandomAccessInput.readLong(long pos) — random positions across blocks
    @Benchmark
    public void randomReadLongFromClone(Blackhole bh) throws Exception {
        runConcurrent((hole) -> {
            long[] offsets = getRandomReadByteOffsets();
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                RandomAccessInput in = (RandomAccessInput) fileInput;
                for (long offset : offsets) {
                    if (offset + Long.BYTES <= fileSize) {
                        hole.consume(in.readLong(offset));
                    }
                }
            }
        }, bh);
    }

    // RandomAccessInput.readBytes(long pos, byte[], int offset, int length) — bulk random read
    @Benchmark
    public void randomReadBulkBytesFromClone(Blackhole bh) throws Exception {
        runConcurrent((hole) -> {
            long[] offsets = getRandomReadByteOffsets();
            byte[] readBuf = new byte[StaticConfigs.CACHE_BLOCK_SIZE];
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                RandomAccessInput in = (RandomAccessInput) fileInput;
                for (long offset : offsets) {
                    int readable = (int) Math.min(readBuf.length, fileSize - offset);
                    if (readable > 0) {
                        in.readBytes(offset, readBuf, 0, readable);
                        hole.consume(readBuf[0]);
                    }
                }
            }
        }, bh);
    }

    // ======================== IndexInput sequential API benchmarks ========================

    // IndexInput.readBytes(byte[], int, int) — bulk sequential read from block starts
    @Benchmark
    public void sequentialReadBulkBytesFromClone(Blackhole bh) throws Exception {
        runConcurrent((hole) -> {
            byte[] readBuf = new byte[StaticConfigs.CACHE_BLOCK_SIZE];
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                for (long offset : blockStartOffsets) {
                    fileInput.seek(offset);
                    int readable = (int) Math.min(readBuf.length, fileSize - offset);
                    if (readable > 0) {
                        fileInput.readBytes(readBuf, 0, readable);
                        hole.consume(readBuf[0]);
                    }
                }
            }
        }, bh);
    }

    // IndexInput.readShort() — sequential short reads from block starts
    @Benchmark
    public void sequentialReadShortFromClone(Blackhole bh) throws Exception {
        runConcurrent((hole) -> {
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                for (long offset : blockStartOffsets) {
                    if (offset + Short.BYTES <= fileSize) {
                        fileInput.seek(offset);
                        hole.consume(fileInput.readShort());
                    }
                }
            }
        }, bh);
    }

    // IndexInput.readInt() — sequential int reads from block starts
    @Benchmark
    public void sequentialReadIntFromClone(Blackhole bh) throws Exception {
        runConcurrent((hole) -> {
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                for (long offset : blockStartOffsets) {
                    if (offset + Integer.BYTES <= fileSize) {
                        fileInput.seek(offset);
                        hole.consume(fileInput.readInt());
                    }
                }
            }
        }, bh);
    }

    // IndexInput.readLong() — sequential long reads from block starts
    @Benchmark
    public void sequentialReadLongFromClone(Blackhole bh) throws Exception {
        runConcurrent((hole) -> {
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                for (long offset : blockStartOffsets) {
                    if (offset + Long.BYTES <= fileSize) {
                        fileInput.seek(offset);
                        hole.consume(fileInput.readLong());
                    }
                }
            }
        }, bh);
    }

    // IndexInput.readInts(int[], int, int) — bulk int array read from block starts
    @Benchmark
    public void sequentialReadIntsFromClone(Blackhole bh) throws Exception {
        runConcurrent((hole) -> {
            int numInts = StaticConfigs.CACHE_BLOCK_SIZE / Integer.BYTES;
            int[] intBuf = new int[numInts];
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                for (long offset : blockStartOffsets) {
                    long remaining = fileSize - offset;
                    int readable = (int) Math.min(numInts, remaining / Integer.BYTES);
                    if (readable > 0) {
                        fileInput.seek(offset);
                        fileInput.readInts(intBuf, 0, readable);
                        hole.consume(intBuf[0]);
                    }
                }
            }
        }, bh);
    }

    // IndexInput.readLongs(long[], int, int) — bulk long array read from block starts
    @Benchmark
    public void sequentialReadLongsFromClone(Blackhole bh) throws Exception {
        runConcurrent((hole) -> {
            int numLongs = StaticConfigs.CACHE_BLOCK_SIZE / Long.BYTES;
            long[] longBuf = new long[numLongs];
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                for (long offset : blockStartOffsets) {
                    long remaining = fileSize - offset;
                    int readable = (int) Math.min(numLongs, remaining / Long.BYTES);
                    if (readable > 0) {
                        fileInput.seek(offset);
                        fileInput.readLongs(longBuf, 0, readable);
                        hole.consume(longBuf[0]);
                    }
                }
            }
        }, bh);
    }

    // IndexInput.readFloats(float[], int, int) — bulk float array read from block starts
    @Benchmark
    public void sequentialReadFloatsFromClone(Blackhole bh) throws Exception {
        runConcurrent((hole) -> {
            int numFloats = StaticConfigs.CACHE_BLOCK_SIZE / Float.BYTES;
            float[] floatBuf = new float[numFloats];
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                for (long offset : blockStartOffsets) {
                    long remaining = fileSize - offset;
                    int readable = (int) Math.min(numFloats, remaining / Float.BYTES);
                    if (readable > 0) {
                        fileInput.seek(offset);
                        fileInput.readFloats(floatBuf, 0, readable);
                        hole.consume(floatBuf[0]);
                    }
                }
            }
        }, bh);
    }

    // IndexInput.slice() + read — create a slice and read through it
    @Benchmark
    public void sliceReadBytesFromClone(Blackhole bh) throws Exception {
        runConcurrent((hole) -> {
            byte[] readBuf = new byte[StaticConfigs.CACHE_BLOCK_SIZE];
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                for (long offset : blockStartOffsets) {
                    long sliceLen = Math.min(StaticConfigs.CACHE_BLOCK_SIZE, fileSize - offset);
                    if (sliceLen > 0) {
                        try (IndexInput slice = fileInput.slice("bench-slice", offset, sliceLen)) {
                            slice.readBytes(readBuf, 0, (int) sliceLen);
                            hole.consume(readBuf[0]);
                        }
                    }
                }
            }
        }, bh);
    }

    // IndexInput.skipBytes(long) — seek + skip pattern
    @Benchmark
    public void skipBytesFromClone(Blackhole bh) throws Exception {
        runConcurrent((hole) -> {
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                fileInput.seek(0);
                long pos = 0;
                while (pos + StaticConfigs.CACHE_BLOCK_SIZE < fileSize) {
                    fileInput.skipBytes(StaticConfigs.CACHE_BLOCK_SIZE);
                    pos += StaticConfigs.CACHE_BLOCK_SIZE;
                    if (pos + 1 <= fileSize) {
                        hole.consume(fileInput.readByte());
                        pos++;
                    }
                }
            }
        }, bh);
    }

    // Mixed workload: randomly exercises all read APIs in a single benchmark
    // to simulate realistic Lucene read patterns where different APIs are interleaved.
    @Benchmark
    public void mixedReadWorkload(Blackhole bh) throws Exception {
        runConcurrent((hole) -> {
            long[] offsets = getRandomReadByteOffsets();
            byte[] readBuf = new byte[StaticConfigs.CACHE_BLOCK_SIZE];
            int numInts = StaticConfigs.CACHE_BLOCK_SIZE / Integer.BYTES;
            int[] intBuf = new int[numInts];
            int numLongs = StaticConfigs.CACHE_BLOCK_SIZE / Long.BYTES;
            long[] longBuf = new long[numLongs];
            int numFloats = StaticConfigs.CACHE_BLOCK_SIZE / Float.BYTES;
            float[] floatBuf = new float[numFloats];

            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                RandomAccessInput rai = (RandomAccessInput) fileInput;
                for (int i = 0; i < offsets.length; i++) {
                    long offset = offsets[i];
                    int op = ThreadLocalRandom.current().nextInt(12);
                    switch (op) {
                        case 0 -> hole.consume(rai.readByte(offset));
                        case 1 -> {
                            if (offset + Short.BYTES <= fileSize)
                                hole.consume(rai.readShort(offset));
                        }
                        case 2 -> {
                            if (offset + Integer.BYTES <= fileSize)
                                hole.consume(rai.readInt(offset));
                        }
                        case 3 -> {
                            if (offset + Long.BYTES <= fileSize)
                                hole.consume(rai.readLong(offset));
                        }
                        case 4 -> {
                            int readable = (int) Math.min(readBuf.length, fileSize - offset);
                            if (readable > 0) {
                                rai.readBytes(offset, readBuf, 0, readable);
                                hole.consume(readBuf[0]);
                            }
                        }
                        case 5 -> {
                            fileInput.seek(offset);
                            hole.consume(fileInput.readByte());
                        }
                        case 6 -> {
                            fileInput.seek(offset);
                            int readable = (int) Math.min(readBuf.length, fileSize - offset);
                            if (readable > 0) {
                                fileInput.readBytes(readBuf, 0, readable);
                                hole.consume(readBuf[0]);
                            }
                        }
                        case 7 -> {
                            fileInput.seek(offset);
                            long remaining = fileSize - offset;
                            int readable = (int) Math.min(numInts, remaining / Integer.BYTES);
                            if (readable > 0) {
                                fileInput.readInts(intBuf, 0, readable);
                                hole.consume(intBuf[0]);
                            }
                        }
                        case 8 -> {
                            fileInput.seek(offset);
                            long remaining = fileSize - offset;
                            int readable = (int) Math.min(numLongs, remaining / Long.BYTES);
                            if (readable > 0) {
                                fileInput.readLongs(longBuf, 0, readable);
                                hole.consume(longBuf[0]);
                            }
                        }
                        case 9 -> {
                            fileInput.seek(offset);
                            long remaining = fileSize - offset;
                            int readable = (int) Math.min(numFloats, remaining / Float.BYTES);
                            if (readable > 0) {
                                fileInput.readFloats(floatBuf, 0, readable);
                                hole.consume(floatBuf[0]);
                            }
                        }
                        case 10 -> {
                            long sliceLen = Math.min(StaticConfigs.CACHE_BLOCK_SIZE, fileSize - offset);
                            if (sliceLen > 0) {
                                try (IndexInput slice = fileInput.slice("bench-slice", offset, sliceLen)) {
                                    slice.readBytes(readBuf, 0, (int) sliceLen);
                                    hole.consume(readBuf[0]);
                                }
                            }
                        }
                        case 11 -> {
                            fileInput.seek(offset);
                            long skip = Math.min(StaticConfigs.CACHE_BLOCK_SIZE, fileSize - offset);
                            if (skip > 0) {
                                fileInput.skipBytes(skip);
                                hole.consume(fileInput.getFilePointer());
                            }
                        }
                    }
                }
            }
        }, bh);
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() throws IOException {
        if (executor != null) {
            executor.shutdownNow();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            executor = null;
        }
    }

    @TearDown(Level.Trial)
    public void tearDownHotPathTrial() throws Exception {
        super.closeInputs();
        super.tearDownTrial();
    }
}

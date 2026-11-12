/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE_POWER;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Drives readahead with simulated readers and a simulated cache, and counts the reads that
 * miss. A miss here is a foreground read that readahead should have prefetched.
 */
public class ReadaheadSequentialScanTests extends OpenSearchTestCase {

    private static final Path PATH = Paths.get("/test/_0.cfs");
    private static final long FILE_BLOCKS = 20_000;
    private static final long FILE_LENGTH = FILE_BLOCKS << CACHE_BLOCK_SIZE_POWER;
    private static final int SCAN_BLOCKS = 2_000;

    /** A worker that loads blocks into a set, either at once or after a number of reader steps. */
    private static final class SimWorker implements Worker {
        final Set<Long> cached = new HashSet<>();
        final ArrayDeque<long[]> pending = new ArrayDeque<>(); // {startBlock, count, dueStep}
        final int lagSteps;
        long step;
        long prefetched;
        long cancels;
        Runnable onSchedule = () -> {};

        SimWorker(int lagSteps) {
            this.lagSteps = lagSteps;
        }

        @Override
        public <T extends AutoCloseable> boolean schedule(BlockCache<T> blockCache, Path path, long offset, long blockCount) {
            onSchedule.run();
            pending.add(new long[] { offset >>> CACHE_BLOCK_SIZE_POWER, blockCount, step + lagSteps });
            prefetched += blockCount;
            completeDue();
            return true;
        }

        void completeDue() {
            while (!pending.isEmpty() && pending.peek()[2] <= step) {
                long[] t = pending.poll();
                for (long b = t[0]; b < t[0] + t[1]; b++) {
                    cached.add(b);
                }
            }
        }

        /** Reads one block the way the index input does: look it up, load it on a miss, report it. */
        boolean read(ReadaheadContext ctx, long block) {
            step++;
            completeDue();
            boolean hit = cached.contains(block);
            if (!hit) {
                cached.add(block);
            }
            ctx.onAccess(block << CACHE_BLOCK_SIZE_POWER, hit);
            return hit;
        }

        long scan(ReadaheadContext ctx, long from, long count) {
            long misses = 0;
            for (long b = from; b < from + count; b++) {
                if (!read(ctx, b)) {
                    misses++;
                }
            }
            return misses;
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public int getQueueSize() {
            return 0;
        }

        @Override
        public int getQueueCapacity() {
            return 1000;
        }

        @Override
        public void cancel(Path path) {
            cancels++;
            pending.clear();
        }

        @Override
        public void cancelByPathPrefix(Path pathPrefix) {}

        @Override
        public boolean isReadAheadPaused() {
            return false;
        }

        @Override
        public void close() {}
    }

    private static ReadaheadContext open(SimWorker worker) {
        return new ReadaheadManagerImpl(worker, null).register(PATH, FILE_LENGTH);
    }

    public void testScanFromStart() {
        SimWorker worker = new SimWorker(0);
        long misses = worker.scan(open(worker), 0, SCAN_BLOCKS);
        assertTrue("misses=" + misses, misses <= 3);
        assertTrue("prefetched=" + worker.prefetched, worker.prefetched <= SCAN_BLOCKS + 64);
    }

    public void testScanFromMiddleOfFile() {
        SimWorker worker = new SimWorker(0);
        long misses = worker.scan(open(worker), 5_000, SCAN_BLOCKS);
        assertTrue("misses=" + misses, misses <= 3);
        assertFalse("nothing before the reader is prefetched", worker.cached.contains(0L));
    }

    public void testScanAfterFooterRead() {
        // Lucene reads the footer (last block) when it opens a file, then scans from the start.
        SimWorker worker = new SimWorker(0);
        ReadaheadContext ctx = open(worker);
        worker.read(ctx, FILE_BLOCKS - 1);
        long misses = worker.scan(ctx, 0, SCAN_BLOCKS);
        assertTrue("misses=" + misses, misses <= 4);
    }

    public void testScanAfterHeaderAndFooterRead() {
        SimWorker worker = new SimWorker(0);
        ReadaheadContext ctx = open(worker);
        worker.read(ctx, 0);
        worker.read(ctx, FILE_BLOCKS - 1);
        long misses = worker.scan(ctx, 1, SCAN_BLOCKS);
        assertTrue("misses=" + misses, misses <= 4);
    }

    public void testScanAfterAnotherScan() {
        SimWorker worker = new SimWorker(0);
        ReadaheadContext ctx = open(worker);
        worker.scan(ctx, 0, 100);
        long misses = worker.scan(ctx, 5_000, SCAN_BLOCKS);
        assertTrue("misses=" + misses, misses <= 4);
    }

    public void testScanWithSlowWorker() {
        // Prefetched blocks land a few reads after they were requested.
        SimWorker worker = new SimWorker(4);
        long misses = worker.scan(open(worker), 300, SCAN_BLOCKS);
        assertTrue("misses=" + misses, misses <= 12);
    }

    public void testInterleavedReadersOfOneFile() {
        // Two readers of one file (as clones or compound-file slices) take turns, each scanning
        // its own region. Each needs its own readahead state.
        SimWorker worker = new SimWorker(0);
        ReadaheadContext master = open(worker);
        ReadaheadContext a = master.newReaderContext();
        ReadaheadContext b = master.newReaderContext();
        long misses = 0;
        for (long i = 0; i < SCAN_BLOCKS; i++) {
            misses += worker.read(a, 1_000 + i) ? 0 : 1;
            misses += worker.read(b, 10_000 + i) ? 0 : 1;
        }
        assertTrue("misses=" + misses, misses <= 6);
    }

    public void testReaderContextStopsWhenFileCloses() {
        SimWorker worker = new SimWorker(0);
        ReadaheadContext master = open(worker);
        ReadaheadContext reader = master.newReaderContext();
        assertTrue(reader.isReadAheadEnabled());
        master.close();
        assertFalse(reader.isReadAheadEnabled());
        long before = worker.prefetched;
        worker.scan(reader, 0, 100);
        assertEquals("no prefetch after close", before, worker.prefetched);
    }

    public void testRootCloseDuringScheduleCancelsTheTask() {
        // The file closes on another thread while a reader is scheduling: the task it queued
        // must be cancelled, since the close may have cancelled the path before it was queued.
        SimWorker worker = new SimWorker(1_000);
        ReadaheadContext master = open(worker);
        ReadaheadContext reader = master.newReaderContext();
        worker.onSchedule = master::close;
        worker.scan(reader, 0, 10);
        assertTrue("prefetch was scheduled", worker.prefetched > 0);
        assertTrue("task cancelled after close", worker.cancels >= 2);
        assertTrue(worker.pending.isEmpty());
    }

    public void testRandomLookupsByNewReadersPrefetchNothing() {
        // Point lookups: each query clones the input and reads one block, or a block and the next.
        SimWorker worker = new SimWorker(0);
        ReadaheadContext master = open(worker);
        Random random = new Random(randomLong());
        for (int q = 0; q < 2_000; q++) {
            ReadaheadContext reader = master.newReaderContext();
            long block = random.nextInt((int) FILE_BLOCKS - 1);
            worker.read(reader, block);
            if (random.nextBoolean()) {
                worker.read(reader, block + 1);
            }
        }
        assertEquals("prefetched=" + worker.prefetched, 0, worker.prefetched);
    }

    public void testRandomTwoBlockReadsByOneReaderPrefetchNothing() {
        SimWorker worker = new SimWorker(0);
        ReadaheadContext reader = open(worker).newReaderContext();
        Random random = new Random(randomLong());
        for (int q = 0; q < 2_000; q++) {
            long block = random.nextInt((int) FILE_BLOCKS - 1);
            worker.read(reader, block);
            worker.read(reader, block + 1);
        }
        assertEquals("prefetched=" + worker.prefetched, 0, worker.prefetched);
        assertEquals("window did not grow", 4, reader.policy().currentWindow());
    }
}

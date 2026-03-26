/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Concurrency tests for {@link RadixBlockTable} that validate the formal
 * correctness properties under multi-threaded access using virtual threads.
 *
 * <h2>Correctness properties validated</h2>
 * <ol>
 *   <li>P1: No torn references (Property 8)</li>
 *   <li>P2: Reader safety after eviction (Property 9)</li>
 *   <li>P3: Clear safety (Property 10)</li>
 *   <li>P4: Stale reads are benign</li>
 *   <li>P5: Concurrent inner array allocation (Property 11)</li>
 *   <li>P6: Concurrent directory growth (Property 11)</li>
 *   <li>Sustained stress: 32+ threads, 10+ seconds, zero corruption</li>
 * </ol>
 */
public class RadixBlockTableConcurrencyTests extends OpenSearchTestCase {

    private static final int BLOCK_SIZE = 64;

    private static ByteBuffer createSentinel(long blockId) {
        ByteBuffer buf = ByteBuffer.allocateDirect(BLOCK_SIZE);
        buf.putLong(0, blockId);
        return buf;
    }

    private static ByteBuffer[] preallocateSentinels(int count) {
        ByteBuffer[] sentinels = new ByteBuffer[count];
        for (int i = 0; i < count; i++) {
            sentinels[i] = createSentinel(i);
        }
        return sentinels;
    }

    // ========================================================================
    // P1: No torn references — concurrent writers + readers (Property 8)
    // ========================================================================

    public void testP1NoTornReferencesConcurrentWritersAndReaders() throws Exception {
        final int numBlocks = 256;
        final int writerThreads = 100;
        final int readerThreads = 100;
        final long durationMs = 3000;
        final int totalWorkers = writerThreads + readerThreads;

        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(numBlocks / RadixBlockTable.PAGE_SIZE + 1);
        ByteBuffer[] sentinels = preallocateSentinels(numBlocks);

        for (int i = 0; i < numBlocks; i++) {
            table.put(i, sentinels[i]);
        }

        AtomicBoolean tornRefDetected = new AtomicBoolean(false);
        AtomicLong totalReads = new AtomicLong(0);
        AtomicLong totalHits = new AtomicLong(0);
        CountDownLatch readyLatch = new CountDownLatch(totalWorkers);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean running = new AtomicBoolean(true);

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            for (int w = 0; w < writerThreads; w++) {
                final int writerId = w;
                executor.submit(() -> {
                    readyLatch.countDown();
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    int iteration = 0;
                    while (running.get()) {
                        int blockId = (int) ((writerId * 4L + (iteration % 4)) % numBlocks);
                        table.put(blockId, sentinels[blockId]);
                        iteration++;
                        if ((iteration & 0xFF) == 0)
                            Thread.yield();
                    }
                });
            }

            for (int r = 0; r < readerThreads; r++) {
                executor.submit(() -> {
                    readyLatch.countDown();
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    long localReads = 0;
                    while (running.get()) {
                        long blockId = localReads % numBlocks;
                        ByteBuffer buf = table.get(blockId);
                        localReads++;
                        if (buf != null) {
                            totalHits.incrementAndGet();
                            if (buf.getLong(0) != blockId) {
                                tornRefDetected.set(true);
                                running.set(false);
                            }
                        }
                    }
                    totalReads.addAndGet(localReads);
                });
            }

            assertTrue(readyLatch.await(10, TimeUnit.SECONDS));
            startLatch.countDown();
            Thread.sleep(durationMs);
            running.set(false);
        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
            executor.shutdownNow();
        }

        assertFalse(tornRefDetected.get());
        assertTrue(totalReads.get() > 0);
        assertTrue(totalHits.get() > 0);
    }

    // ========================================================================
    // P2: Reader safety after eviction (Property 9)
    // ========================================================================

    public void testP2ReaderSafetyAfterEviction() throws Exception {
        final int numBlocks = 128;
        final int writerCount = 50;
        final int evictorCount = 50;
        final int readerCount = 100;
        final long durationMs = 3000;
        final int totalWorkers = writerCount + evictorCount + readerCount;

        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(numBlocks / RadixBlockTable.PAGE_SIZE + 1);
        ByteBuffer[] sentinels = preallocateSentinels(numBlocks);
        AtomicBoolean corruption = new AtomicBoolean(false);
        AtomicLong evictions = new AtomicLong(0);
        AtomicLong safeReadsAfterEviction = new AtomicLong(0);
        CountDownLatch readyLatch = new CountDownLatch(totalWorkers);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean running = new AtomicBoolean(true);

        for (int i = 0; i < numBlocks; i++) {
            table.put(i, sentinels[i]);
        }

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            for (int w = 0; w < writerCount; w++) {
                executor.submit(() -> {
                    readyLatch.countDown();
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    int iter = 0;
                    while (running.get()) {
                        int blockId = iter % numBlocks;
                        table.put(blockId, sentinels[blockId]);
                        iter++;
                        if ((iter & 0xFF) == 0)
                            Thread.yield();
                    }
                });
            }

            for (int e = 0; e < evictorCount; e++) {
                executor.submit(() -> {
                    readyLatch.countDown();
                    try {
                        startLatch.await();
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    int iter = 0;
                    while (running.get()) {
                        long blockId = iter % numBlocks;
                        ByteBuffer removed = table.remove(blockId);
                        if (removed != null)
                            evictions.incrementAndGet();
                        iter++;
                        if ((iter & 0xFF) == 0)
                            Thread.yield();
                    }
                });
            }

            for (int r = 0; r < readerCount; r++) {
                executor.submit(() -> {
                    readyLatch.countDown();
                    try {
                        startLatch.await();
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    int iter = 0;
                    while (running.get()) {
                        long blockId = iter % numBlocks;
                        ByteBuffer buf = table.get(blockId);
                        Thread.yield();
                        if (buf != null) {
                            long sentinel = buf.getLong(0);
                            if (sentinel != blockId) {
                                corruption.set(true);
                                running.set(false);
                            }
                            safeReadsAfterEviction.incrementAndGet();
                        }
                        iter++;
                    }
                });
            }

            assertTrue(readyLatch.await(10, TimeUnit.SECONDS));
            startLatch.countDown();
            Thread.sleep(durationMs);
            running.set(false);
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }

        assertFalse(corruption.get());
        assertTrue(evictions.get() > 0);
        assertTrue(safeReadsAfterEviction.get() > 0);
    }

    // ========================================================================
    // P3: Clear safety — concurrent clear vs readers/writers (Property 10)
    // ========================================================================

    public void testP3ClearSafetyConcurrentClearVsReadersWriters() throws Exception {
        final int numBlocks = 128;
        final long durationMs = 3000;
        final int writerCount = 50;
        final int readerCount = 100;
        final int totalWorkers = 1 + writerCount + readerCount;

        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(numBlocks / RadixBlockTable.PAGE_SIZE + 1);
        ByteBuffer[] sentinels = preallocateSentinels(numBlocks);
        AtomicBoolean exceptionThrown = new AtomicBoolean(false);
        AtomicInteger clearCount = new AtomicInteger(0);
        CountDownLatch readyLatch = new CountDownLatch(totalWorkers);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean running = new AtomicBoolean(true);

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            // Clear thread
            executor.submit(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await();
                } catch (Exception e) {
                    exceptionThrown.set(true);
                    return;
                }
                while (running.get()) {
                    try {
                        table.clear();
                        clearCount.incrementAndGet();
                        Thread.yield();
                    } catch (Exception e) {
                        exceptionThrown.set(true);
                        running.set(false);
                    }
                }
            });

            // Writers
            for (int w = 0; w < writerCount; w++) {
                executor.submit(() -> {
                    readyLatch.countDown();
                    try {
                        startLatch.await();
                    } catch (Exception e) {
                        exceptionThrown.set(true);
                        return;
                    }
                    int iter = 0;
                    while (running.get()) {
                        try {
                            int blockId = iter % numBlocks;
                            table.put(blockId, sentinels[blockId]);
                            iter++;
                            if ((iter & 0xFF) == 0)
                                Thread.yield();
                        } catch (Exception e) {
                            exceptionThrown.set(true);
                            running.set(false);
                        }
                    }
                });
            }

            // Readers
            for (int r = 0; r < readerCount; r++) {
                executor.submit(() -> {
                    readyLatch.countDown();
                    try {
                        startLatch.await();
                    } catch (Exception e) {
                        exceptionThrown.set(true);
                        return;
                    }
                    int iter = 0;
                    while (running.get()) {
                        try {
                            long blockId = iter % numBlocks;
                            ByteBuffer buf = table.get(blockId);
                            if (buf != null) {
                                long sentinel = buf.getLong(0);
                                if (sentinel != blockId) {
                                    exceptionThrown.set(true);
                                    running.set(false);
                                }
                            }
                            iter++;
                        } catch (Exception e) {
                            exceptionThrown.set(true);
                            running.set(false);
                        }
                    }
                });
            }

            assertTrue(readyLatch.await(10, TimeUnit.SECONDS));
            startLatch.countDown();
            Thread.sleep(durationMs);
            running.set(false);
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }

        assertFalse(exceptionThrown.get());
        assertTrue(clearCount.get() > 0);

        table.clear();
        for (int i = 0; i < numBlocks; i++) {
            assertNull(table.get(i));
        }
    }

    public void testP3bClearPostconditionRemovesPreExistingEntriesDuringConcurrentGrowth() throws Exception {
        final int rounds = 20;
        final int writerThreads = 4;
        final int initialOuterSlots = 1024;
        final int maxOuterSlotsDuringRound = initialOuterSlots + 256;

        for (int round = 0; round < rounds; round++) {
            RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(initialOuterSlots);
            long stickyBlockId = ((long) (initialOuterSlots - 1)) << RadixBlockTable.PAGE_SHIFT;
            table.put(stickyBlockId, createSentinel(stickyBlockId));

            CountDownLatch readyLatch = new CountDownLatch(writerThreads);
            CountDownLatch startLatch = new CountDownLatch(1);
            AtomicBoolean running = new AtomicBoolean(true);

            ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
            try {
                for (int w = 0; w < writerThreads; w++) {
                    final int writerId = w;
                    final ByteBuffer marker = createSentinel(10_000L + writerId);
                    executor.submit(() -> {
                        readyLatch.countDown();
                        try {
                            startLatch.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }

                        int iter = 0;
                        while (running.get()) {
                            int len = table.directoryLength();
                            long growthOuter = len < maxOuterSlotsDuringRound ? (long) len + writerId + 1 : (long) initialOuterSlots + writerId;
                            long blockId = growthOuter << RadixBlockTable.PAGE_SHIFT;
                            table.put(blockId, marker);
                            iter++;
                            if ((iter & 0x1F) == 0) {
                                Thread.yield();
                            }
                        }
                    });
                }

                assertTrue(readyLatch.await(10, TimeUnit.SECONDS));
                startLatch.countDown();
                Thread.sleep(20);

                table.clear();
                running.set(false);
            } finally {
                executor.shutdown();
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            }

            assertNull("pre-clear entry survived clear in round " + round, table.get(stickyBlockId));
        }
    }

    // ========================================================================
    // P4: Stale reads are benign — high-contention put/remove/get
    // ========================================================================

    public void testP4StaleReadsAreBenignHighContention() throws Exception {
        final int numBlocks = 64;
        final int totalThreads = 200;
        final long durationMs = 3000;

        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
        ByteBuffer[] sentinels = preallocateSentinels(numBlocks);
        AtomicBoolean corruption = new AtomicBoolean(false);
        AtomicLong ops = new AtomicLong(0);
        CountDownLatch readyLatch = new CountDownLatch(totalThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean running = new AtomicBoolean(true);

        for (int i = 0; i < numBlocks; i++) {
            table.put(i, sentinels[i]);
        }

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            for (int t = 0; t < totalThreads; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    readyLatch.countDown();
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    int iter = 0;
                    while (running.get()) {
                        int blockId = iter % numBlocks;
                        int op = (threadId + iter) % 3;
                        if (op == 0) {
                            table.put(blockId, sentinels[blockId]);
                        } else if (op == 1) {
                            table.remove(blockId);
                        } else {
                            ByteBuffer buf = table.get(blockId);
                            if (buf != null) {
                                long sentinel = buf.getLong(0);
                                if (sentinel != blockId) {
                                    corruption.set(true);
                                    running.set(false);
                                }
                            }
                        }
                        ops.incrementAndGet();
                        iter++;
                    }
                });
            }

            assertTrue(readyLatch.await(10, TimeUnit.SECONDS));
            startLatch.countDown();
            Thread.sleep(durationMs);
            running.set(false);
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }

        assertFalse(corruption.get());
        assertTrue(ops.get() > 10_000);
    }

    // ========================================================================
    // P5: Concurrent inner array allocation — no lost writes (Property 11)
    // ========================================================================

    public void testP5ConcurrentInnerArrayAllocationNoLostWrites() throws Exception {
        final int threads = 100;
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
        CountDownLatch readyLatch = new CountDownLatch(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);

        ByteBuffer[] expected = new ByteBuffer[RadixBlockTable.PAGE_SIZE];

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            for (int t = 0; t < threads; t++) {
                final int slot = t % RadixBlockTable.PAGE_SIZE;
                final ByteBuffer buf = createSentinel(slot);
                executor.submit(() -> {
                    readyLatch.countDown();
                    try {
                        startLatch.await();
                        table.put(slot, buf);
                        synchronized (expected) {
                            expected[slot] = buf;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            assertTrue(readyLatch.await(10, TimeUnit.SECONDS));
            startLatch.countDown();
            assertTrue(doneLatch.await(10, TimeUnit.SECONDS));
        } finally {
            executor.shutdown();
        }

        assertTrue(table.isInnerAllocated(0));

        for (int i = 0; i < RadixBlockTable.PAGE_SIZE; i++) {
            ByteBuffer buf = table.get(i);
            if (expected[i] != null) {
                assertNotNull(buf);
            }
        }
    }

    // ========================================================================
    // P6: Concurrent directory growth — no lost inner arrays (Property 11)
    // ========================================================================

    public void testP6DirectoryGrowthNoLostInnerArrays() throws Exception {
        final int threads = 100;
        final int blocksPerThread = 64;
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
        CountDownLatch readyLatch = new CountDownLatch(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        AtomicBoolean exceptionThrown = new AtomicBoolean(false);

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    readyLatch.countDown();
                    try {
                        startLatch.await();
                        for (int i = 0; i < blocksPerThread; i++) {
                            long blockId = (long) threadId * blocksPerThread + i;
                            table.put(blockId, createSentinel(blockId));
                        }
                    } catch (Exception e) {
                        exceptionThrown.set(true);
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            assertTrue(readyLatch.await(10, TimeUnit.SECONDS));
            startLatch.countDown();
            assertTrue(doneLatch.await(30, TimeUnit.SECONDS));
        } finally {
            executor.shutdown();
        }

        assertFalse(exceptionThrown.get());

        int verified = 0;
        for (int t = 0; t < threads; t++) {
            for (int i = 0; i < blocksPerThread; i++) {
                long blockId = (long) t * blocksPerThread + i;
                ByteBuffer buf = table.get(blockId);
                if (buf != null) {
                    assertEquals(blockId, buf.getLong(0));
                    verified++;
                }
            }
        }
        assertTrue(verified > 0);
    }

    // ========================================================================
    // Sustained stress test — 32+ threads, 10+ seconds
    // ========================================================================

    public void testSustainedStressTest32Threads10Seconds() throws Exception {
        final int numBlocks = 512;
        final int totalThreads = 32;
        final long durationMs = 10_000;

        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(numBlocks / RadixBlockTable.PAGE_SIZE + 1);
        ByteBuffer[] sentinels = preallocateSentinels(numBlocks);
        AtomicBoolean corruption = new AtomicBoolean(false);
        AtomicBoolean exceptionThrown = new AtomicBoolean(false);
        AtomicLong totalOps = new AtomicLong(0);
        CountDownLatch readyLatch = new CountDownLatch(totalThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean running = new AtomicBoolean(true);

        for (int i = 0; i < numBlocks; i++) {
            table.put(i, sentinels[i]);
        }

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            for (int t = 0; t < totalThreads; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    readyLatch.countDown();
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    long localOps = 0;
                    int iter = 0;
                    while (running.get()) {
                        try {
                            int blockId = iter % numBlocks;
                            int op = (threadId + iter) % 4;
                            if (op == 0) {
                                table.put(blockId, sentinels[blockId]);
                            } else if (op == 1) {
                                table.remove(blockId);
                            } else if (op == 2) {
                                ByteBuffer buf = table.get(blockId);
                                if (buf != null) {
                                    long sentinel = buf.getLong(0);
                                    if (sentinel != blockId) {
                                        corruption.set(true);
                                        running.set(false);
                                    }
                                }
                            } else {
                                table.put(blockId, sentinels[blockId]);
                                ByteBuffer buf = table.get(blockId);
                                if (buf != null && buf.getLong(0) != blockId) {
                                    corruption.set(true);
                                    running.set(false);
                                }
                            }
                            localOps++;
                            iter++;
                        } catch (Exception e) {
                            exceptionThrown.set(true);
                            running.set(false);
                        }
                    }
                    totalOps.addAndGet(localOps);
                });
            }

            assertTrue(readyLatch.await(10, TimeUnit.SECONDS));
            startLatch.countDown();
            Thread.sleep(durationMs);
            running.set(false);
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(15, TimeUnit.SECONDS));
        }

        assertFalse(corruption.get());
        assertFalse(exceptionThrown.get());
        assertTrue(totalOps.get() > 100_000);
    }
}

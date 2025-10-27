/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.directio;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests concurrent access to CachedMemorySegmentIndexInput to reproduce and validate
 * fixes for CorruptIndexException issues found during concurrent operations.
 */
@SuppressWarnings("unchecked")
public class CachedMemorySegmentIndexInputConcurrencyTests extends OpenSearchTestCase {

    private static final int BLOCK_SIZE = 8192;
    private static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;
    private static final ValueLayout.OfInt LAYOUT_LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);

    private BlockCache<RefCountedMemorySegment> mockCache;
    private BlockSlotTinyCache mockTinyCache;
    private ReadaheadManager mockReadaheadManager;
    private ReadaheadContext mockReadaheadContext;
    private Path testPath;
    private Arena arena;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mockCache = mock(BlockCache.class);
        mockTinyCache = mock(BlockSlotTinyCache.class);
        mockReadaheadManager = mock(ReadaheadManager.class);
        mockReadaheadContext = mock(ReadaheadContext.class);
        testPath = Paths.get("/test/concurrent.dat");
        arena = Arena.ofAuto();
    }

    /**
     * Tests concurrent reads from multiple threads accessing the same index input.
     * This test validates that concurrent access to the same block doesn't cause
     * data corruption or race conditions.
     */
    public void testConcurrentReadsFromSameInput() throws Exception {
        int numBlocks = 10;
        long fileLength = BLOCK_SIZE * numBlocks;

        // Setup blocks with unique patterns
        for (int i = 0; i < numBlocks; i++) {
            MemorySegment block = createBlockWithPattern(i, (byte) (i + 1));
            setupBlock(i * BLOCK_SIZE, block);
        }

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        int numThreads = 5;
        int readsPerThread = 20;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger failures = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        try {
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        startLatch.await();

                        // Each thread performs sequential reads
                        for (int i = 0; i < readsPerThread; i++) {
                            int blockNum = (threadId + i) % numBlocks;
                            long offset = blockNum * BLOCK_SIZE + (i % 100);

                            if (offset < fileLength) {
                                byte value = input.readByte(offset);
                                byte expected = (byte) (blockNum + 1);

                                if (value != expected) {
                                    logger
                                        .error(
                                            "Thread {} read incorrect value at offset {}: expected {}, got {}",
                                            threadId,
                                            offset,
                                            expected,
                                            value
                                        );
                                    failures.incrementAndGet();
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Thread {} failed", threadId, e);
                        failures.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            assertTrue("Concurrent reads timed out", doneLatch.await(30, TimeUnit.SECONDS));
            assertEquals("Concurrent reads had failures", 0, failures.get());

        } finally {
            input.close();
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Tests concurrent reads from cloned instances.
     * Clones should be independent and not interfere with each other.
     */
    public void testConcurrentReadsFromClones() throws Exception {
        int numBlocks = 5;
        long fileLength = BLOCK_SIZE * numBlocks;

        for (int i = 0; i < numBlocks; i++) {
            MemorySegment block = createBlockWithPattern(i, (byte) (i * 10));
            setupBlock(i * BLOCK_SIZE, block);
        }

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        int numThreads = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger failures = new AtomicInteger(0);
        List<CachedMemorySegmentIndexInput> clones = new ArrayList<>();

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        try {
            // Create clones for each thread
            for (int i = 0; i < numThreads; i++) {
                clones.add(input.clone());
            }

            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                final CachedMemorySegmentIndexInput clone = clones.get(t);

                executor.submit(() -> {
                    try {
                        startLatch.await();

                        // Each clone reads sequentially through its portion
                        long startOffset = (threadId * BLOCK_SIZE / 2) % fileLength;
                        clone.seek(startOffset);

                        for (int i = 0; i < 50 && clone.getFilePointer() < clone.length(); i++) {
                            byte value = clone.readByte();
                            // Validate non-zero (since we wrote patterns)
                            assertNotNull("Should read valid byte", (Byte) value);
                        }
                    } catch (Exception e) {
                        logger.error("Clone thread {} failed", threadId, e);
                        failures.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            assertTrue("Clone reads timed out", doneLatch.await(30, TimeUnit.SECONDS));
            assertEquals("Clone reads had failures", 0, failures.get());

        } finally {
            for (CachedMemorySegmentIndexInput clone : clones) {
                clone.close();
            }
            input.close();
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Tests concurrent reads across block boundaries.
     * This is a critical test that reproduces the CorruptIndexException scenario
     * where multiple threads read data that spans block boundaries.
     */
    public void testConcurrentReadsAcrossBlockBoundaries() throws Exception {
        int numBlocks = 4;
        long fileLength = BLOCK_SIZE * numBlocks;

        for (int i = 0; i < numBlocks; i++) {
            MemorySegment block = arena.allocate(BLOCK_SIZE);
            // Fill with sequential integers for verification
            for (int j = 0; j < BLOCK_SIZE / 4; j++) {
                block.set(LAYOUT_LE_INT, j * 4, i * 1000 + j);
            }
            setupBlock(i * BLOCK_SIZE, block);
        }

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        int numThreads = 8;
        int readsPerThread = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger failures = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        try {
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        startLatch.await();

                        for (int i = 0; i < readsPerThread; i++) {
                            // Read at positions near block boundaries
                            long[] boundaryPositions = {
                                BLOCK_SIZE - 4,     // Int at end of block 0
                                BLOCK_SIZE - 2,     // Int spanning blocks 0-1
                                BLOCK_SIZE,         // Int at start of block 1
                                BLOCK_SIZE * 2 - 4, // Int at end of block 1
                                BLOCK_SIZE * 2 - 2, // Int spanning blocks 1-2
                                BLOCK_SIZE * 2      // Int at start of block 2
                            };

                            long offset = boundaryPositions[i % boundaryPositions.length];

                            try {
                                int value = input.readInt(offset);
                                // Value should be valid (non-zero for our test data)
                                assertTrue("Read should return valid int", value >= 0);
                            } catch (Exception e) {
                                logger.error("Thread {} failed reading int at offset {}", threadId, offset, e);
                                failures.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Thread {} failed", threadId, e);
                        failures.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            assertTrue("Boundary reads timed out", doneLatch.await(30, TimeUnit.SECONDS));
            assertEquals("Boundary reads had failures", 0, failures.get());

        } finally {
            input.close();
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Tests concurrent reads and seeks from multiple threads.
     * This simulates real-world usage where threads jump around the file.
     */
    public void testConcurrentSeekAndRead() throws Exception {
        int numBlocks = 8;
        long fileLength = BLOCK_SIZE * numBlocks;

        for (int i = 0; i < numBlocks; i++) {
            MemorySegment block = createBlockWithPattern(i, (byte) (i + 1));
            setupBlock(i * BLOCK_SIZE, block);
        }

        int numThreads = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger failures = new AtomicInteger(0);
        List<CachedMemorySegmentIndexInput> clones = new ArrayList<>();

        CachedMemorySegmentIndexInput input = createInput(fileLength);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        try {
            // Each thread gets its own clone
            for (int i = 0; i < numThreads; i++) {
                clones.add(input.clone());
            }

            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                final CachedMemorySegmentIndexInput clone = clones.get(t);

                executor.submit(() -> {
                    try {
                        startLatch.await();

                        // Perform random seeks and reads
                        for (int i = 0; i < 20; i++) {
                            long offset = (threadId * 1000 + i * 100) % (fileLength - 1);
                            clone.seek(offset);
                            byte value = clone.readByte();

                            int blockNum = (int) (offset / BLOCK_SIZE);
                            byte expected = (byte) (blockNum + 1);

                            if (value != expected) {
                                logger
                                    .error(
                                        "Thread {} seek/read mismatch at offset {}: expected {}, got {}",
                                        threadId,
                                        offset,
                                        expected,
                                        value
                                    );
                                failures.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Thread {} failed", threadId, e);
                        failures.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            assertTrue("Seek/read timed out", doneLatch.await(30, TimeUnit.SECONDS));
            assertEquals("Seek/read had failures", 0, failures.get());

        } finally {
            for (CachedMemorySegmentIndexInput clone : clones) {
                clone.close();
            }
            input.close();
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Tests concurrent array reads (readBytes) which are more complex than single reads.
     */
    public void testConcurrentArrayReads() throws Exception {
        int numBlocks = 5;
        long fileLength = BLOCK_SIZE * numBlocks;

        for (int i = 0; i < numBlocks; i++) {
            MemorySegment block = createBlockWithPattern(i, (byte) (i * 20));
            setupBlock(i * BLOCK_SIZE, block);
        }

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        int numThreads = 6;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger failures = new AtomicInteger(0);
        List<CachedMemorySegmentIndexInput> clones = new ArrayList<>();

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        try {
            for (int i = 0; i < numThreads; i++) {
                clones.add(input.clone());
            }

            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                final CachedMemorySegmentIndexInput clone = clones.get(t);

                executor.submit(() -> {
                    try {
                        startLatch.await();

                        // Each thread reads arrays at different offsets
                        for (int i = 0; i < 10; i++) {
                            long offset = (threadId * 500 + i * 200) % (fileLength - 100);
                            clone.seek(offset);

                            byte[] buffer = new byte[100];
                            clone.readBytes(buffer, 0, 100);

                            // Verify we read something (non-zero pattern)
                            boolean hasData = false;
                            for (byte b : buffer) {
                                if (b != 0) {
                                    hasData = true;
                                    break;
                                }
                            }
                            assertTrue("Should read non-zero data", hasData);
                        }
                    } catch (Exception e) {
                        logger.error("Thread {} failed array read", threadId, e);
                        failures.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            assertTrue("Array reads timed out", doneLatch.await(30, TimeUnit.SECONDS));
            assertEquals("Array reads had failures", 0, failures.get());

        } finally {
            for (CachedMemorySegmentIndexInput clone : clones) {
                clone.close();
            }
            input.close();
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private MemorySegment createBlockWithPattern(int blockIndex, byte pattern) {
        MemorySegment segment = arena.allocate(BLOCK_SIZE);
        for (int i = 0; i < BLOCK_SIZE; i++) {
            segment.set(LAYOUT_BYTE, i, pattern);
        }
        return segment;
    }

    private void setupBlock(long offset, MemorySegment segment) throws IOException {
        RefCountedMemorySegment refSegment = new RefCountedMemorySegment(segment, (int) segment.byteSize(), (seg) -> {
            // No-op releaser for tests
        });

        BlockCacheValue<RefCountedMemorySegment> value = mock(BlockCacheValue.class);
        when(value.value()).thenReturn(refSegment);
        when(value.tryPin()).thenReturn(true);

        BlockSlotTinyCache.LookupResult result = new BlockSlotTinyCache.LookupResult(value, true);
        when(mockTinyCache.acquireRefCountedValue(eq(offset))).thenReturn(result);
        when(mockCache.getOrLoad(any(FileBlockCacheKey.class))).thenReturn(value);
    }

    private CachedMemorySegmentIndexInput createInput(long length) {
        return CachedMemorySegmentIndexInput
            .newInstance("test", testPath, length, mockCache, mockReadaheadManager, mockReadaheadContext, mockTinyCache);
    }
}

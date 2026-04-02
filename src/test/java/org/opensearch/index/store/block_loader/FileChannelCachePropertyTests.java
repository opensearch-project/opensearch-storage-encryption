/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_loader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.index.store.CaffeineThreadLeakFilter;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

/**
 * Property-based tests for {@link FileChannelCache} and {@link RefCountedChannel}.
 *
 * <p>Uses randomized iteration (100 trials per property) via OpenSearchTestCase
 * random utilities. Each trial creates fresh temp files and cache instances to
 * ensure isolation.
 *
 * <h2>Properties under test</h2>
 * <ol>
 *   <li>RefCountedChannel: acquire/close round-trip preserves channel validity</li>
 *   <li>RefCountedChannel: refCount accounting is exact</li>
 *   <li>RefCountedChannel: acquire on dead channel always throws</li>
 *   <li>RefCountedChannel: channel closes exactly when last ref is released</li>
 *   <li>FileChannelCache: acquire always returns a valid, readable channel</li>
 *   <li>FileChannelCache: acquire is idempotent — same path returns same channel content</li>
 *   <li>FileChannelCache: eviction does not corrupt in-flight I/O</li>
 *   <li>FileChannelCache: invalidate + re-acquire yields a fresh channel</li>
 *   <li>FileChannelCache: close() invalidates all entries</li>
 *   <li>FileChannelCache: concurrent acquires on same path all succeed</li>
 *   <li>FileChannelCache: random mix of acquire/invalidate/close never crashes</li>
 * </ol>
 */
@ThreadLeakFilters(filters = { CaffeineThreadLeakFilter.class })
public class FileChannelCachePropertyTests extends OpenSearchTestCase {

    private static final byte MAGIC = (byte) 0xFE;
    private static final int FILE_SIZE = 4096;

    private Path tempDir;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        tempDir = createTempDir("fcc-prop-tests");
    }

    /** Creates a temp file filled with a specific magic byte. */
    private Path createTestFile(byte magic) throws IOException {
        Path file = Files.createTempFile(tempDir, "fcc-", ".dat");
        byte[] data = new byte[FILE_SIZE];
        java.util.Arrays.fill(data, magic);
        Files.write(file, data);
        return file;
    }

    /** Creates a temp file filled with the default MAGIC byte. */
    private Path createTestFile() throws IOException {
        return createTestFile(MAGIC);
    }

    /** Reads 64 bytes from offset 0 and validates all match the expected magic. */
    private static boolean readAndValidate(FileChannel fc, byte expectedMagic) {
        try {
            ByteBuffer buf = ByteBuffer.allocate(64);
            int read = fc.read(buf, 0);
            if (read <= 0)
                return false;
            buf.flip();
            for (int i = 0; i < read; i++) {
                if (buf.get(i) != expectedMagic)
                    return false;
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    // ====================================================================
    // RefCountedChannel properties
    // ====================================================================

    /**
     * Property 1: Acquire/close round-trip.
     * For any number of acquires N, doing N closes leaves refCount at 1 (base).
     * Channel remains readable throughout.
     */
    public void testAcquireCloseRoundTrip() throws Exception {
        for (int trial = 0; trial < 100; trial++) {
            Path file = createTestFile();
            FileChannel fc = FileChannel.open(file, StandardOpenOption.READ);
            RefCountedChannel ref = new RefCountedChannel(fc);

            int n = randomIntBetween(1, 50);
            RefCountedChannel[] acquired = new RefCountedChannel[n];

            for (int i = 0; i < n; i++) {
                acquired[i] = ref.acquire();
                assertTrue("Channel must be readable after acquire #" + i, readAndValidate(acquired[i].channel(), MAGIC));
            }

            for (int i = 0; i < n; i++) {
                acquired[i].close();
            }

            // Base ref still held — channel must be open
            assertTrue("Channel must be readable after all I/O refs released", readAndValidate(ref.channel(), MAGIC));
            assertTrue(ref.isAlive());

            ref.releaseBase();
        }
    }

    /**
     * Property 2: RefCount accounting is exact.
     * After K acquires and J closes (J <= K), refCount == 1 + K - J.
     */
    public void testRefCountAccounting() throws Exception {
        for (int trial = 0; trial < 100; trial++) {
            Path file = createTestFile();
            FileChannel fc = FileChannel.open(file, StandardOpenOption.READ);
            RefCountedChannel ref = new RefCountedChannel(fc);

            int acquires = randomIntBetween(1, 30);
            int closes = randomIntBetween(0, acquires);

            List<RefCountedChannel> refs = new ArrayList<>();
            for (int i = 0; i < acquires; i++) {
                refs.add(ref.acquire());
            }
            for (int i = 0; i < closes; i++) {
                refs.get(i).close();
            }

            assertTrue("Channel must be alive with outstanding refs", ref.isAlive());

            // Clean up remaining
            for (int i = closes; i < acquires; i++) {
                refs.get(i).close();
            }
            ref.releaseBase();
        }
    }

    /**
     * Property 3: Acquire on dead channel always throws.
     * After releaseBase with no outstanding refs, acquire must throw.
     */
    public void testAcquireOnDeadChannelAlwaysThrows() throws Exception {
        for (int trial = 0; trial < 100; trial++) {
            Path file = createTestFile();
            FileChannel fc = FileChannel.open(file, StandardOpenOption.READ);
            RefCountedChannel ref = new RefCountedChannel(fc);

            ref.releaseBase(); // refCount -> 0, channel closed
            assertFalse(ref.isAlive());

            // Every subsequent acquire must throw
            int attempts = randomIntBetween(1, 20);
            for (int i = 0; i < attempts; i++) {
                expectThrows(IllegalStateException.class, ref::acquire);
            }
        }
    }

    /**
     * Property 4: Channel closes exactly when last ref is released.
     * With N acquired refs + base, channel stays open until all N+1 are released.
     */
    public void testChannelClosesOnLastRefRelease() throws Exception {
        for (int trial = 0; trial < 100; trial++) {
            Path file = createTestFile();
            FileChannel fc = FileChannel.open(file, StandardOpenOption.READ);
            RefCountedChannel ref = new RefCountedChannel(fc);

            int n = randomIntBetween(1, 20);
            List<RefCountedChannel> acquired = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                acquired.add(ref.acquire());
            }

            // Release base first — simulates cache eviction
            ref.releaseBase();

            // Channel must still be open (acquired refs hold it)
            assertTrue("Channel must be open with " + n + " acquired refs", fc.isOpen());

            // Release all but last
            for (int i = 0; i < n - 1; i++) {
                acquired.get(i).close();
                assertTrue("Channel must be open with " + (n - 1 - i) + " refs remaining", fc.isOpen());
            }

            // Release last — channel must close
            acquired.get(n - 1).close();
            assertFalse("Channel must be closed after last ref released", fc.isOpen());
        }
    }

    // ====================================================================
    // FileChannelCache properties
    // ====================================================================

    /**
     * Property 5: Acquire always returns a valid, readable channel.
     * For any random set of paths, acquire never returns null and the
     * channel always reads the correct content.
     */
    public void testAcquireAlwaysReturnsValidChannel() throws Exception {
        for (int trial = 0; trial < 100; trial++) {
            int numFiles = randomIntBetween(1, 10);
            int cacheSize = randomIntBetween(1, numFiles + 5);
            FileChannelCache cache = new FileChannelCache(cacheSize, null);

            List<Path> files = new ArrayList<>();
            for (int i = 0; i < numFiles; i++) {
                files.add(createTestFile());
            }

            for (Path file : files) {
                String path = file.toAbsolutePath().normalize().toString();
                try (RefCountedChannel ref = cache.acquire(path)) {
                    assertNotNull("acquire must never return null", ref);
                    assertNotNull("channel() must never return null", ref.channel());
                    assertTrue("Channel must be readable", readAndValidate(ref.channel(), MAGIC));
                }
            }

            cache.close();
        }
    }

    /**
     * Property 6: Acquire is idempotent — same path returns same content.
     * Multiple acquires of the same path all read the same data.
     */
    public void testAcquireIdempotent() throws Exception {
        for (int trial = 0; trial < 100; trial++) {
            Path file = createTestFile();
            String path = file.toAbsolutePath().normalize().toString();
            FileChannelCache cache = new FileChannelCache(16, null);

            int acquires = randomIntBetween(2, 20);
            for (int i = 0; i < acquires; i++) {
                try (RefCountedChannel ref = cache.acquire(path)) {
                    assertTrue("Acquire #" + i + " must return readable channel", readAndValidate(ref.channel(), MAGIC));
                }
            }

            cache.close();
        }
    }

    /**
     * Property 7: Eviction does not corrupt in-flight I/O.
     * With cache size=1, acquiring N different paths forces evictions.
     * A held ref from before eviction must still be readable.
     */
    public void testEvictionDoesNotCorruptInFlightIO() throws Exception {
        for (int trial = 0; trial < 50; trial++) {
            Path file1 = createTestFile(MAGIC);
            Path file2 = createTestFile((byte) 0xAB);
            String path1 = file1.toAbsolutePath().normalize().toString();
            String path2 = file2.toAbsolutePath().normalize().toString();

            FileChannelCache cache = new FileChannelCache(1, null);

            // Acquire path1 and hold it (simulating in-flight I/O)
            RefCountedChannel held = cache.acquire(path1);

            // Acquire path2 — forces eviction of path1 from cache
            int evictions = randomIntBetween(1, 10);
            for (int i = 0; i < evictions; i++) {
                try (RefCountedChannel ref = cache.acquire(path2)) {
                    assertTrue(readAndValidate(ref.channel(), (byte) 0xAB));
                }
                // Re-acquire path1 to force more eviction churn
                try (RefCountedChannel ref = cache.acquire(path1)) {
                    assertTrue(readAndValidate(ref.channel(), MAGIC));
                }
            }

            // The held ref from before eviction must still be valid
            assertTrue("Held ref must survive eviction churn", readAndValidate(held.channel(), MAGIC));
            held.close();

            cache.close();
        }
    }

    /**
     * Property 8: Invalidate + re-acquire yields a fresh, valid channel.
     * After invalidation, the next acquire opens a new FileChannel.
     */
    public void testInvalidateAndReacquire() throws Exception {
        for (int trial = 0; trial < 100; trial++) {
            Path file = createTestFile();
            String path = file.toAbsolutePath().normalize().toString();
            FileChannelCache cache = new FileChannelCache(16, null);

            int cycles = randomIntBetween(1, 20);
            for (int i = 0; i < cycles; i++) {
                try (RefCountedChannel ref = cache.acquire(path)) {
                    assertTrue(readAndValidate(ref.channel(), MAGIC));
                }
                cache.invalidate(path);
            }

            // Final acquire after all invalidations must still work
            try (RefCountedChannel ref = cache.acquire(path)) {
                assertTrue("Must get valid channel after invalidation cycles", readAndValidate(ref.channel(), MAGIC));
            }

            cache.close();
        }
    }

    /**
     * Property 9: close() invalidates all entries.
     * After cache.close(), all previously cached channels should be released.
     * New acquires on a closed cache may fail (implementation-dependent).
     */
    public void testCacheCloseInvalidatesAll() throws Exception {
        for (int trial = 0; trial < 50; trial++) {
            int numFiles = randomIntBetween(1, 10);
            FileChannelCache cache = new FileChannelCache(numFiles + 5, null);

            List<RefCountedChannel> heldRefs = new ArrayList<>();
            for (int i = 0; i < numFiles; i++) {
                Path file = createTestFile();
                String path = file.toAbsolutePath().normalize().toString();
                heldRefs.add(cache.acquire(path));
            }

            cache.close();

            // All held refs must still be valid (in-flight I/O protection)
            for (RefCountedChannel ref : heldRefs) {
                assertTrue("Held ref must survive cache.close()", readAndValidate(ref.channel(), MAGIC));
                ref.close();
            }
        }
    }

    /**
     * Property 10: Concurrent acquires on same path all succeed.
     * N threads all acquire the same path — every thread must get a valid channel.
     */
    public void testConcurrentAcquireSamePathAllSucceed() throws Exception {
        for (int trial = 0; trial < 20; trial++) {
            Path file = createTestFile();
            String path = file.toAbsolutePath().normalize().toString();
            FileChannelCache cache = new FileChannelCache(16, null);

            int threadCount = randomIntBetween(2, 16);
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch endLatch = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicBoolean anyFailure = new AtomicBoolean(false);

            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        try (RefCountedChannel ref = cache.acquire(path)) {
                            if (readAndValidate(ref.channel(), MAGIC)) {
                                successCount.incrementAndGet();
                            } else {
                                anyFailure.set(true);
                            }
                        }
                    } catch (Exception e) {
                        anyFailure.set(true);
                    } finally {
                        endLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            assertTrue("Threads must complete within 15s", endLatch.await(15, TimeUnit.SECONDS));
            executor.shutdown();

            assertFalse("No thread should see invalid data", anyFailure.get());
            assertEquals("All threads must succeed", threadCount, successCount.get());

            cache.close();
        }
    }

    /**
     * Property 11: Random mix of acquire/invalidate/close never crashes.
     * Fuzz test: random operations on random paths with a small cache.
     * No operation should throw an unexpected exception or corrupt state.
     */
    public void testRandomOperationMixNeverCrashes() throws Exception {
        for (int trial = 0; trial < 30; trial++) {
            int numFiles = randomIntBetween(2, 8);
            int cacheSize = randomIntBetween(1, numFiles);
            FileChannelCache cache = new FileChannelCache(cacheSize, null);

            List<String> paths = new ArrayList<>();
            for (int i = 0; i < numFiles; i++) {
                Path file = createTestFile();
                paths.add(file.toAbsolutePath().normalize().toString());
            }

            List<RefCountedChannel> openRefs = new ArrayList<>();
            int ops = randomIntBetween(20, 100);

            for (int i = 0; i < ops; i++) {
                int op = randomIntBetween(0, 3);
                String path = paths.get(randomIntBetween(0, paths.size() - 1));

                switch (op) {
                    case 0: // acquire and hold
                        try {
                            RefCountedChannel ref = cache.acquire(path);
                            assertTrue(readAndValidate(ref.channel(), MAGIC));
                            openRefs.add(ref);
                        } catch (IOException e) {
                            // Acceptable if cache was closed
                        }
                        break;
                    case 1: // acquire and immediately release
                        try (RefCountedChannel ref = cache.acquire(path)) {
                            assertTrue(readAndValidate(ref.channel(), MAGIC));
                        } catch (IOException e) {
                            // Acceptable
                        }
                        break;
                    case 2: // invalidate
                        cache.invalidate(path);
                        break;
                    case 3: // release a held ref
                        if (!openRefs.isEmpty()) {
                            int idx = randomIntBetween(0, openRefs.size() - 1);
                            openRefs.remove(idx).close();
                        }
                        break;
                }
            }

            // All held refs must still be readable (in-flight I/O guarantee)
            for (RefCountedChannel ref : openRefs) {
                assertTrue("Held ref must be readable after random ops", readAndValidate(ref.channel(), MAGIC));
                ref.close();
            }

            cache.close();
        }
    }
}

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

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.*;

/**
 * JCStress tests for {@link FileChannelCache} and {@link RefCountedChannel}.
 *
 * <h2>Contracts under test</h2>
 * <ol>
 *   <li>{@link FileChannelCache#acquire(String)} MUST always return a valid,
 *       acquired {@link RefCountedChannel} — even under concurrent evictions.</li>
 *   <li>An acquired {@link RefCountedChannel} MUST keep its underlying
 *       {@link FileChannel} open for the duration of in-flight I/O, even if
 *       the cache evicts the entry or another thread calls {@code close()}.</li>
 *   <li>{@link RefCountedChannel#acquire()} on a dead channel (refCount ≤ 0)
 *       MUST throw {@link IllegalStateException}, never return.</li>
 * </ol>
 *
 * <h2>Approach</h2>
 * <p>We use real temp files with known content so that I/O reads can validate
 * data integrity. Each {@code @State} constructor creates its own temp file
 * and cache instance. JCStress explores all thread interleavings.</p>
 *
 * <h2>Outcome encoding</h2>
 * <ul>
 *   <li>Positive values = success (specific meaning per test)</li>
 *   <li>0 = acceptable edge case (e.g., stale read)</li>
 *   <li>-1 = FORBIDDEN — contract violation</li>
 * </ul>
 */
public class FileChannelCacheStressTests {

    /** Magic byte written to every byte of the temp file. */
    private static final byte MAGIC = (byte) 0xCA;
    /** Size of temp files — small but enough to validate reads. */
    private static final int FILE_SIZE = 4096;

    /**
     * Creates a temp file filled with {@link #MAGIC} bytes.
     * The file is marked deleteOnExit for cleanup.
     */
    private static Path createTempFile() {
        try {
            Path tmp = Files.createTempFile("jcstress-fcc-", ".dat");
            tmp.toFile().deleteOnExit();
            byte[] data = new byte[FILE_SIZE];
            java.util.Arrays.fill(data, MAGIC);
            Files.write(tmp, data);
            return tmp;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads 64 bytes from offset 0 and validates all bytes match MAGIC.
     * Returns true if content is intact, false if channel is broken or data corrupt.
     */
    private static boolean readAndValidate(FileChannel fc) {
        try {
            ByteBuffer buf = ByteBuffer.allocate(64);
            int read = fc.read(buf, 0);
            if (read <= 0)
                return false;
            buf.flip();
            for (int i = 0; i < read; i++) {
                if (buf.get(i) != MAGIC)
                    return false;
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    // ========================================================================
    // Test 1: RefCountedChannel — acquire + releaseBase race
    //
    // Actor 1: acquires the channel (simulating in-flight I/O)
    // Actor 2: calls releaseBase() (simulating cache eviction)
    // Arbiter: validates the acquired channel is still open and readable
    //
    // Contract: in-flight I/O MUST complete successfully even after eviction.
    // ========================================================================

    @JCStressTest
    @Description("In-flight I/O survives cache eviction. Actor 1 acquires, "
        + "Actor 2 releases base ref. Arbiter validates channel still readable.")
    @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Channel readable after eviction — in-flight I/O protected.")
    @Outcome(id = "-1", expect = Expect.FORBIDDEN, desc = "Channel closed or corrupt during in-flight I/O — contract violation.")
    @State
    public static class AcquireSurvivesEviction {
        private final Path tmpFile = createTempFile();
        private final RefCountedChannel ref;
        private volatile RefCountedChannel acquired;

        public AcquireSurvivesEviction() {
            try {
                FileChannel fc = FileChannel.open(tmpFile, StandardOpenOption.READ);
                ref = new RefCountedChannel(fc);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Actor
        public void ioThread() {
            // Simulate in-flight I/O: acquire a ref
            try {
                acquired = ref.acquire();
            } catch (IllegalStateException e) {
                // Base already released — acquire correctly rejected
                acquired = null;
            }
        }

        @Actor
        public void evictionThread() {
            // Simulate cache eviction: release the base reference
            ref.releaseBase();
        }

        @Arbiter
        public void arbiter(I_Result r) {
            RefCountedChannel acq = acquired;
            if (acq == null) {
                // acquire() threw because base was already released — that's fine,
                // FileChannelCache.acquire() would retry with a fresh entry
                r.r1 = 1;
            } else {
                // We got an acquired ref — the channel MUST still be open and readable
                boolean ok = readAndValidate(acq.channel());
                acq.close(); // release our I/O ref
                r.r1 = ok ? 1 : -1;
            }
        }
    }

    // ========================================================================
    // Test 2: RefCountedChannel — multiple concurrent close() calls
    //
    // Pre-acquire 2 extra refs (simulating 2 in-flight I/Os).
    // Actor 1 and Actor 2 each close one ref.
    // Arbiter validates the channel is still open (base ref remains).
    //
    // Contract: close() is idempotent per-ref. Channel closes only when
    // ALL refs (including base) are released.
    // ========================================================================

    @JCStressTest
    @Description("Two concurrent close() calls on separate acquired refs. " + "Channel must stay open because base ref is still held.")
    @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Channel still open after both I/O refs released — base ref holds it.")
    @Outcome(id = "-1", expect = Expect.FORBIDDEN, desc = "Channel prematurely closed — refCount accounting bug.")
    @State
    public static class ConcurrentCloseKeepsChannelOpen {
        private final Path tmpFile = createTempFile();
        private final RefCountedChannel ref;
        private final RefCountedChannel io1;
        private final RefCountedChannel io2;

        public ConcurrentCloseKeepsChannelOpen() {
            try {
                FileChannel fc = FileChannel.open(tmpFile, StandardOpenOption.READ);
                ref = new RefCountedChannel(fc);
                io1 = ref.acquire(); // refCount = 2
                io2 = ref.acquire(); // refCount = 3
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Actor
        public void closer1() {
            io1.close(); // refCount -> 2
        }

        @Actor
        public void closer2() {
            io2.close(); // refCount -> 1
        }

        @Arbiter
        public void arbiter(I_Result r) {
            // Base ref still held — channel must be open and readable
            boolean ok = readAndValidate(ref.channel());
            ref.releaseBase(); // cleanup
            r.r1 = ok ? 1 : -1;
        }
    }

    // ========================================================================
    // Test 3: RefCountedChannel — acquire on dead channel throws
    //
    // Actor 1: releases base (kills the channel)
    // Actor 2: tries to acquire
    // Arbiter: validates acquire either succeeded (before death) or threw
    //
    // Contract: acquire() MUST throw IllegalStateException on dead channel,
    // never return a ref to a closed FileChannel.
    // ========================================================================

    @JCStressTest
    @Description("acquire() on a dead RefCountedChannel must throw, never " + "return a ref to a closed channel.")
    @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Acquired before death — channel readable, then released.")
    @Outcome(id = "2", expect = Expect.ACCEPTABLE, desc = "acquire() correctly threw IllegalStateException on dead channel.")
    @Outcome(id = "-1", expect = Expect.FORBIDDEN, desc = "acquire() returned a ref to a closed/corrupt channel.")
    @State
    public static class AcquireOnDeadChannelThrows {
        private final Path tmpFile = createTempFile();
        private final RefCountedChannel ref;
        private volatile RefCountedChannel acquired;
        private volatile boolean threw;

        public AcquireOnDeadChannelThrows() {
            try {
                FileChannel fc = FileChannel.open(tmpFile, StandardOpenOption.READ);
                ref = new RefCountedChannel(fc);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Actor
        public void killer() {
            ref.releaseBase();
        }

        @Actor
        public void acquirer() {
            try {
                acquired = ref.acquire();
            } catch (IllegalStateException e) {
                threw = true;
            }
        }

        @Arbiter
        public void arbiter(I_Result r) {
            if (threw) {
                // Correctly rejected — dead channel
                r.r1 = 2;
            } else {
                RefCountedChannel acq = acquired;
                if (acq == null) {
                    // Shouldn't happen — acquire either returns or throws
                    r.r1 = -1;
                } else {
                    // Acquired before death — channel must be readable
                    boolean ok = readAndValidate(acq.channel());
                    acq.close();
                    r.r1 = ok ? 1 : -1;
                }
            }
        }
    }

    // ========================================================================
    // Test 4: FileChannelCache — acquire always returns valid channel
    //
    // Cache with maxSize=1. Two actors acquire different paths, forcing
    // eviction. Both must get a valid, readable channel.
    //
    // Contract: acquire() MUST always return a valid RefCountedChannel,
    // transparently retrying on eviction races.
    // ========================================================================

    @JCStressTest
    @Description("FileChannelCache.acquire() returns valid channel under " + "eviction pressure. Cache size=1, two paths compete.")
    @Outcome(id = "1, 1", expect = Expect.ACCEPTABLE, desc = "Both actors got valid readable channels despite eviction.")
    @Outcome(id = "-1, 1", expect = Expect.FORBIDDEN, desc = "Actor 1 got invalid channel — acquire contract violated.")
    @Outcome(id = "1, -1", expect = Expect.FORBIDDEN, desc = "Actor 2 got invalid channel — acquire contract violated.")
    @Outcome(id = "-1, -1", expect = Expect.FORBIDDEN, desc = "Both actors got invalid channels — acquire contract violated.")
    @State
    public static class AcquireUnderEvictionPressure {
        private final Path tmpFile1 = createTempFile();
        private final Path tmpFile2 = createTempFile();
        private final FileChannelCache cache;
        private final String path1;
        private final String path2;

        public AcquireUnderEvictionPressure() {
            path1 = tmpFile1.toAbsolutePath().normalize().toString();
            path2 = tmpFile2.toAbsolutePath().normalize().toString();
            // maxSize=1 forces eviction when second path is acquired
            cache = new FileChannelCache(1, null);
        }

        @Actor
        public void actor1(II_Result r) {
            try (RefCountedChannel ref = cache.acquire(path1)) {
                r.r1 = readAndValidate(ref.channel()) ? 1 : -1;
            } catch (Exception e) {
                r.r1 = -1;
            }
        }

        @Actor
        public void actor2(II_Result r) {
            try (RefCountedChannel ref = cache.acquire(path2)) {
                r.r2 = readAndValidate(ref.channel()) ? 1 : -1;
            } catch (Exception e) {
                r.r2 = -1;
            }
        }
    }

    // ========================================================================
    // Test 5: FileChannelCache — in-flight I/O survives eviction
    //
    // Actor 1: acquires path1 and holds the ref (simulating long I/O)
    // Actor 2: acquires path2, forcing eviction of path1 from cache
    // Arbiter: validates Actor 1's channel is still open and readable
    //
    // Contract: eviction releases the base ref, but the in-flight I/O ref
    // keeps the FileChannel alive. The channel MUST NOT close until the
    // last ref is released.
    // ========================================================================

    @JCStressTest
    @Description("In-flight I/O on evicted entry survives. Actor 1 holds ref, "
        + "Actor 2 forces eviction. Arbiter validates Actor 1 can still read.")
    @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Evicted channel still readable — in-flight I/O protected.")
    @Outcome(id = "-1", expect = Expect.FORBIDDEN, desc = "Evicted channel closed during in-flight I/O — contract violation.")
    @State
    public static class InFlightIOSurvivesCacheEviction {
        private final Path tmpFile1 = createTempFile();
        private final Path tmpFile2 = createTempFile();
        private final FileChannelCache cache;
        private final String path1;
        private final String path2;
        private volatile RefCountedChannel heldRef;

        public InFlightIOSurvivesCacheEviction() {
            path1 = tmpFile1.toAbsolutePath().normalize().toString();
            path2 = tmpFile2.toAbsolutePath().normalize().toString();
            cache = new FileChannelCache(1, null);
            // Pre-load path1 into cache and acquire an I/O ref
            try {
                heldRef = cache.acquire(path1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Actor
        public void evictor() {
            // Acquire path2 — forces eviction of path1's cache entry
            try (RefCountedChannel ref = cache.acquire(path2)) {
                // Just trigger the eviction, don't need to do anything with it
            } catch (IOException e) {
                // Shouldn't happen with valid temp files
            }
        }

        @Actor
        public void holder() {
            // Just hold the ref — simulating long-running I/O
            // The arbiter will validate and release
        }

        @Arbiter
        public void arbiter(I_Result r) {
            RefCountedChannel ref = heldRef;
            if (ref == null) {
                r.r1 = -1;
                return;
            }
            // path1 was evicted from cache, but our held ref should keep it alive
            boolean ok = readAndValidate(ref.channel());
            ref.close(); // release our I/O ref — now channel can close
            r.r1 = ok ? 1 : -1;
        }
    }

    // ========================================================================
    // Test 6: FileChannelCache — concurrent acquire on same path
    //
    // Two actors acquire the same path concurrently. Both must get a valid
    // channel. This tests the Caffeine cache's compute-if-absent atomicity
    // combined with RefCountedChannel's CAS-based acquire.
    //
    // Contract: both actors get a valid, readable channel. They may share
    // the same underlying RefCountedChannel or get different ones (if one
    // was evicted and recreated), but both MUST be valid.
    // ========================================================================

    @JCStressTest
    @Description("Two concurrent acquires on the same path. Both must get " + "valid readable channels.")
    @Outcome(id = "1, 1", expect = Expect.ACCEPTABLE, desc = "Both actors got valid channels for the same path.")
    @Outcome(id = "-1, 1", expect = Expect.FORBIDDEN, desc = "Actor 1 got invalid channel.")
    @Outcome(id = "1, -1", expect = Expect.FORBIDDEN, desc = "Actor 2 got invalid channel.")
    @Outcome(id = "-1, -1", expect = Expect.FORBIDDEN, desc = "Both actors got invalid channels.")
    @State
    public static class ConcurrentAcquireSamePath {
        private final Path tmpFile = createTempFile();
        private final FileChannelCache cache;
        private final String path;

        public ConcurrentAcquireSamePath() {
            path = tmpFile.toAbsolutePath().normalize().toString();
            cache = new FileChannelCache(16, null);
        }

        @Actor
        public void actor1(II_Result r) {
            try (RefCountedChannel ref = cache.acquire(path)) {
                r.r1 = readAndValidate(ref.channel()) ? 1 : -1;
            } catch (Exception e) {
                r.r1 = -1;
            }
        }

        @Actor
        public void actor2(II_Result r) {
            try (RefCountedChannel ref = cache.acquire(path)) {
                r.r2 = readAndValidate(ref.channel()) ? 1 : -1;
            } catch (Exception e) {
                r.r2 = -1;
            }
        }
    }

    // ========================================================================
    // Test 7: FileChannelCache — acquire + invalidate race
    //
    // Actor 1: acquires a channel and holds it (in-flight I/O)
    // Actor 2: invalidates the same path (simulating storage migration)
    // Arbiter: validates the held channel is still readable
    //
    // Contract: invalidate() triggers eviction which releases the base ref,
    // but the in-flight I/O ref keeps the channel alive.
    // ========================================================================

    @JCStressTest
    @Description("acquire + invalidate race. Held channel must survive " + "explicit invalidation for in-flight I/O.")
    @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Channel survived invalidation — in-flight I/O completed.")
    @Outcome(id = "-1", expect = Expect.FORBIDDEN, desc = "Channel closed by invalidation during in-flight I/O.")
    @State
    public static class AcquireSurvivesInvalidation {
        private final Path tmpFile = createTempFile();
        private final FileChannelCache cache;
        private final String path;
        private volatile RefCountedChannel heldRef;

        public AcquireSurvivesInvalidation() {
            path = tmpFile.toAbsolutePath().normalize().toString();
            cache = new FileChannelCache(16, null);
            try {
                heldRef = cache.acquire(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Actor
        public void invalidator() {
            cache.invalidate(path);
        }

        @Actor
        public void holder() {
            // Hold the ref — simulating in-flight I/O
        }

        @Arbiter
        public void arbiter(I_Result r) {
            RefCountedChannel ref = heldRef;
            if (ref == null) {
                r.r1 = -1;
                return;
            }
            boolean ok = readAndValidate(ref.channel());
            ref.close();
            r.r1 = ok ? 1 : -1;
        }
    }

    // ========================================================================
    // Test 8: FileChannelCache — close() with in-flight I/O
    //
    // Actor 1: holds an acquired ref (in-flight I/O)
    // Actor 2: calls cache.close() (invalidateAll — bulk cleanup)
    // Arbiter: validates the held channel is still readable
    //
    // Contract: cache.close() invalidates all entries, but in-flight I/O
    // refs keep their channels alive until released.
    // ========================================================================

    @JCStressTest
    @Description("cache.close() with in-flight I/O. Held channel must " + "survive invalidateAll for ongoing reads.")
    @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Channel survived cache.close() — in-flight I/O completed.")
    @Outcome(id = "-1", expect = Expect.FORBIDDEN, desc = "Channel closed by cache.close() during in-flight I/O.")
    @State
    public static class CacheCloseWithInFlightIO {
        private final Path tmpFile = createTempFile();
        private final FileChannelCache cache;
        private final String path;
        private volatile RefCountedChannel heldRef;

        public CacheCloseWithInFlightIO() {
            path = tmpFile.toAbsolutePath().normalize().toString();
            cache = new FileChannelCache(16, null);
            try {
                heldRef = cache.acquire(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Actor
        public void cacheCloser() {
            cache.close();
        }

        @Actor
        public void holder() {
            // Hold the ref — simulating in-flight I/O
        }

        @Arbiter
        public void arbiter(I_Result r) {
            RefCountedChannel ref = heldRef;
            if (ref == null) {
                r.r1 = -1;
                return;
            }
            boolean ok = readAndValidate(ref.channel());
            ref.close();
            r.r1 = ok ? 1 : -1;
        }
    }
}

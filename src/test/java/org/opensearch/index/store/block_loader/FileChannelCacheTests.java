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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.CaffeineThreadLeakFilter;
import org.opensearch.index.store.bufferpoolfs.StaticConfigs;
import org.opensearch.index.store.pool.PoolSizeCalculator;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

/**
 * Unit tests for {@link FileChannelCache} covering:
 * <ul>
 *   <li>Node setting defaults and overrides for max file channels and expire-after-access</li>
 *   <li>3-arg constructor with expiry enabled</li>
 *   <li>Time-based expiry behavior</li>
 *   <li>Metrics recording via {@link FileChannelCache#recordStats()}</li>
 *   <li>Size-based eviction combined with time-based expiry</li>
 * </ul>
 */
@ThreadLeakFilters(filters = { CaffeineThreadLeakFilter.class })
public class FileChannelCacheTests extends OpenSearchTestCase {

    private static final byte MAGIC = (byte) 0xCA;
    private static final int FILE_SIZE = 4096;

    private Path tempDir;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        tempDir = createTempDir("fcc-unit-tests");
    }

    private Path createTestFile() throws IOException {
        Path file = Files.createTempFile(tempDir, "fcc-", ".dat");
        byte[] data = new byte[FILE_SIZE];
        java.util.Arrays.fill(data, MAGIC);
        Files.write(file, data);
        return file;
    }

    private static boolean readAndValidate(FileChannel fc, byte expected) {
        try {
            ByteBuffer buf = ByteBuffer.allocate(64);
            int read = fc.read(buf, 0);
            if (read <= 0)
                return false;
            buf.flip();
            for (int i = 0; i < read; i++) {
                if (buf.get(i) != expected)
                    return false;
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    // ====================================================================
    // Setting defaults
    // ====================================================================

    /**
     * NODE_MAX_FILE_CHANNELS_SETTING defaults to StaticConfigs.DEFAULT_MAX_FILE_CHANNELS (256).
     */
    public void testMaxFileChannelsSettingDefault() {
        Settings settings = Settings.EMPTY;
        int value = PoolSizeCalculator.NODE_MAX_FILE_CHANNELS_SETTING.get(settings);
        assertEquals(StaticConfigs.DEFAULT_MAX_FILE_CHANNELS, value);
        assertEquals(256, value);
    }

    /**
     * NODE_FD_CACHE_EXPIRE_SECONDS_SETTING defaults to StaticConfigs.DEFAULT_FD_CACHE_EXPIRE_AFTER_ACCESS_SECONDS (300).
     */
    public void testFdCacheExpireSettingDefault() {
        Settings settings = Settings.EMPTY;
        long value = PoolSizeCalculator.NODE_FD_CACHE_EXPIRE_SECONDS_SETTING.get(settings);
        assertEquals(StaticConfigs.DEFAULT_FD_CACHE_EXPIRE_AFTER_ACCESS_SECONDS, value);
        assertEquals(300L, value);
    }

    // ====================================================================
    // Setting overrides
    // ====================================================================

    /**
     * NODE_MAX_FILE_CHANNELS_SETTING can be overridden via Settings.
     */
    public void testMaxFileChannelsSettingOverride() {
        Settings settings = Settings.builder().put("node.store.crypto.max_file_channels", 512).build();
        assertEquals(512, (int) PoolSizeCalculator.NODE_MAX_FILE_CHANNELS_SETTING.get(settings));
    }

    /**
     * NODE_FD_CACHE_EXPIRE_SECONDS_SETTING can be overridden via Settings.
     */
    public void testFdCacheExpireSettingOverride() {
        Settings settings = Settings.builder().put("node.store.crypto.fd_cache_expire_after_access_seconds", 600).build();
        assertEquals(600L, (long) PoolSizeCalculator.NODE_FD_CACHE_EXPIRE_SECONDS_SETTING.get(settings));
    }

    /**
     * Setting expire to 0 disables time-based expiry.
     */
    public void testFdCacheExpireSettingZeroDisablesExpiry() {
        Settings settings = Settings.builder().put("node.store.crypto.fd_cache_expire_after_access_seconds", 0).build();
        assertEquals(0L, (long) PoolSizeCalculator.NODE_FD_CACHE_EXPIRE_SECONDS_SETTING.get(settings));
    }

    /**
     * NODE_MAX_FILE_CHANNELS_SETTING rejects values below 1.
     */
    public void testMaxFileChannelsSettingRejectsZero() {
        Settings settings = Settings.builder().put("node.store.crypto.max_file_channels", 0).build();
        expectThrows(IllegalArgumentException.class, () -> PoolSizeCalculator.NODE_MAX_FILE_CHANNELS_SETTING.get(settings));
    }

    /**
     * NODE_FD_CACHE_EXPIRE_SECONDS_SETTING rejects negative values.
     */
    public void testFdCacheExpireSettingRejectsNegative() {
        Settings settings = Settings.builder().put("node.store.crypto.fd_cache_expire_after_access_seconds", -1).build();
        expectThrows(IllegalArgumentException.class, () -> PoolSizeCalculator.NODE_FD_CACHE_EXPIRE_SECONDS_SETTING.get(settings));
    }

    // ====================================================================
    // 3-arg constructor with expiry
    // ====================================================================

    /**
     * FileChannelCache with expiry enabled still acquires and reads correctly.
     */
    public void testConstructorWithExpiry() throws Exception {
        Path file = createTestFile();
        String path = file.toAbsolutePath().normalize().toString();

        FileChannelCache cache = new FileChannelCache(16, 60, null);
        try (RefCountedChannel ref = cache.acquire(path)) {
            assertTrue("Channel must be readable with expiry enabled", readAndValidate(ref.channel(), MAGIC));
        }
        cache.close();
    }

    /**
     * FileChannelCache with expiry=0 (disabled) still works correctly.
     */
    public void testConstructorWithExpiryDisabled() throws Exception {
        Path file = createTestFile();
        String path = file.toAbsolutePath().normalize().toString();

        FileChannelCache cache = new FileChannelCache(16, 0, null);
        try (RefCountedChannel ref = cache.acquire(path)) {
            assertTrue("Channel must be readable with expiry disabled", readAndValidate(ref.channel(), MAGIC));
        }
        cache.close();
    }

    /**
     * 2-arg constructor (test convenience) delegates to 3-arg with expire=0.
     */
    public void testTwoArgConstructorEquivalent() throws Exception {
        Path file = createTestFile();
        String path = file.toAbsolutePath().normalize().toString();

        FileChannelCache cache = new FileChannelCache(16, null);
        try (RefCountedChannel ref = cache.acquire(path)) {
            assertTrue(readAndValidate(ref.channel(), MAGIC));
        }
        cache.close();
    }

    // ====================================================================
    // Time-based expiry behavior
    // ====================================================================

    /**
     * Entries expire after the configured access timeout.
     * Uses a raw Caffeine cache with 50ms expiry and a CountDownLatch in the
     * eviction listener to detect expiry. Spins cleanUp() to trigger maintenance.
     */
    public void testExpireAfterAccess() throws Exception {
        Path file = createTestFile();
        String path = file.toAbsolutePath().normalize().toString();

        CountDownLatch expired = new CountDownLatch(1);

        Cache<String, RefCountedChannel> cache = Caffeine
            .newBuilder()
            .maximumSize(16)
            .expireAfterAccess(50, TimeUnit.MILLISECONDS)
            .evictionListener((String key, RefCountedChannel ref, RemovalCause cause) -> {
                if (ref != null) {
                    ref.releaseBase();
                    if (key.equals(path)) {
                        expired.countDown();
                    }
                }
            })
            .build();

        // Populate cache
        FileChannel fc = FileChannel.open(file, StandardOpenOption.READ);
        RefCountedChannel ref = new RefCountedChannel(fc);
        cache.put(path, ref);

        // Acquire + release (refCount: 1→2→1)
        ref.acquire();
        ref.close();

        // Spin cleanUp until eviction fires — Caffeine is lazy, needs maintenance trigger
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (expired.getCount() > 0 && System.nanoTime() < deadline) {
            cache.cleanUp();
        }
        assertEquals("Entry must expire", 0, expired.getCount());

        // Channel must be closed after expiry (no I/O refs held)
        assertFalse("Channel must be closed after time-based expiry", fc.isOpen());

        cache.cleanUp();
    }

    /**
     * Accessing a cached entry resets the expiry timer.
     * Uses a raw Caffeine cache with 200ms expiry. Repeatedly accesses the entry
     * via cleanUp-spin loops shorter than the expiry, then verifies it's still alive.
     * Uses a CountDownLatch to detect if expiry fires (it shouldn't).
     */
    public void testAccessResetsExpiryTimer() throws Exception {
        Path file = createTestFile();
        String path = file.toAbsolutePath().normalize().toString();

        CountDownLatch expired = new CountDownLatch(1);

        Cache<String, RefCountedChannel> cache = Caffeine
            .newBuilder()
            .maximumSize(16)
            .expireAfterAccess(200, TimeUnit.MILLISECONDS)
            .evictionListener((String key, RefCountedChannel ref, RemovalCause cause) -> {
                if (ref != null) {
                    ref.releaseBase();
                    if (key.equals(path)) {
                        expired.countDown();
                    }
                }
            })
            .build();

        FileChannel fc = FileChannel.open(file, StandardOpenOption.READ);
        RefCountedChannel ref1 = new RefCountedChannel(fc);
        cache.put(path, ref1);

        // Spin for 500ms total (well past the 200ms expiry window), accessing every ~100ms.
        // Each getIfPresent resets the access timer, so it should never expire.
        long start = System.nanoTime();
        long spinDuration = TimeUnit.MILLISECONDS.toNanos(500);
        long accessInterval = TimeUnit.MILLISECONDS.toNanos(100);
        long lastAccess = start;
        while (System.nanoTime() - start < spinDuration) {
            cache.cleanUp();
            if (System.nanoTime() - lastAccess >= accessInterval) {
                RefCountedChannel r = cache.getIfPresent(path);
                assertNotNull("Entry must still be cached", r);
                lastAccess = System.nanoTime();
            }
        }

        // Latch must NOT have fired — entry was kept alive by repeated access
        assertEquals("Entry must not have expired during repeated access", 1, expired.getCount());
        assertTrue("Channel must still be open", fc.isOpen());

        // Cleanup
        ref1.releaseBase();
        cache.cleanUp();
    }

    // ====================================================================
    // Size + time eviction combined
    // ====================================================================

    /**
     * Both size-based and time-based eviction work together.
     * Uses a raw Caffeine cache with maxSize=2 and 50ms expiry.
     * CountDownLatches track eviction events for each path.
     * Spin-cleanUp to trigger maintenance — no sleeps.
     */
    public void testSizeAndTimeEvictionCombined() throws Exception {
        Path file1 = createTestFile();
        Path file2 = createTestFile();
        Path file3 = createTestFile();
        String path1 = file1.toAbsolutePath().normalize().toString();
        String path2 = file2.toAbsolutePath().normalize().toString();
        String path3 = file3.toAbsolutePath().normalize().toString();

        CountDownLatch path1Evicted = new CountDownLatch(1);
        CountDownLatch path2Evicted = new CountDownLatch(1);

        Cache<String, RefCountedChannel> cache = Caffeine
            .newBuilder()
            .maximumSize(2)
            .expireAfterAccess(50, TimeUnit.MILLISECONDS)
            .evictionListener((String key, RefCountedChannel ref, RemovalCause cause) -> {
                if (ref != null) {
                    ref.releaseBase();
                    if (key.equals(path1))
                        path1Evicted.countDown();
                    if (key.equals(path2))
                        path2Evicted.countDown();
                }
            })
            .build();

        // Fill cache to capacity (2 entries)
        FileChannel fc1 = FileChannel.open(file1, StandardOpenOption.READ);
        RefCountedChannel ref1 = new RefCountedChannel(fc1);
        cache.put(path1, ref1);

        FileChannel fc2 = FileChannel.open(file2, StandardOpenOption.READ);
        RefCountedChannel ref2 = new RefCountedChannel(fc2);
        cache.put(path2, ref2);

        // Add 3rd entry — triggers size-based eviction of path1
        FileChannel fc3 = FileChannel.open(file3, StandardOpenOption.READ);
        RefCountedChannel ref3 = new RefCountedChannel(fc3);
        cache.put(path3, ref3);

        // Spin cleanUp until path1 is evicted by size
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (path1Evicted.getCount() > 0 && System.nanoTime() < deadline) {
            cache.cleanUp();
        }
        assertEquals("path1 must be evicted by size", 0, path1Evicted.getCount());
        assertFalse("path1 channel must be closed", fc1.isOpen());

        // Spin cleanUp until path2 expires by time (50ms)
        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (path2Evicted.getCount() > 0 && System.nanoTime() < deadline) {
            cache.cleanUp();
        }
        assertEquals("path2 must expire by time", 0, path2Evicted.getCount());
        assertFalse("path2 channel must be closed after time expiry", fc2.isOpen());

        // Cleanup
        ref3.releaseBase();
        cache.cleanUp();
    }

    // ====================================================================
    // Metrics recording
    // ====================================================================

    /**
     * recordStats() does not throw when CryptoMetricsService is not initialized.
     * The method catches IllegalStateException internally.
     */
    public void testRecordStatsWithoutMetricsServiceDoesNotThrow() throws Exception {
        Path file = createTestFile();
        String path = file.toAbsolutePath().normalize().toString();

        FileChannelCache cache = new FileChannelCache(16, 60, null);

        // Generate some hits and misses
        try (RefCountedChannel ref = cache.acquire(path)) {
            assertTrue(readAndValidate(ref.channel(), MAGIC));
        }
        // Second acquire should be a hit
        try (RefCountedChannel ref = cache.acquire(path)) {
            assertTrue(readAndValidate(ref.channel(), MAGIC));
        }

        // Should not throw even without CryptoMetricsService initialized
        cache.recordStats();

        cache.close();
    }

    /**
     * estimatedSize() reflects the number of cached entries.
     */
    public void testEstimatedSizeReflectsCacheState() throws Exception {
        FileChannelCache cache = new FileChannelCache(16, null);

        assertEquals("Empty cache should have size 0", 0, cache.estimatedSize());

        List<String> paths = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Path f = createTestFile();
            String p = f.toAbsolutePath().normalize().toString();
            paths.add(p);
            try (RefCountedChannel ref = cache.acquire(p)) {
                assertTrue(readAndValidate(ref.channel(), MAGIC));
            }
        }
        assertEquals("Cache should have 5 entries", 5, cache.estimatedSize());

        // Invalidate 2
        cache.invalidate(paths.get(0));
        cache.invalidate(paths.get(1));
        assertEquals("Cache should have 3 entries after invalidation", 3, cache.estimatedSize());

        cache.close();
        assertEquals("Cache should be empty after close", 0, cache.estimatedSize());
    }

    // ====================================================================
    // In-flight I/O safety with expiry
    // ====================================================================

    /**
     * A held RefCountedChannel survives time-based expiry.
     * The channel stays open for in-flight I/O even after the cache evicts it.
     * Uses a raw Caffeine cache with 50ms expiry and spin-cleanUp to trigger
     * maintenance. CountDownLatch detects the eviction event.
     */
    public void testHeldRefSurvivesTimeExpiry() throws Exception {
        Path file = createTestFile();
        String path = file.toAbsolutePath().normalize().toString();

        CountDownLatch expired = new CountDownLatch(1);

        Cache<String, RefCountedChannel> cache = Caffeine
            .newBuilder()
            .maximumSize(16)
            .expireAfterAccess(50, TimeUnit.MILLISECONDS)
            .evictionListener((String key, RefCountedChannel ref, RemovalCause cause) -> {
                if (ref != null) {
                    ref.releaseBase();
                    if (key.equals(path)) {
                        expired.countDown();
                    }
                }
            })
            .build();

        // Insert and acquire I/O ref (refCount: 1→2)
        FileChannel fc = FileChannel.open(file, StandardOpenOption.READ);
        RefCountedChannel ref1 = new RefCountedChannel(fc);
        cache.put(path, ref1);
        ref1.acquire(); // refCount=2 (base + I/O)

        // Spin cleanUp until eviction fires
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (expired.getCount() > 0 && System.nanoTime() < deadline) {
            cache.cleanUp();
        }
        assertEquals("Eviction listener must fire", 0, expired.getCount());

        // Key invariant: channel must STILL be open — I/O ref is held (refCount=1)
        assertTrue("Held ref must survive time-based expiry", fc.isOpen());
        assertTrue("Must still be able to read", readAndValidate(fc, MAGIC));

        // Release I/O ref → refCount 1→0 → channel closes
        ref1.close();
        assertFalse("Channel must close after last ref released", fc.isOpen());

        cache.cleanUp();
    }

    public void testInvalidateByPathPrefix() throws Exception {
        // Create files in two different "shard" directories
        Path shardA = tempDir.resolve("shardA");
        Path shardB = tempDir.resolve("shardB");
        Files.createDirectories(shardA);
        Files.createDirectories(shardB);

        Path fileA1 = Files.write(shardA.resolve("seg_0.dat"), new byte[] { MAGIC });
        Path fileA2 = Files.write(shardA.resolve("seg_1.dat"), new byte[] { MAGIC });
        Path fileB1 = Files.write(shardB.resolve("seg_0.dat"), new byte[] { MAGIC });

        String pathA1 = fileA1.toAbsolutePath().normalize().toString();
        String pathA2 = fileA2.toAbsolutePath().normalize().toString();
        String pathB1 = fileB1.toAbsolutePath().normalize().toString();

        FileChannelCache cache = new FileChannelCache(16, null);

        // Populate cache with all 3 files
        try (RefCountedChannel r = cache.acquire(pathA1)) {
            assertTrue(r.channel().isOpen());
        }
        try (RefCountedChannel r = cache.acquire(pathA2)) {
            assertTrue(r.channel().isOpen());
        }
        try (RefCountedChannel r = cache.acquire(pathB1)) {
            assertTrue(r.channel().isOpen());
        }
        assertEquals(3, cache.estimatedSize());

        // Invalidate shardA prefix — should remove A1 and A2, keep B1
        cache.invalidateByPathPrefix(shardA);

        // shardB entry should still be in cache and accessible
        try (RefCountedChannel r = cache.acquire(pathB1)) {
            assertTrue("shardB file should still be accessible", r.channel().isOpen());
        }
        assertEquals(1, cache.estimatedSize());
        cache.close();
    }

    public void testInvalidateByPathPrefixWithHeldRef() throws Exception {
        Path subDir = tempDir.resolve("shard");
        Files.createDirectories(subDir);
        Path file = Files.write(subDir.resolve("seg.dat"), new byte[] { MAGIC });
        String path = file.toAbsolutePath().normalize().toString();

        FileChannelCache cache = new FileChannelCache(16, null);

        // Acquire and hold — simulating in-flight I/O
        RefCountedChannel held = cache.acquire(path);

        // Invalidate by prefix while ref is held
        cache.invalidateByPathPrefix(subDir);

        // Key invariant: held ref must still be valid — channel stays open for in-flight I/O
        assertTrue("Held ref must survive prefix invalidation", held.channel().isOpen());
        assertTrue("Must still be able to read through held ref", readAndValidate(held.channel(), MAGIC));

        held.close();
        cache.close();
    }

    /**
     * Verifies that invalidate() actually closes the underlying FileChannel
     * when no I/O refs are held. This catches FD leaks where the eviction
     * listener fails to call releaseBase() on explicit invalidation.
     */
    public void testInvalidateClosesIdleChannel() throws Exception {
        Path file = createTestFile();
        String path = file.toAbsolutePath().normalize().toString();

        FileChannelCache cache = new FileChannelCache(16, null);

        // Acquire and immediately release — channel is now idle in cache
        FileChannel fc;
        try (RefCountedChannel ref = cache.acquire(path)) {
            fc = ref.channel();
            assertTrue("Channel should be open during I/O", fc.isOpen());
        }
        // At this point: refCount=1 (only the cache's base ref)

        // Invalidate the entry — this MUST close the channel since no I/O is in flight
        cache.invalidate(path);

        // The channel must be closed — if not, we have an FD leak
        assertFalse("FileChannel must be closed after invalidation with no held refs (FD leak)", fc.isOpen());

        cache.close();
    }

    /**
     * Double-close on RefCountedChannel must not go negative or throw.
     * Guards against accidental double-release from eviction + manual invalidation.
     */
    public void testRefCountedChannelDoubleCloseIsIdempotent() throws Exception {
        Path file = createTestFile();
        FileChannel fc = FileChannel.open(file, StandardOpenOption.READ);
        RefCountedChannel ref = new RefCountedChannel(fc);

        // First close releases the base ref (1 -> 0) and closes the channel
        ref.close();
        assertFalse("Channel should be closed after first close()", fc.isOpen());

        // Second close must be a no-op — no exception, no negative refCount
        ref.close();
        assertFalse("Channel should still be closed after double close()", fc.isOpen());
        assertFalse("RefCountedChannel should not be alive after double close", ref.isAlive());
    }

    // ====================================================================
    // Eviction listener + CountDownLatch tests
    // ====================================================================

    /**
     * Size-based eviction with in-flight I/O: proves the eviction listener fires
     * exactly once (latch goes from 1→0), the channel stays open while a ref is
     * held, and closes only after the last ref is released.
     *
     * Uses a raw Caffeine cache with evictionListener that counts down a latch
     * per evicted path. No sleeps — purely latch-driven synchronization.
     */
    public void testSizeEvictionWithEvictionListenerLatch() throws Exception {
        Path file1 = createTestFile();
        Path file2 = createTestFile();
        String path1 = file1.toAbsolutePath().normalize().toString();
        String path2 = file2.toAbsolutePath().normalize().toString();

        // Latch: counts down when eviction listener fires for path1
        CountDownLatch path1Evicted = new CountDownLatch(1);

        Cache<String, RefCountedChannel> cache = Caffeine
            .newBuilder()
            .maximumSize(1)
            .evictionListener((String key, RefCountedChannel ref, RemovalCause cause) -> {
                if (ref != null) {
                    ref.releaseBase();
                    if (key.equals(path1)) {
                        path1Evicted.countDown();
                    }
                }
            })
            .build();

        // Insert path1, acquire I/O ref (refCount: 1→2)
        FileChannel fc1 = FileChannel.open(file1, StandardOpenOption.READ);
        RefCountedChannel ref1 = new RefCountedChannel(fc1);
        cache.put(path1, ref1);
        ref1.acquire(); // refCount=2 (base + I/O)

        // Latch count must be 1 — eviction hasn't happened yet
        assertEquals("Eviction must not have fired yet", 1, path1Evicted.getCount());
        assertTrue("Channel must be open before eviction", fc1.isOpen());

        // Insert path2 into size=1 cache → triggers eviction of path1
        FileChannel fc2 = FileChannel.open(file2, StandardOpenOption.READ);
        RefCountedChannel ref2 = new RefCountedChannel(fc2);
        cache.put(path2, ref2);
        cache.cleanUp(); // force Caffeine maintenance

        // Wait for eviction listener — it fires synchronously during cleanUp
        assertTrue("Eviction listener must fire for path1", path1Evicted.await(5, TimeUnit.SECONDS));

        // Latch is now 0 — eviction listener fired exactly once
        assertEquals("Eviction listener must have fired exactly once", 0, path1Evicted.getCount());

        // Key invariant: channel must STILL be open — I/O ref is held (refCount=1)
        assertTrue("Channel must survive eviction while I/O ref is held", fc1.isOpen());
        assertTrue("Must still be able to read through held ref", readAndValidate(fc1, MAGIC));

        // Release the I/O ref → refCount 1→0 → channel closes
        ref1.close();
        assertFalse("Channel must close after last ref released", fc1.isOpen());

        // Cleanup
        ref2.releaseBase();
        cache.cleanUp();
    }

    /**
     * Proves that with an ASYNC removalListener, explicit removal (asMap().remove())
     * does NOT close the FD synchronously — there is a leak window where the FD
     * stays open because the listener runs on ForkJoinPool.commonPool().
     *
     * Uses CountDownLatch to block the async listener, proving the FD is still open
     * after removal. Then unblocks the listener and verifies it eventually closes.
     *
     * Contrasts with FileChannelCache.invalidate() which does manual releaseBase()
     * inline — closing the FD synchronously with no leak window.
     */
    public void testAsyncRemovalListenerLeakWindowVsSyncManualRelease() throws Exception {
        Path file = createTestFile();
        String path = file.toAbsolutePath().normalize().toString();

        // --- Part 1: async removalListener has a leak window ---
        CountDownLatch allowListenerToRun = new CountDownLatch(1);
        CountDownLatch listenerDone = new CountDownLatch(1);

        Cache<String, RefCountedChannel> asyncCache = Caffeine
            .newBuilder()
            .maximumSize(16)
            .removalListener((String key, RefCountedChannel ref, RemovalCause cause) -> {
                try {
                    allowListenerToRun.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (ref != null) {
                    ref.releaseBase();
                }
                listenerDone.countDown();
            })
            .build();

        FileChannel fc1 = FileChannel.open(file, StandardOpenOption.READ);
        RefCountedChannel ref1 = new RefCountedChannel(fc1);
        asyncCache.put(path, ref1);

        // Simulate acquire + release (refCount: 1→2→1, base only)
        ref1.acquire();
        ref1.close();

        // Explicit removal — async listener is blocked
        RefCountedChannel removed = asyncCache.asMap().remove(path);
        assertSame(ref1, removed);

        // Listener hasn't run yet — FD is STILL OPEN (this is the leak window)
        assertEquals("Listener must not have completed yet", 1, listenerDone.getCount());
        assertTrue("FD must still be open — async listener blocked (LEAK WINDOW)", fc1.isOpen());

        // Unblock the listener
        allowListenerToRun.countDown();
        assertTrue("Listener must complete", listenerDone.await(5, TimeUnit.SECONDS));
        assertFalse("FD closed after async listener finally runs", fc1.isOpen());

        // --- Part 2: our invalidate() closes FD synchronously ---
        Path file2 = createTestFile();
        String path2 = file2.toAbsolutePath().normalize().toString();

        FileChannelCache fdCache = new FileChannelCache(16, null);
        FileChannel fc2;
        try (RefCountedChannel ref = fdCache.acquire(path2)) {
            fc2 = ref.channel();
        }
        assertTrue("Channel idle in cache, must be open", fc2.isOpen());

        fdCache.invalidate(path2);

        // No latch needed — invalidate() is synchronous
        assertFalse("FD must be closed immediately after invalidate() (no leak window)", fc2.isOpen());
        fdCache.close();
    }

    /**
     * Concurrent invalidate() racing with acquire() — uses CountDownLatch to
     * orchestrate the interleaving deterministically.
     *
     * Thread A: acquire(path) → holds ref (refCount=2)
     * Thread B: invalidate(path) → manual releaseBase (refCount 2→1), signals latch
     * Thread A: awaits latch, verifies channel still open, releases (refCount 1→0, closes)
     *
     * Proves: manual releaseBase in invalidate() fires synchronously (latch goes 0),
     * channel survives while I/O ref held, closes when last ref released.
     */
    public void testConcurrentInvalidateWhileAcquireHeld() throws Exception {
        Path file = createTestFile();
        String path = file.toAbsolutePath().normalize().toString();

        FileChannelCache cache = new FileChannelCache(16, null);

        CountDownLatch threadAHoldsRef = new CountDownLatch(1);
        CountDownLatch threadBInvalidated = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<FileChannel> channelRef = new AtomicReference<>();

        Thread threadA = new Thread(() -> {
            try {
                RefCountedChannel held = cache.acquire(path);
                channelRef.set(held.channel());
                assertTrue("Channel must be open", held.channel().isOpen());

                threadAHoldsRef.countDown();

                assertTrue("Timed out waiting for invalidation", threadBInvalidated.await(5, TimeUnit.SECONDS));

                // Channel must STILL be open — I/O ref held (refCount=1)
                assertTrue("Channel must survive invalidation while ref held", held.channel().isOpen());
                assertTrue("Must still read correctly", readAndValidate(held.channel(), MAGIC));

                // Release → refCount 1→0 → channel closes
                held.close();
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "thread-A-io");

        Thread threadB = new Thread(() -> {
            try {
                assertTrue("Timed out waiting for Thread A", threadAHoldsRef.await(5, TimeUnit.SECONDS));

                cache.invalidate(path);

                // invalidate() is synchronous — releaseBase already called
                threadBInvalidated.countDown();
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "thread-B-invalidator");

        threadA.start();
        threadB.start();
        threadA.join(10_000);
        threadB.join(10_000);

        assertNull("Thread failure: " + failure.get(), failure.get());
        assertFalse("Channel must be closed after all refs released (FD leak)", channelRef.get().isOpen());

        cache.close();
    }

    /**
     * Verifies that invalidateByPathPrefix() closes all idle FileChannels
     * under the prefix. This catches FD leaks on shard close.
     */
    public void testInvalidateByPathPrefixClosesIdleChannels() throws Exception {
        Path shardDir = tempDir.resolve("shard");
        Files.createDirectories(shardDir);

        Path file1 = Files.write(shardDir.resolve("seg_0.dat"), new byte[] { MAGIC });
        Path file2 = Files.write(shardDir.resolve("seg_1.dat"), new byte[] { MAGIC });
        String path1 = file1.toAbsolutePath().normalize().toString();
        String path2 = file2.toAbsolutePath().normalize().toString();

        FileChannelCache cache = new FileChannelCache(16, null);

        // Populate cache, then release I/O refs — channels are idle
        FileChannel fc1, fc2;
        try (RefCountedChannel ref = cache.acquire(path1)) {
            fc1 = ref.channel();
        }
        try (RefCountedChannel ref = cache.acquire(path2)) {
            fc2 = ref.channel();
        }

        assertTrue("Channel 1 should be open while cached", fc1.isOpen());
        assertTrue("Channel 2 should be open while cached", fc2.isOpen());

        // Invalidate by prefix — both channels must close (no in-flight I/O)
        cache.invalidateByPathPrefix(shardDir);

        assertFalse("FileChannel 1 must be closed after prefix invalidation (FD leak)", fc1.isOpen());
        assertFalse("FileChannel 2 must be closed after prefix invalidation (FD leak)", fc2.isOpen());

        cache.close();
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.cache;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.metrics.FileOpenTracker;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

/**
 * A two-level cache for FileChannel instances.
 *
 * <p>Primary: Caffeine Cache keyed by (path|options) → FileChannel.
 * Secondary: ConcurrentHashMap keyed by directory path → Set of cache keys.
 *
 * <p>The secondary index enables O(1) directory-level invalidation (e.g., shard close/delete)
 * instead of O(n) linear scan over all cache keys.
 *
 * <p>The primary key includes open options because a FileChannel opened with READ
 * cannot be used for Direct I/O — the DIRECT flag must be set at open time.
 */
public class FileChannelCache {
    private static final Logger LOGGER = LogManager.getLogger(FileChannelCache.class);
    private static final int MAX_ENTRIES = 100_000;
    private static final Duration EXPIRE_AFTER_ACCESS = Duration.ofMinutes(10);

    /** Primary cache: cacheKey → FileChannel */
    private static final Cache<String, FileChannel> CACHE = Caffeine
        .newBuilder()
        .maximumSize(MAX_ENTRIES)
        .expireAfterAccess(EXPIRE_AFTER_ACCESS)
        .recordStats()
        .removalListener((String key, FileChannel channel, RemovalCause cause) -> {
            if (channel != null) {
                boolean wasOpen = channel.isOpen();
                try {
                    channel.close();
                } catch (IOException e) {
                    LOGGER.warn("Failed to close FileChannel on {}: key={}", cause, key, e);
                }
                LOGGER.info("FileChannelCache removal: cause={}, wasOpen={}, key={}", cause, wasOpen, key);
            }
            // Clean secondary index on Caffeine-initiated eviction (SIZE, EXPIRED, COLLECTED)
            if (cause != RemovalCause.EXPLICIT && cause != RemovalCause.REPLACED && key != null) {
                removeFromDirIndex(key);
            }
        })
        .build();

    /** Secondary index: directoryPath → Set of cache keys in that directory */
    private static final ConcurrentHashMap<String, Set<String>> DIR_INDEX = new ConcurrentHashMap<>();

    private static String buildKey(Path path, OpenOption... options) {
        String pathKey = path.toAbsolutePath().normalize().toString();
        if (options == null || options.length == 0) {
            return pathKey;
        }
        String optionsKey = Arrays.stream(options).map(Object::toString).sorted().reduce((a, b) -> a + "," + b).orElse("");
        return pathKey + "|" + optionsKey;
    }

    private static String dirKey(Path path) {
        return path.toAbsolutePath().normalize().getParent().toString();
    }

    /**
     * Removes a cache key from the secondary directory index.
     * Extracts the directory from the key (everything before the filename).
     */
    private static void removeFromDirIndex(String cacheKey) {
        // cacheKey format: "/abs/path/to/file|OPTIONS" or "/abs/path/to/file"
        // Extract directory: everything up to the last '/' before '|'
        String pathPart = cacheKey.contains("|") ? cacheKey.substring(0, cacheKey.indexOf('|')) : cacheKey;
        int lastSlash = pathPart.lastIndexOf('/');
        if (lastSlash > 0) {
            String dir = pathPart.substring(0, lastSlash);
            Set<String> dirSet = DIR_INDEX.get(dir);
            if (dirSet != null) {
                dirSet.remove(cacheKey);
                if (dirSet.isEmpty()) {
                    DIR_INDEX.remove(dir, Set.of()); // only remove if still empty
                }
            }
        }
    }

    /**
     * Returns a cached FileChannel for the given path and options, opening one if absent.
     * Atomic — no TOCTOU race. Also registers the key in the secondary directory index.
     *
     * @throws IOException if the file cannot be opened
     */
    public static FileChannel getOrOpen(Path path, OpenOption... options) throws IOException {
        String key = buildKey(path, options);
        try {
            FileChannel channel = CACHE.get(key, k -> {
                try {
                    FileOpenTracker.trackOpen(path.toAbsolutePath().normalize().toString());
                    LOGGER.info("FileChannelCache OPEN: key={}", k);
                    return FileChannel.open(path, options);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            // Register in secondary index (idempotent — concurrent set handles duplicates)
            String dir = dirKey(path);
            DIR_INDEX.computeIfAbsent(dir, d -> ConcurrentHashMap.newKeySet()).add(key);
            return channel;
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /**
     * Invalidates cached channels for a single file. Uses the secondary index to find
     * all option variants (READ, READ+DIRECT) for the file in O(k) where k = variants (typically 1-2).
     * Triggers the removal listener which closes the channels.
     */
    public static void invalidate(Path path) {
        String dir = dirKey(path);
        String pathStr = path.toAbsolutePath().normalize().toString();
        Set<String> dirSet = DIR_INDEX.get(dir);
        if (dirSet != null) {
            // Find all keys for this file (e.g., "path|READ", "path|DIRECT,READ")
            var keysToRemove = dirSet.stream().filter(k -> {
                String keyPath = k.contains("|") ? k.substring(0, k.indexOf('|')) : k;
                return keyPath.equals(pathStr);
            }).toList();
            if (!keysToRemove.isEmpty()) {
                LOGGER.info("FileChannelCache invalidate file: path={}, keys={}", pathStr, keysToRemove.size());
                CACHE.invalidateAll(keysToRemove);
                keysToRemove.forEach(dirSet::remove);
                if (dirSet.isEmpty()) {
                    DIR_INDEX.remove(dir);
                }
            }
        }
    }

    /**
     * Invalidates all cached channels for a directory (e.g., shard close/delete).
     * O(1) lookup + O(k) invalidation where k = files in the directory.
     */
    public static void invalidateDirectory(Path dirPath) {
        String dir = dirPath.toAbsolutePath().normalize().toString();
        Set<String> keys = DIR_INDEX.remove(dir);
        if (keys != null && !keys.isEmpty()) {
            LOGGER.info("FileChannelCache invalidateDirectory: dir={}, keys={}", dir, keys.size());
            CACHE.invalidateAll(keys);
        }
    }

    /**
     * Closes all cached channels and clears the secondary index.
     */
    public static void closeAll() {
        LOGGER.info("FileChannelCache closeAll: cacheSize={}, dirIndexSize={}", CACHE.estimatedSize(), DIR_INDEX.size());
        CACHE.invalidateAll();
        DIR_INDEX.clear();
    }

    public static long size() {
        return CACHE.estimatedSize();
    }

    public static int dirIndexSize() {
        return DIR_INDEX.size();
    }

    /**
     * Returns cache statistics for debugging/monitoring.
     */
    public static String stats() {
        var s = CACHE.stats();
        return String
            .format(
                "FileChannelCache[size=%d, dirs=%d, hits=%d, misses=%d, hitRate=%.2f%%, evictions=%d, avgLoadMs=%.2f]",
                CACHE.estimatedSize(),
                DIR_INDEX.size(),
                s.hitCount(),
                s.missCount(),
                s.hitRate() * 100,
                s.evictionCount(),
                s.averageLoadPenalty() / 1_000_000.0
            );
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_loader;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

import org.opensearch.index.store.metrics.CryptoMetricsService;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

/**
 * Node-level cache of {@link FileChannel} instances, bounded by max open FDs
 * and optional time-based expiry for idle channels.
 *
 * <p>Each cached entry is a {@link RefCountedChannel}. The cache holds one "base"
 * reference. When Caffeine evicts an entry (size pressure, time expiry, or explicit
 * invalidation), the base ref is released via {@link RefCountedChannel#releaseBase()}.
 * If no I/O is in flight, the FileChannel closes immediately. If I/O is in flight,
 * the channel stays open until the last I/O completes.
 *
 * <p>Eviction policies (both active simultaneously):
 * <ul>
 *   <li>Size-based: LRU eviction when cache reaches {@code maxOpenFDs}</li>
 *   <li>Time-based: channels not accessed within {@code expireAfterAccessSeconds}
 *       are evicted (disabled when set to 0)</li>
 * </ul>
 *
 * <p>Usage pattern in BlockLoader:
 * <pre>{@code
 *   try (RefCountedChannel ref = fileChannelCache.acquire(path)) {
 *       ref.channel().read(buf, offset);
 *   }
 * }</pre>
 *
 * <p>For bulk cleanup (e.g., storage migration): call {@link #close()} which
 * invalidates all entries. Idle channels close immediately. In-flight channels
 * close when I/O finishes.
 */
public class FileChannelCache implements Closeable {

    private static final int MAX_ACQUIRE_RETRIES = 3;

    private final Cache<String, RefCountedChannel> fdCache;
    private final OpenOption directOpenOption;

    /**
     * @param maxOpenFDs maximum number of cached FileChannels
     * @param expireAfterAccessSeconds seconds of idle time before a channel is evicted (0 to disable)
     * @param directOpenOption the O_DIRECT open option, or null for buffered I/O
     */
    public FileChannelCache(int maxOpenFDs, long expireAfterAccessSeconds, OpenOption directOpenOption) {
        this.directOpenOption = directOpenOption;
        Caffeine<String, RefCountedChannel> builder = Caffeine
            .newBuilder()
            .maximumSize(maxOpenFDs)
            .recordStats()
            .evictionListener((String path, RefCountedChannel ref, RemovalCause cause) -> {
                // evictionListener fires synchronously and ONLY for automatic evictions
                // (size pressure, time expiry). Manual invalidations are handled explicitly
                // in invalidate(), invalidateByPathPrefix(), and close() to guarantee
                // synchronous FileChannel closure and avoid FD leaks.
                if (ref != null) {
                    ref.releaseBase();
                }
            });
        if (expireAfterAccessSeconds > 0) {
            builder.expireAfterAccess(expireAfterAccessSeconds, TimeUnit.SECONDS);
        }
        this.fdCache = builder.build();
    }

    /**
     * Convenience constructor without time-based expiry (size-based eviction only).
     * Primarily for tests.
     *
     * @param maxOpenFDs maximum number of cached FileChannels
     * @param directOpenOption the O_DIRECT open option, or null for buffered I/O
     */
    public FileChannelCache(int maxOpenFDs, OpenOption directOpenOption) {
        this(maxOpenFDs, 0, directOpenOption);
    }

    /**
     * Acquire a {@link RefCountedChannel} for the given path. The returned ref
     * has already been acquired (refCount incremented) and MUST be closed when
     * done, typically via try-with-resources.
     *
     * <p>If the cached entry was concurrently evicted (stale), this method
     * transparently evicts the stale entry and retries up to {@link #MAX_ACQUIRE_RETRIES} times.
     *
     * @param normalizedPath the absolute normalized file path
     * @return an acquired RefCountedChannel — never null
     * @throws IOException if the channel cannot be opened or acquired after retries
     */
    public RefCountedChannel acquire(String normalizedPath) throws IOException {
        for (int attempt = 0; attempt < MAX_ACQUIRE_RETRIES; attempt++) {
            RefCountedChannel ref;
            try {
                ref = fdCache.get(normalizedPath, this::openChannel);
            } catch (RuntimeException e) {
                // Caffeine wraps loader exceptions in RuntimeException.
                // Unwrap so callers see the original IOException (e.g., NoSuchFileException).
                if (e.getCause() instanceof IOException ioe) {
                    throw ioe;
                }
                throw e;
            }
            try {
                return ref.acquire();
            } catch (IllegalStateException e) {
                // Stale entry — evict and release its base ref to close the FD.
                // asMap().remove(key, value) is a conditional remove — only removes
                // if the mapping still points to this exact ref (avoids racing with
                // a concurrent loader that already replaced it).
                if (fdCache.asMap().remove(normalizedPath, ref)) {
                    ref.releaseBase();
                }
            }
        }
        throw new IOException("Failed to acquire FileChannel after " + MAX_ACQUIRE_RETRIES + " retries: " + normalizedPath);
    }

    /**
     * Invalidate the cached channel for a specific path.
     * Synchronously releases the base reference to close the FileChannel
     * (or mark it for deferred close if I/O is in flight).
     */
    public void invalidate(String normalizedPath) {
        RefCountedChannel ref = fdCache.asMap().remove(normalizedPath);
        if (ref != null) {
            ref.releaseBase();
        }
    }

    /**
     * Invalidate all cached channels whose path starts with the given prefix.
     * Used when a shard directory is closed or an index is deleted.
     * Synchronously releases base references for all matching entries.
     *
     * @param prefix the directory path prefix (e.g., shard or index directory)
     */
    public void invalidateByPathPrefix(Path prefix) {
        String normalized = prefix.toAbsolutePath().normalize().toString();
        // Ensure prefix ends with separator to avoid matching /indices/abc when prefix is /indices/ab
        String prefixStr = normalized.endsWith("/") ? normalized : normalized + "/";
        var iterator = fdCache.asMap().entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (entry.getKey().startsWith(prefixStr)) {
                iterator.remove();
                entry.getValue().releaseBase();
            }
        }
    }

    /**
     * Returns the estimated number of entries in the cache.
     */
    public long estimatedSize() {
        return fdCache.estimatedSize();
    }

    /**
     * Trigger pending maintenance operations (evictions, size recalculation).
     * Caffeine caches are lazy — this forces immediate processing.
     */
    public void cleanUp() {
        fdCache.cleanUp();
    }

    /**
     * Publish FD cache statistics to the metrics service.
     */
    public void recordStats() {
        var stats = fdCache.stats();
        try {
            CryptoMetricsService
                .getInstance()
                .recordFdCacheStats(
                    fdCache.estimatedSize(),
                    stats.hitCount(),
                    stats.missCount(),
                    stats.hitRate() * 100,
                    stats.evictionCount()
                );
        } catch (IllegalStateException e) {
            // Metrics not initialized yet — skip
        }
    }

    private RefCountedChannel openChannel(String path) {
        try {
            FileChannel fc;
            if (directOpenOption != null) {
                fc = FileChannel.open(Paths.get(path), StandardOpenOption.READ, directOpenOption);
            } else {
                fc = FileChannel.open(Paths.get(path), StandardOpenOption.READ);
            }
            return new RefCountedChannel(fc);
        } catch (IOException e) {
            throw new RuntimeException("Failed to open FileChannel: " + path, e);
        }
    }

    @Override
    public void close() {
        // Synchronously release all base refs to close idle channels immediately
        var iterator = fdCache.asMap().entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            iterator.remove();
            entry.getValue().releaseBase();
        }
        fdCache.cleanUp();
    }
}

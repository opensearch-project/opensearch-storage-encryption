/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

/**
 * Builder for creating block caches with proper lifecycle management.
 */
@SuppressForbidden(reason = "use ThreadPoolExecutor with silent failures, failures may not need to panic threads")
public final class BlockCacheBuilder {

    private static final Logger LOGGER = LogManager.getLogger(BlockCacheBuilder.class);

    private BlockCacheBuilder() {}

    /**
     * Creates a block cache with the specified capacity and removal handling.
     *
     * @param <T> the type of cached block values
     * @param <V> the type returned by the block loader
     * @param initialCapacity initial capacity hint for the cache
     * @param maxBlocks maximum number of blocks to cache
     * @return configured CaffeineBlockCache instance
     */
    public static <T extends AutoCloseable, V> CaffeineBlockCache<T, V> build(int initialCapacity, long maxBlocks) {
        @SuppressWarnings("resource")
        ThreadPoolExecutor removalExec = new ThreadPoolExecutor(4, 8, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), r -> {
            Thread t = new Thread(r, "block-cache-maint");
            t.setDaemon(true);
            return t;
        });

        Cache<BlockCacheKey, BlockCacheValue<T>> cache = Caffeine
            .newBuilder()
            .initialCapacity(initialCapacity)
            .recordStats()
            .maximumSize(maxBlocks)
            .removalListener((BlockCacheKey key, BlockCacheValue<T> value, RemovalCause cause) -> {
                if (value != null) {
                    removalExec.execute(() -> {
                        try {
                            value.close();
                        } catch (Throwable t) {
                            LOGGER.warn("Failed to close cached value during removal {}", key, t);
                        }
                    });
                }
            })
            .build();

        // Loader is null here because this creates a shared cache instance.
        // Per-directory caches will wrap this cache with their own loaders
        // that provide directory-specific decryption keys.
        return new CaffeineBlockCache<>(cache, null, maxBlocks);
    }
}

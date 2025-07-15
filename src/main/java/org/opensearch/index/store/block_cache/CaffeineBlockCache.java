/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import com.github.benmanes.caffeine.cache.Cache;

public final class CaffeineBlockCache<T> implements BlockCache<T> {

    private final Cache<BlockCacheKey, BlockCacheValue<T>> cache;
    private final BlockLoader<T> blockLoader;

    public CaffeineBlockCache(Cache<BlockCacheKey, BlockCacheValue<T>> cache, BlockLoader<T> blockLoader, long maxBlocks) {
        this.blockLoader = blockLoader;
        this.cache = cache;
    }

    @Override
    public BlockCacheValue<T> get(BlockCacheKey key) {
        return cache.getIfPresent(key);
    }

    @Override
    public Optional<BlockCacheValue<T>> getOrLoad(BlockCacheKey key, int size) throws IOException {
        try {
            BlockCacheValue<T> value = cache.get(key, k -> {
                try {
                    return blockLoader.load(k, size).orElse(null); // unwrap Optional
                } catch (Exception e) {
                    return handleLoadException(k, e);
                }
            });
            return Optional.ofNullable(value);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        } catch (RuntimeException e) {
            throw new IOException("Failed to load block for key: " + key, e);
        }
    }

    @Override
    public void prefetch(BlockCacheKey key, int size) {
        cache.asMap().computeIfAbsent(key, k -> {
            try {
                return blockLoader.load(k, size).orElse(null); // unwrap Optional
            } catch (Exception e) {
                return handleLoadException(k, e);
            }
        });
    }

    private BlockCacheValue<T> handleLoadException(BlockCacheKey key, Exception e) {
        switch (e) {
            case IOException io -> throw new UncheckedIOException(io);
            case RuntimeException rte -> throw rte;
            default -> throw new RuntimeException("Unexpected exception during block load for key: " + key, e);
        }
    }

    @Override
    public void put(BlockCacheKey key, BlockCacheValue<T> value) {
        cache.put(key, value);
    }

    @Override
    public void invalidate(BlockCacheKey key) {
        cache.invalidate(key);
    }

    @Override
    public void clear() {
        cache.invalidateAll();
    }
}

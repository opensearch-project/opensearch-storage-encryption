/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE_POWER;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Registry that manages per-file {@link RadixBlockTable} instances and routes
 * L2 eviction notifications to the correct table.
 *
 * <h2>Purpose</h2>
 * Each file gets its own RadixBlockTable for L1 caching. When the shared Caffeine
 * L2 cache evicts a block, the eviction listener needs to find the correct file's
 * RadixBlockTable to call {@code remove(blockId)} on. This registry provides that
 * file-path-to-table mapping.
 *
 * <h2>Reference counting</h2>
 * Multiple IndexInput instances (clones, slices) may share the same RadixBlockTable
 * for a given file. The registry tracks a reference count per entry:
 * <ul>
 *   <li>{@link #acquire(Path)} increments the ref count (or creates a new entry)</li>
 *   <li>{@link #release(Path)} decrements it; when it reaches 0, the entry is removed</li>
 * </ul>
 *
 * <h2>Thread safety</h2>
 * All operations are thread-safe via {@link ConcurrentHashMap#compute}.
 */
public class RadixBlockTableRegistry {

    private static class RegistryEntry {
        final RadixBlockTable<L1CacheEntry> table;
        final AtomicInteger refCount;

        RegistryEntry(RadixBlockTable<L1CacheEntry> table) {
            this.table = table;
            this.refCount = new AtomicInteger(1);
        }
    }

    private final ConcurrentHashMap<Path, RegistryEntry> tables = new ConcurrentHashMap<>();

    /**
     * Acquires a RadixBlockTable for the given file path. If a table already exists
     * for this path, increments the reference count and returns it. Otherwise creates
     * a new table.
     *
     * @param path the normalized absolute file path
     * @return the RadixBlockTable for this file
     */
    public RadixBlockTable<L1CacheEntry> acquire(Path path) {
        Path normalized = path.toAbsolutePath().normalize();
        return tables.compute(normalized, (k, existing) -> {
            if (existing != null) {
                existing.refCount.incrementAndGet();
                return existing;
            }
            return new RegistryEntry(new RadixBlockTable<>());
        }).table;
    }

    /**
     * Releases a reference to the RadixBlockTable for the given file path.
     * When the reference count reaches 0, the table is cleared and removed
     * from the registry.
     *
     * @param path the normalized absolute file path
     */
    public void release(Path path) {
        Path normalized = path.toAbsolutePath().normalize();
        tables.computeIfPresent(normalized, (k, entry) -> {
            if (entry.refCount.decrementAndGet() <= 0) {
                entry.table.clear();
                return null; // remove from map
            }
            return entry;
        });
    }

    /**
     * Called when the L2 Caffeine cache evicts a block. Routes the eviction
     * to the correct file's RadixBlockTable, clearing the L1 entry so that
     * future reads see a clean miss rather than a stale pointer.
     *
     * @param path the file path of the evicted block
     * @param blockOffset the block-aligned byte offset of the evicted block
     */
    public void onEviction(Path path, long blockOffset) {
        RegistryEntry entry = tables.get(path);
        if (entry != null) {
            long blockId = blockOffset >>> CACHE_BLOCK_SIZE_POWER;
            entry.table.remove(blockId);
        }
    }

    /**
     * Clears all entries from the registry. Used during shutdown.
     */
    public void clear() {
        tables.forEach((path, entry) -> entry.table.clear());
        tables.clear();
    }
}

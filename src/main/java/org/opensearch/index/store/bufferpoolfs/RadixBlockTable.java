/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

/**
 * A two-level radix table mapping blockIds to values of type {@code V}.
 * Designed as a per-file L1 lookup cache, but generic enough for any
 * blockId-to-value mapping.
 *
 * <h2>Structure</h2>
 * <pre>
 *   outer index = blockId >>> PAGE_SHIFT   (which group of 1024)
 *   inner slot  = blockId &amp; PAGE_MASK     (position within group)
 * </pre>
 *
 * The outer level defaults to {@value #DEFAULT_OUTER_SLOTS} entries (2 KB),
 * covering up to 2 GB of file data with 8 KB cache blocks. It can grow
 * lazily on demand for larger files. Each inner array is a fixed-size
 * {@code Object[PAGE_SIZE]} (1024 slots / 8 KB), directly indexed by
 * the inner slot — no popcount, no COW. Inner arrays are reclaimed (nulled)
 * when all their slots become empty, keeping memory overhead proportional
 * to actual cached blocks.
 *
 * <h2>Sizing rationale</h2>
 * With 8 KB cache blocks, a 5 GB file has ~655,360 block IDs.
 * {@code PAGE_SHIFT = 10} gives 1024-slot inner arrays, so the outer
 * directory needs only 640 entries for 5 GB (256 entries covers 2 GB).
 * Opening 10,000 idle files (footer-read only) costs:
 * {@code 10,000 x (2 KB outer + 8 KB inner) = ~100 MB} — well within
 * the 600 MB budget. The read path remains two plain array loads with
 * bit-shift indexing.
 *
 * <h2>Thread safety</h2>
 * <ul>
 *   <li>Reads are plain array loads — no fences, no synchronization.
 *       JLS §17.7 guarantees reference writes/reads are atomic, so a reader
 *       either sees the old value or the new value, never a torn reference.</li>
 *   <li>Stale reads are benign: a stale {@code null} simply means an L1 miss,
 *       falling through to the Caffeine L2 cache which is the source of truth.</li>
 *   <li>Writers (search threads on L1/L2 miss, eviction threads nulling slots)
 *       perform plain stores. Concurrent inner-array allocation is guarded by
 *       {@code synchronized(this)} only for the allocation; subsequent slot
 *       writes are plain stores.</li>
 *   <li>Inner-array reclamation on {@link #remove} is guarded by
 *       {@code synchronized(this)} to avoid races with concurrent
 *       {@link #put} that may be allocating the same inner array.</li>
 *   <li>No {@code volatile}, no {@code VarHandle}, no CAS on the read path.</li>
 * </ul>
 *
 * <h2>Expected ownership and lifecycle</h2>
 * <ul>
 *   <li>This table may be shared by multiple parent IndexInput instances that open
 *       the same absolute file path, and by each parent's clones/slices.</li>
 *   <li>Recommended ownership model: keep the table in a higher-level cache keyed by
 *       absolute file path (or equivalent file identity) and manage a thread-safe
 *       shared refcount. Decrement on parent close, and remove the table from the
 *       external cache when the shared refcount reaches 0.</li>
 *   <li>Under that model, {@link #clear()} is optional for correctness; once the table is
 *       unreachable, GC reclaims it. Use {@link #clear()} only if deterministic eager release
 *       is desired while the table object itself remains reachable.</li>
 *   <li>Clones/slices must not outlive the parent lifecycle contract. This class does not
 *       enforce close state; callers must prevent post-close use at a higher layer.</li>
 *   <li>Callers should provide non-negative block IDs in the valid cache-block address space
 *       for the backing file.</li>
 * </ul>
 *
 * <h2>How entries are removed from this L1 table</h2>
 * <ul>
 *   <li><b>Targeted eviction:</b> call {@link #remove(long)} for a specific block ID
 *       when L2/owner evicts that block; this nulls the slot.</li>
 *   <li><b>Inner-array reclamation:</b> after {@link #remove(long)}, if an inner page has
 *       no remaining non-null slots, the page is detached from the outer directory.</li>
 *   <li><b>Whole-table reset:</b> {@link #clear()} swaps to an empty directory, dropping
 *       all L1 entries at once.</li>
 *   <li><b>Lifecycle removal:</b> when the external owner removes the table from its
 *       absolute-path cache at refcount 0, the table and any remaining entries are reclaimed
 *       by GC once unreachable.</li>
 * </ul>
 *
 * <h2>Formal correctness properties</h2>
 * <ol>
 *   <li><b>No torn references:</b> JLS §17.7 — reference read/write is atomic.</li>
 *   <li><b>No use-after-free:</b> Values are owned by the L2 cache; L1 holds
 *       a reference. After eviction, the slot is nulled. A reader that grabbed
 *       the reference before the null-write safely completes its read (the
 *       value is still valid until GC collects it after all references
 *       are released).</li>
 *   <li><b>No memory leak:</b> {@link #clear()} atomically swaps the directory
 *       to a fresh empty one. {@link #remove} reclaims empty inner arrays. Eviction nulls
 *       individual slots.</li>
 *   <li><b>Stale reads are benign:</b> A reader seeing a stale non-null
 *       reference reads valid (possibly evicted) data — harmless for a cache.
 *       A reader seeing stale null gets an L1 miss — correct fallback.</li>
 * </ol>
 *
 * @param <V> the type of values stored in the table
 */
public final class RadixBlockTable<V> {

    /** Each inner array covers 1024 consecutive blockIds. */
    public static final int PAGE_SHIFT = 10;
    public static final int PAGE_SIZE = 1 << PAGE_SHIFT; // 1024
    private static final int PAGE_MASK = PAGE_SIZE - 1;   // 1023

    /**
     * Default outer directory size. 256 x 1024 = 262,144 block IDs.
     * With 8 KB cache blocks this covers 2 GB per file without growth.
     */
    public static final int DEFAULT_OUTER_SLOTS = 256;

    /**
     * Outer directory: {@code directory[outer]} is either null (no inner array)
     * or an {@code Object[PAGE_SIZE]}. Grown on demand under synchronized.
     * Uses Object[][] due to Java generic array creation restrictions.
     */
    private Object[][] directory;

    public RadixBlockTable() {
        this.directory = new Object[DEFAULT_OUTER_SLOTS][];
    }

    public RadixBlockTable(int initialOuterSlots) {
        this.directory = new Object[Math.max(initialOuterSlots, 1)][];
    }

    /**
     * Looks up the value for the given blockId.
     * Lock-free, no fences, no synchronization.
     *
     * @return the cached value, or null if not present (L1 miss)
     */
    @SuppressWarnings("unchecked")
    public V get(long blockId) {
        int outer = (int) (blockId >>> PAGE_SHIFT);
        Object[][] dir = directory; // single read of the reference
        if (outer >= dir.length)
            return null;

        Object[] inner = dir[outer]; // plain array load
        if (inner == null)
            return null;

        int slot = (int) (blockId & PAGE_MASK);
        return (V) inner[slot]; // plain array load — JLS §17.7 atomic reference read
    }

    /**
     * Stores a value at the given blockId.
     * Allocates the inner array lazily if needed (synchronized for allocation only).
     * The slot write itself is a plain store.
     */
    public void put(long blockId, V value) {
        int outer = (int) (blockId >>> PAGE_SHIFT);
        int slot = (int) (blockId & PAGE_MASK);

        Object[][] dir = directory;
        if (outer < dir.length) {
            Object[] inner = dir[outer];
            if (inner != null) {
                // Fast path: inner array exists, plain store
                inner[slot] = value;
                return;
            }
        }

        // Slow path: need to allocate inner array or grow directory
        putSlow(outer, slot, value);
    }

    private synchronized void putSlow(int outer, int slot, V value) {
        if (outer >= directory.length) {
            growDirectory(outer);
        }
        Object[] inner = directory[outer];
        if (inner == null) {
            inner = new Object[PAGE_SIZE];
            directory[outer] = inner;
        }
        inner[slot] = value;
    }

    /**
     * Removes (nulls) the entry at the given blockId.
     * After nulling the slot, scans the inner array. If all slots are null,
     * the inner array is reclaimed (set to null) under synchronization to
     * avoid races with concurrent {@link #put} allocating the same inner array.
     *
     * @return the previous value, or null if slot was empty
     */
    @SuppressWarnings("unchecked")
    public V remove(long blockId) {
        int outer = (int) (blockId >>> PAGE_SHIFT);
        Object[][] dir = directory;
        if (outer >= dir.length)
            return null;

        Object[] inner = dir[outer];
        if (inner == null)
            return null;

        int slot = (int) (blockId & PAGE_MASK);
        V prev = (V) inner[slot];
        inner[slot] = null; // plain store — JLS §17.7 atomic reference write

        // Check if inner array is now empty and reclaim if so
        if (prev != null) {
            reclaimIfEmpty(outer, inner);
        }
        return prev;
    }

    /**
     * Scans the inner array for any non-null slot. If all slots are null,
     * nulls the inner array in the directory under synchronization.
     * The scan is 1024 reference checks — fits in ~16 cache lines.
     */
    private synchronized void reclaimIfEmpty(int outer, Object[] inner) {
        // Re-check: another thread may have put into this inner array
        // between our null-write and acquiring the lock
        if (directory[outer] != inner) {
            return; // inner array was replaced (e.g., by clear + re-put)
        }
        for (int i = 0; i < PAGE_SIZE; i++) {
            if (inner[i] != null) {
                return; // still has entries, don't reclaim
            }
        }
        directory[outer] = null;
    }

    /**
     * Clears all entries.
     * Atomically swaps to a fresh empty directory of the same length.
     * <p>
     * Optional in a refcounted ownership model where the table is removed from the
     * external cache when its refcount reaches 0. In that case, making the table
     * unreachable is sufficient for reclamation, and {@code clear()} is only for
     * eager memory release while the table remains reachable.
     */
    public synchronized void clear() {
        directory = new Object[directory.length][];
    }

    /**
     * Estimates the memory overhead of this table in bytes.
     * Counts the object header, directory array, and all allocated inner arrays.
     * Does not count the values themselves (owned by the L2 cache).
     *
     * @return estimated overhead in bytes
     */
    public long memoryOverheadBytes() {
        Object[][] dir = directory;
        long overhead = 16; // RadixBlockTable object header
        overhead += 16 + 4 + 4 + (long) dir.length * 8; // directory array (header + length + padding + refs)
        for (Object[] inner : dir) {
            if (inner != null) {
                overhead += 16 + 4 + 4 + (long) PAGE_SIZE * 8; // inner array (header + length + padding + refs)
            }
        }
        return overhead;
    }

    /**
     * Returns whether the inner array at the given outer index is allocated.
     * Visible for testing.
     */
    boolean isInnerAllocated(int outer) {
        Object[][] dir = directory;
        return outer < dir.length && dir[outer] != null;
    }

    /**
     * Returns the current outer directory length. Visible for testing.
     */
    int directoryLength() {
        return directory.length;
    }

    /**
     * Counts non-null entries in the inner array at the given outer index.
     * Visible for testing. Returns -1 if inner array is not allocated.
     */
    int countEntries(int outer) {
        Object[][] dir = directory;
        if (outer >= dir.length)
            return -1;
        Object[] inner = dir[outer];
        if (inner == null)
            return -1;
        int count = 0;
        for (Object v : inner) {
            if (v != null)
                count++;
        }
        return count;
    }

    /**
     * Returns the number of allocated inner arrays. Visible for testing
     * and memory overhead measurement.
     */
    int allocatedInnerCount() {
        Object[][] dir = directory;
        int count = 0;
        for (Object[] inner : dir) {
            if (inner != null)
                count++;
        }
        return count;
    }

    // ---- internal ----
    private void growDirectory(int requiredOuter) {
        int newSize = Math.max(requiredOuter + 1, directory.length * 2);
        Object[][] newDir = new Object[newSize][];
        System.arraycopy(directory, 0, newDir, 0, directory.length);
        directory = newDir;
    }
}

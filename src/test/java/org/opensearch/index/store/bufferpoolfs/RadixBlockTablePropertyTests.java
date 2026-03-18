/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Property-based tests for {@link RadixBlockTable}.
 * Uses randomized iteration (100 tries per property) via OpenSearchTestCase
 * random utilities instead of jqwik.
 *
 * Feature: radix-block-table-pr
 */
public class RadixBlockTablePropertyTests extends OpenSearchTestCase {

    private static final int PAGE_SIZE = RadixBlockTable.PAGE_SIZE;

    /** Property 1: Put/get round-trip. */
    public void testPutGetRoundTrip() {
        for (int trial = 0; trial < 100; trial++) {
            int blockId = randomIntBetween(0, 2_621_439);
            RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.putInt(0, blockId);

            assertNull(table.get(blockId));
            table.put(blockId, buf);
            assertSame(buf, table.get(blockId));
        }
    }

    /** Property 2: Remove returns previous and nulls slot. */
    public void testRemoveReturnsPreviousAndNullsSlot() {
        for (int trial = 0; trial < 100; trial++) {
            int blockId = randomIntBetween(0, 2_621_439);
            RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.putInt(0, blockId);

            table.put(blockId, buf);
            ByteBuffer removed = table.remove(blockId);
            assertSame(buf, removed);
            assertNull(table.get(blockId));
        }
    }

    /** Property 3: Clear resets all state. */
    public void testClearResetsAllState() {
        for (int trial = 0; trial < 100; trial++) {
            int count = randomIntBetween(1, 50);
            RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
            long[] blockIds = new long[count];

            for (int i = 0; i < count; i++) {
                blockIds[i] = (long) i * PAGE_SIZE + (i % PAGE_SIZE);
                table.put(blockIds[i], ByteBuffer.allocate(8));
            }

            table.clear();

            for (long id : blockIds) {
                assertNull(table.get(id));
            }
        }
    }

    /** Property 4: Last writer wins (put overwrites). */
    public void testLastWriterWins() {
        for (int trial = 0; trial < 100; trial++) {
            int blockId = randomIntBetween(0, 2_621_439);
            RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
            ByteBuffer a = ByteBuffer.allocate(8);
            ByteBuffer b = ByteBuffer.allocate(16);

            table.put(blockId, a);
            assertSame(a, table.get(blockId));

            table.put(blockId, b);
            assertSame(b, table.get(blockId));
        }
    }

    /** Property 5: Directory growth preserves entries. */
    public void testDirectoryGrowthPreservesEntries() {
        for (int trial = 0; trial < 100; trial++) {
            int numGrowths = randomIntBetween(2, 20);
            RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
            Map<Long, ByteBuffer> entries = new HashMap<>();

            int prevDirLen = table.directoryLength();
            for (int i = 0; i < numGrowths; i++) {
                int outerIndex = prevDirLen + i;
                long blockId = (long) outerIndex * PAGE_SIZE;
                ByteBuffer buf = ByteBuffer.allocate(8);
                buf.putInt(0, i);
                table.put(blockId, buf);
                entries.put(blockId, buf);

                int newDirLen = table.directoryLength();
                assertTrue(newDirLen >= outerIndex + 1);
                prevDirLen = newDirLen;
            }

            for (Map.Entry<Long, ByteBuffer> entry : entries.entrySet()) {
                assertSame(entry.getValue(), table.get(entry.getKey()));
            }
        }
    }

    /** Property 6: Inner allocation accuracy. */
    public void testInnerAllocationAccuracy() {
        for (int trial = 0; trial < 100; trial++) {
            int numPuts = randomIntBetween(1, 80);
            RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
            Set<Integer> allocatedOuters = new HashSet<>();

            for (int i = 0; i < numPuts; i++) {
                int outerIndex = i % 80;
                long blockId = (long) outerIndex * PAGE_SIZE;
                table.put(blockId, ByteBuffer.allocate(8));
                allocatedOuters.add(outerIndex);
            }

            for (int outerIndex : allocatedOuters) {
                assertTrue(table.isInnerAllocated(outerIndex));
            }

            assertEquals(allocatedOuters.size(), table.allocatedInnerCount());
        }
    }

    /** Property 7: countEntries reflects actual state. */
    public void testCountEntriesReflectsActualState() {
        for (int trial = 0; trial < 100; trial++) {
            int outerIndex = randomIntBetween(0, 10);
            int numOps = randomIntBetween(1, 100);
            RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);

            assertEquals(-1, table.countEntries(outerIndex));

            Set<Integer> occupiedSlots = new HashSet<>();

            for (int i = 0; i < numOps; i++) {
                int slot = i % PAGE_SIZE;
                long blockId = (long) outerIndex * PAGE_SIZE + slot;

                if (occupiedSlots.contains(slot)) {
                    table.remove(blockId);
                    occupiedSlots.remove(slot);
                } else {
                    table.put(blockId, ByteBuffer.allocate(8));
                    occupiedSlots.add(slot);
                }
            }

            int expected = occupiedSlots.size();
            int actual = table.countEntries(outerIndex);

            if (expected == 0) {
                // Inner array reclaimed when all slots removed
                assertEquals(-1, actual);
            } else {
                assertEquals(expected, actual);
            }
        }
    }

    /** Property 12: Multi-table isolation. */
    public void testMultiTableIsolation() {
        for (int trial = 0; trial < 100; trial++) {
            int blockId = randomIntBetween(0, 2_621_439);
            RadixBlockTable<ByteBuffer> table1 = new RadixBlockTable<>(1);
            RadixBlockTable<ByteBuffer> table2 = new RadixBlockTable<>(1);

            ByteBuffer buf1 = ByteBuffer.allocate(8);
            buf1.putInt(0, 1);

            table1.put(blockId, buf1);

            assertNull(table2.get(blockId));
            assertSame(buf1, table1.get(blockId));

            ByteBuffer buf2 = ByteBuffer.allocate(16);
            buf2.putInt(0, 2);
            table2.put(blockId, buf2);

            assertSame(buf1, table1.get(blockId));
            assertSame(buf2, table2.get(blockId));
        }
    }
}

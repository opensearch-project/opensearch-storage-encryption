/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import java.nio.ByteBuffer;

import org.opensearch.test.OpenSearchTestCase;

public class RadixBlockTableTests extends OpenSearchTestCase {

    private static final int PS = RadixBlockTable.PAGE_SIZE; // 1024

    private static ByteBuffer buf(int capacity) {
        return ByteBuffer.allocate(capacity);
    }

    // ---- basic put/get/remove ----

    public void testPutGetRoundTrip() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        ByteBuffer b = buf(8);
        table.put(10, b);
        assertSame(b, table.get(10));
    }

    public void testRemoveReturnsPreviousAndNullsSlot() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        ByteBuffer b = buf(8);
        table.put(10, b);
        assertSame(b, table.remove(10));
        assertNull(table.get(10));
    }

    public void testClearResetsAllState() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        table.put(0, buf(8));
        table.put(PS, buf(8));
        table.put(2 * PS, buf(8));
        table.clear();
        assertNull(table.get(0));
        assertNull(table.get(PS));
        assertNull(table.get(2 * PS));
    }

    public void testPutOverwritesExistingEntry() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        ByteBuffer a = buf(8);
        ByteBuffer b = buf(16);
        table.put(5, a);
        assertSame(a, table.get(5));
        table.put(5, b);
        assertSame(b, table.get(5));
    }

    // ---- get edge cases ----

    public void testGetOuterOobReturnsNull() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
        assertNull(table.get(PS));
    }

    public void testGetInnerNullReturnsNull() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        assertNull(table.get(0));
    }

    public void testGetSlotNullReturnsNull() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        table.put(0, buf(8));
        assertNull(table.get(1));
    }

    public void testGetSlotPopulatedReturnsBuffer() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        ByteBuffer b = buf(8);
        table.put(5, b);
        assertSame(b, table.get(5));
    }

    // ---- put paths ----

    public void testPutFastPathInnerExists() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        table.put(0, buf(8));
        ByteBuffer b = buf(16);
        table.put(1, b);
        assertSame(b, table.get(1));
    }

    public void testPutSlowInnerNull() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        assertFalse(table.isInnerAllocated(0));
        ByteBuffer b = buf(8);
        table.put(0, b);
        assertTrue(table.isInnerAllocated(0));
        assertSame(b, table.get(0));
    }

    public void testPutSlowDirectoryGrowth() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
        assertEquals(1, table.directoryLength());
        ByteBuffer b = buf(8);
        table.put(PS, b);
        assertTrue(table.directoryLength() > 1);
        assertSame(b, table.get(PS));
    }

    public void testPutSlowRaceInnerAlreadyAllocated() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        ByteBuffer a = buf(8);
        table.put(0, a);
        assertTrue(table.isInnerAllocated(0));
        ByteBuffer b = buf(16);
        table.put(1, b);
        assertSame(a, table.get(0));
        assertSame(b, table.get(1));
        assertEquals(2, table.countEntries(0));
    }

    // ---- remove edge cases ----

    public void testRemoveOuterOobReturnsNull() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
        assertNull(table.remove(PS));
    }

    public void testRemoveInnerNullReturnsNull() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        assertNull(table.remove(0));
    }

    public void testRemoveSlotNullReturnsNull() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        table.put(0, buf(8));
        assertNull(table.remove(1));
    }

    public void testRemoveSlotPopulatedReturnsPrevious() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        ByteBuffer b = buf(8);
        table.put(5, b);
        assertSame(b, table.remove(5));
        assertNull(table.get(5));
    }

    // ---- clear ----

    public void testClearPopulatedTable() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        table.put(0, buf(8));
        table.put(PS, buf(8));
        table.put(2 * PS, buf(8));
        table.clear();
        assertNull(table.get(0));
        assertNull(table.get(PS));
        assertNull(table.get(2 * PS));
        assertFalse(table.isInnerAllocated(0));
        assertFalse(table.isInnerAllocated(1));
        assertFalse(table.isInnerAllocated(2));
    }

    public void testClearEmptyTable() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        table.clear(); // should not throw
    }

    // ---- directory growth ----

    public void testGrowDirectorySizingInvariants() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(2);
        assertEquals(2, table.directoryLength());
        ByteBuffer existing = buf(8);
        table.put(0, existing);
        table.put(5 * PS, buf(8));
        int newLen = table.directoryLength();
        assertTrue(newLen >= 6);
        assertSame(existing, table.get(0));
    }

    public void testInitialOuterSlots1WithMultipleGrowths() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
        assertEquals(1, table.directoryLength());
        ByteBuffer b1 = buf(8);
        table.put(PS, b1);
        assertTrue(table.directoryLength() >= 2);
        ByteBuffer b2 = buf(8);
        table.put(4 * PS, b2);
        assertTrue(table.directoryLength() >= 5);
        ByteBuffer b3 = buf(8);
        table.put(20 * PS, b3);
        assertTrue(table.directoryLength() >= 21);
        assertSame(b1, table.get(PS));
        assertSame(b2, table.get(4 * PS));
        assertSame(b3, table.get(20 * PS));
    }

    public void testGrowDirectoryDoublesOrFitsRequired() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
        assertEquals(1, table.directoryLength());
        table.put(3 * PS, buf(8));
        int len1 = table.directoryLength();
        assertTrue(len1 >= 4);
        table.put(100 * PS, buf(8));
        int len2 = table.directoryLength();
        assertTrue(len2 >= 101);
        assertNotNull(table.get(3 * PS));
        assertNotNull(table.get(100 * PS));
    }

    public void testGrowDirectoryPreservesAllEntries() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
        ByteBuffer[] bufs = new ByteBuffer[50];
        for (int i = 0; i < 50; i++) {
            bufs[i] = buf(8);
            table.put((long) i * PS, bufs[i]);
        }
        assertTrue(table.directoryLength() >= 50);
        for (int i = 0; i < 50; i++) {
            assertSame(bufs[i], table.get((long) i * PS));
        }
    }

    // ---- edge cases ----

    public void testEdgeCaseBlockId0() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        ByteBuffer b = buf(8);
        table.put(0, b);
        assertSame(b, table.get(0));
        assertSame(b, table.remove(0));
        assertNull(table.get(0));
    }

    public void testEdgeCaseLastSlotInInnerArray() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        ByteBuffer b = buf(8);
        table.put(PS - 1, b);
        assertSame(b, table.get(PS - 1));
        assertEquals(1, table.countEntries(0));
    }

    public void testEdgeCaseFirstSlotInSecondInnerArray() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        ByteBuffer b = buf(8);
        table.put(PS, b);
        assertSame(b, table.get(PS));
        assertTrue(table.isInnerAllocated(1));
        assertFalse(table.isInnerAllocated(0));
    }

    public void testEdgeCaseMaxBlockId() {
        long maxBlockId = 2_621_439L;
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
        ByteBuffer b = buf(8);
        table.put(maxBlockId, b);
        assertSame(b, table.get(maxBlockId));
        assertSame(b, table.remove(maxBlockId));
        assertNull(table.get(maxBlockId));
    }

    public void testFullInnerArrayPopulateThenRemoveAll() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        ByteBuffer[] buffers = new ByteBuffer[PS];
        for (int i = 0; i < PS; i++) {
            buffers[i] = buf(8);
            table.put(i, buffers[i]);
        }
        assertEquals(PS, table.countEntries(0));
        for (int i = 0; i < PS; i++) {
            assertSame(buffers[i], table.remove(i));
        }
        assertFalse(table.isInnerAllocated(0));
    }

    // ---- helper method tests ----

    public void testIsInnerAllocatedCorrectness() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        assertFalse(table.isInnerAllocated(0));
        assertFalse(table.isInnerAllocated(1));
        table.put(0, buf(8));
        assertTrue(table.isInnerAllocated(0));
        assertFalse(table.isInnerAllocated(1));
        table.put(PS, buf(8));
        assertTrue(table.isInnerAllocated(0));
        assertTrue(table.isInnerAllocated(1));
        assertFalse(table.isInnerAllocated(100));
    }

    public void testCountEntriesCorrectness() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        assertEquals(-1, table.countEntries(0));
        table.put(0, buf(8));
        assertEquals(1, table.countEntries(0));
        table.put(1, buf(8));
        assertEquals(2, table.countEntries(0));
        table.remove(0);
        assertEquals(1, table.countEntries(0));
        table.remove(1);
        assertEquals(-1, table.countEntries(0));
        assertEquals(-1, table.countEntries(100));
    }

    public void testDirectoryLengthCorrectness() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        assertEquals(4, table.directoryLength());
        table.put(10 * PS, buf(8));
        assertTrue(table.directoryLength() >= 11);
        int lenBeforeClear = table.directoryLength();
        table.clear();
        assertEquals(lenBeforeClear, table.directoryLength());
    }

    public void testAllocatedInnerCount() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        assertEquals(0, table.allocatedInnerCount());
        table.put(0, buf(8));
        assertEquals(1, table.allocatedInnerCount());
        table.put(PS, buf(8));
        assertEquals(2, table.allocatedInnerCount());
        table.put(2 * PS, buf(8));
        assertEquals(3, table.allocatedInnerCount());
        table.put(1, buf(8));
        assertEquals(3, table.allocatedInnerCount());
    }

    // ---- inner array reclamation tests ----

    public void testRemoveLastSlotReclaimsInnerArray() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        table.put(0, buf(8));
        assertTrue(table.isInnerAllocated(0));
        assertEquals(1, table.allocatedInnerCount());
        table.remove(0);
        assertFalse(table.isInnerAllocated(0));
        assertEquals(0, table.allocatedInnerCount());
    }

    public void testRemoveNonLastSlotDoesNotReclaimInnerArray() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        table.put(0, buf(8));
        table.put(1, buf(8));
        assertEquals(2, table.countEntries(0));
        table.remove(0);
        assertTrue(table.isInnerAllocated(0));
        assertEquals(1, table.countEntries(0));
    }

    public void testRemoveAlreadyNullSlotDoesNotReclaim() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        table.put(0, buf(8));
        table.put(1, buf(8));
        assertNull(table.remove(2));
        assertTrue(table.isInnerAllocated(0));
        assertEquals(2, table.countEntries(0));
    }

    public void testReclaimMultipleInnerArraysIndependently() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        table.put(0, buf(8));
        table.put(PS, buf(8));
        table.put(2 * PS, buf(8));
        assertEquals(3, table.allocatedInnerCount());
        table.remove(PS);
        assertEquals(2, table.allocatedInnerCount());
        assertFalse(table.isInnerAllocated(1));
        assertTrue(table.isInnerAllocated(0));
        assertTrue(table.isInnerAllocated(2));
        table.remove(0);
        assertEquals(1, table.allocatedInnerCount());
        assertFalse(table.isInnerAllocated(0));
    }

    public void testReclaimThenReAllocateInnerArray() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        ByteBuffer a = buf(8);
        table.put(0, a);
        assertTrue(table.isInnerAllocated(0));
        table.remove(0);
        assertFalse(table.isInnerAllocated(0));
        ByteBuffer b = buf(16);
        table.put(0, b);
        assertTrue(table.isInnerAllocated(0));
        assertSame(b, table.get(0));
    }

    public void testReclaimAllSlotsInFullInnerArray() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        for (int i = 0; i < PS; i++) {
            table.put(i, buf(8));
        }
        assertEquals(PS, table.countEntries(0));
        assertTrue(table.isInnerAllocated(0));
        for (int i = 0; i < PS - 1; i++) {
            table.remove(i);
        }
        assertTrue(table.isInnerAllocated(0));
        assertEquals(1, table.countEntries(0));
        table.remove(PS - 1);
        assertFalse(table.isInnerAllocated(0));
        assertEquals(-1, table.countEntries(0));
    }

    public void testClearAfterPartialReclamation() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        table.put(0, buf(8));
        table.put(PS, buf(8));
        table.put(2 * PS, buf(8));
        table.remove(PS);
        assertFalse(table.isInnerAllocated(1));
        table.clear();
        assertFalse(table.isInnerAllocated(0));
        assertFalse(table.isInnerAllocated(1));
        assertFalse(table.isInnerAllocated(2));
        assertEquals(0, table.allocatedInnerCount());
    }

    // ---- memory overhead measurement tests ----

    public void testMemoryOverheadUntouchedTable() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
        assertEquals(1, table.directoryLength());
        assertEquals(0, table.allocatedInnerCount());
    }

    public void testMemoryOverheadProportionalToUsage() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
        for (int i = 0; i < 1000; i++) {
            table.put(i, buf(8));
        }
        int innerCount = table.allocatedInnerCount();
        // ceil(1000/1024) = 1 inner array
        assertEquals(1, innerCount);
        long overheadBytes = (long) innerCount * PS * 8; // inner array ref slots (8 bytes per ref)
        long dataBytes = 1000L * 8; // actual entry references
        double overheadRatio = (double) overheadBytes / dataBytes;
        assertTrue("Overhead ratio " + overheadRatio + " exceeds 5%", overheadRatio < 1.10);
    }

    public void testMemoryOverheadAfterFullEviction() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
        for (int i = 0; i < 1000; i++) {
            table.put(i, buf(8));
        }
        assertEquals(1, table.allocatedInnerCount());
        for (int i = 0; i < 1000; i++) {
            table.remove(i);
        }
        assertEquals(0, table.allocatedInnerCount());
    }

    public void testMemoryOverheadSparseAccess() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
        int sparseCount = 100;
        for (int i = 0; i < sparseCount; i++) {
            table.put((long) i * PS, buf(8));
        }
        assertEquals(sparseCount, table.allocatedInnerCount());
        long overheadBytes = (long) sparseCount * PS * 8;
        long dataBytes = (long) sparseCount * 8;
        double overheadRatio = (double) overheadBytes / dataBytes;
        assertTrue("Sparse overhead ratio " + overheadRatio + " is unreasonable", overheadRatio <= (double) PS);
    }

    public void testMemoryOverheadSparseEvictionReclaims() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
        int sparseCount = 100;
        for (int i = 0; i < sparseCount; i++) {
            table.put((long) i * PS, buf(8));
        }
        assertEquals(sparseCount, table.allocatedInnerCount());
        for (int i = 0; i < sparseCount; i++) {
            table.remove((long) i * PS);
        }
        assertEquals(0, table.allocatedInnerCount());
    }

    public void testDirectoryGrowthPattern() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
        assertEquals(1, table.directoryLength());
        int prevLen = table.directoryLength();
        for (int outer = 0; outer < 200; outer++) {
            table.put((long) outer * PS, buf(8));
            int curLen = table.directoryLength();
            if (curLen != prevLen) {
                assertTrue(
                    "Directory grew from " + prevLen + " to " + curLen + " which is less than required " + (outer + 1),
                    curLen >= outer + 1
                );
                prevLen = curLen;
            }
        }
        assertTrue(table.directoryLength() >= 200);
    }

    // ---- default constructor test ----

    public void testDefaultConstructorUsesDefaultOuterSlots() {
        RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>();
        assertEquals(RadixBlockTable.DEFAULT_OUTER_SLOTS, table.directoryLength());
        assertEquals(0, table.allocatedInnerCount());
    }
}

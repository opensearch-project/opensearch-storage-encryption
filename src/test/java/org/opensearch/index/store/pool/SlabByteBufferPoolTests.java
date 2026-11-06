/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.opensearch.index.store.block.RefCountedByteBuffer;
import org.opensearch.test.OpenSearchTestCase;

public class SlabByteBufferPoolTests extends OpenSearchTestCase {

    private static final int BLOCK_SIZE = 8192;

    public void testAcquireReturnsSlotSizedDirectBuffer() throws Exception {
        SlabByteBufferPool pool = new SlabByteBufferPool(BLOCK_SIZE * 10L, BLOCK_SIZE);
        RefCountedByteBuffer buf = pool.acquire();
        assertTrue(buf.buffer().isDirect());
        assertEquals(BLOCK_SIZE, buf.buffer().capacity());
        assertEquals(1, pool.getBuffersInUse());
        pool.close();
    }

    public void testSlotsComeBackAfterGcAndMemoryStaysBounded() throws Exception {
        SlabByteBufferPool pool = new SlabByteBufferPool(BLOCK_SIZE * 64L, BLOCK_SIZE);
        // 20 rounds of filling the whole pool and dropping every block: each round must reuse the same
        // slots once the GC has collected the previous round's buffers, never allocating past the pool.
        for (int round = 0; round < 20; round++) {
            for (int i = 0; i < 64; i++) {
                pool.acquire().buffer().put(0, (byte) i);
            }
            assertBusy(() -> {
                System.gc();
                assertEquals(0, pool.getBuffersInUse());
            });
        }
        assertEquals(pool.poolStats(), 0, pool.getOverflowAllocations());
        assertTrue(pool.poolStats(), pool.poolStats().contains("slabs=1/1"));
        pool.close();
    }

    public void testHeldBufferIsNeverReused() throws Exception {
        SlabByteBufferPool pool = new SlabByteBufferPool(BLOCK_SIZE * 16L, BLOCK_SIZE);
        RefCountedByteBuffer held = pool.acquire();
        ByteBuffer heldView = held.buffer();
        for (int i = 0; i < BLOCK_SIZE; i++) {
            heldView.put(i, (byte) 0x5A);
        }
        held = null; // only the raw view stays referenced, as in a reader mid-read
        for (int round = 0; round < 50; round++) {
            List<RefCountedByteBuffer> burst = new ArrayList<>();
            for (int i = 0; i < 15; i++) {
                RefCountedByteBuffer b = pool.acquire();
                for (int j = 0; j < BLOCK_SIZE; j++) {
                    b.buffer().put(j, (byte) 0x11);
                }
                burst.add(b);
            }
            burst.clear();
            System.gc();
        }
        for (int i = 0; i < BLOCK_SIZE; i++) {
            assertEquals("byte " + i + " of a still-referenced buffer was overwritten", (byte) 0x5A, heldView.get(i));
        }
        assertTrue(pool.getBuffersInUse() >= 1);
        pool.close();
    }

    public void testReadPathFallsBackToStandaloneBufferWhenFull() throws Exception {
        SlabByteBufferPool pool = new SlabByteBufferPool(BLOCK_SIZE * 2L, BLOCK_SIZE);
        RefCountedByteBuffer a = pool.acquire();
        RefCountedByteBuffer b = pool.acquire();
        long t0 = System.nanoTime();
        RefCountedByteBuffer c = pool.acquire();
        long waitedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        assertNotNull(c);
        assertEquals(BLOCK_SIZE, c.buffer().capacity());
        assertEquals(1, pool.getOverflowAllocations());
        assertTrue("read path waited " + waitedMs + " ms", waitedMs < 2_000);
        assertNotNull(a);
        assertNotNull(b);
        pool.close();
    }

    public void testWritePathFailsAfterTimeoutWhenFull() throws Exception {
        SlabByteBufferPool pool = new SlabByteBufferPool(BLOCK_SIZE * 2L, BLOCK_SIZE);
        RefCountedByteBuffer a = pool.acquire();
        RefCountedByteBuffer b = pool.acquire();
        expectThrows(IOException.class, () -> pool.tryAcquire(20, TimeUnit.MILLISECONDS));
        assertNotNull(a);
        assertNotNull(b);
        pool.close();
    }

    public void testWritePathGetsSlotOnceGcReturnsOne() throws Exception {
        SlabByteBufferPool pool = new SlabByteBufferPool(BLOCK_SIZE * 2L, BLOCK_SIZE);
        RefCountedByteBuffer keep = pool.acquire();
        pool.acquire(); // dropped right away: its slot comes back once collected
        RefCountedByteBuffer got = pool.tryAcquire(10, TimeUnit.SECONDS);
        assertNotNull(got);
        assertEquals(0, pool.getOverflowAllocations());
        assertNotNull(keep);
        pool.close();
    }

    public void testSlabsGrowLazilyUpToTheCap() throws Exception {
        int slotsPerSlab = SlabByteBufferPool.SLAB_BYTES / BLOCK_SIZE;
        SlabByteBufferPool pool = new SlabByteBufferPool((long) BLOCK_SIZE * (slotsPerSlab + 3), BLOCK_SIZE);
        assertTrue(pool.poolStats(), pool.poolStats().contains("slabs=0/2"));
        List<RefCountedByteBuffer> held = new ArrayList<>();
        for (int i = 0; i < slotsPerSlab; i++) {
            held.add(pool.acquire());
        }
        assertTrue(pool.poolStats(), pool.poolStats().contains("slabs=1/2"));
        held.add(pool.acquire());
        assertTrue(pool.poolStats(), pool.poolStats().contains("slabs=2/2"));
        assertEquals(slotsPerSlab + 1, pool.getBuffersInUse());
        pool.close();
    }

    public void testAcquireThrowsWhenClosed() {
        SlabByteBufferPool pool = new SlabByteBufferPool(BLOCK_SIZE * 4L, BLOCK_SIZE);
        pool.close();
        expectThrows(IllegalStateException.class, pool::acquire);
    }
}

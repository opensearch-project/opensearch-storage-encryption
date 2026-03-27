/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import org.opensearch.index.store.block.RefCountedByteBuffer;
import org.opensearch.test.OpenSearchTestCase;

public class ByteBufferPoolTests extends OpenSearchTestCase {

    private static final int BLOCK_SIZE = 8192;

    public void testAcquireReturnsDirectByteBuffer() throws Exception {
        ByteBufferPool pool = new ByteBufferPool(BLOCK_SIZE * 10L, BLOCK_SIZE);
        RefCountedByteBuffer buf = pool.acquire();
        assertNotNull(buf);
        assertNotNull(buf.buffer());
        assertTrue(buf.buffer().isDirect());
        assertEquals(BLOCK_SIZE, buf.buffer().capacity());
        pool.close();
    }

    public void testAcquireTracksBuffersInUse() throws Exception {
        ByteBufferPool pool = new ByteBufferPool(BLOCK_SIZE * 10L, BLOCK_SIZE);
        assertEquals(0, pool.getBuffersInUse());

        RefCountedByteBuffer buf1 = pool.acquire();
        assertEquals(1, pool.getBuffersInUse());

        RefCountedByteBuffer buf2 = pool.acquire();
        assertEquals(2, pool.getBuffersInUse());

        pool.close();
    }

    public void testAcquireOverCapacitySucceeds() throws Exception {
        // Pool with capacity for only 2 buffers
        ByteBufferPool pool = new ByteBufferPool(BLOCK_SIZE * 2L, BLOCK_SIZE);

        RefCountedByteBuffer buf1 = pool.acquire();
        RefCountedByteBuffer buf2 = pool.acquire();
        // Over capacity — acquire should still succeed (read path must not fail)
        RefCountedByteBuffer buf3 = pool.acquire();
        assertNotNull(buf3);
        assertEquals(3, pool.getBuffersInUse());

        pool.close();
    }

    public void testTryAcquireThrowsWhenOverCapacity() throws Exception {
        ByteBufferPool pool = new ByteBufferPool(BLOCK_SIZE * 2L, BLOCK_SIZE);

        pool.acquire();
        pool.acquire();

        expectThrows(java.io.IOException.class, () -> pool.tryAcquire(1, java.util.concurrent.TimeUnit.MILLISECONDS));
        pool.close();
    }

    public void testAcquireThrowsWhenClosed() throws Exception {
        ByteBufferPool pool = new ByteBufferPool(BLOCK_SIZE * 10L, BLOCK_SIZE);
        pool.close();
        assertTrue(pool.isClosed());
        expectThrows(IllegalStateException.class, () -> pool.acquire());
    }

    public void testAvailableMemory() throws Exception {
        ByteBufferPool pool = new ByteBufferPool(BLOCK_SIZE * 4L, BLOCK_SIZE);
        assertEquals(BLOCK_SIZE * 4L, pool.availableMemory());

        pool.acquire();
        assertEquals(BLOCK_SIZE * 3L, pool.availableMemory());

        pool.acquire();
        assertEquals(BLOCK_SIZE * 2L, pool.availableMemory());

        pool.close();
    }

    public void testPoolStats() throws Exception {
        ByteBufferPool pool = new ByteBufferPool(BLOCK_SIZE * 10L, BLOCK_SIZE);
        RefCountedByteBuffer buf1 = pool.acquire();
        RefCountedByteBuffer buf2 = pool.acquire();

        String stats = pool.poolStats();
        assertTrue(stats.contains("inUse=2"));
        assertTrue(stats.contains("max=10"));

        // Keep references alive to prevent GC/Cleaner from decrementing
        assertNotNull(buf1);
        assertNotNull(buf2);
        pool.close();
    }

    public void testIsUnderPressure() throws Exception {
        // Pool with 10 slots, pressure at <10% = <1 slot available
        ByteBufferPool pool = new ByteBufferPool(BLOCK_SIZE * 10L, BLOCK_SIZE);
        assertFalse(pool.isUnderPressure());

        // Acquire 9 — 1 remaining = 10% = not under pressure
        for (int i = 0; i < 9; i++)
            pool.acquire();
        assertFalse(pool.isUnderPressure());

        // Acquire 10th — 0 remaining = under pressure
        pool.acquire();
        assertTrue(pool.isUnderPressure());

        pool.close();
    }

    public void testTotalMemory() {
        ByteBufferPool pool = new ByteBufferPool(BLOCK_SIZE * 10L, BLOCK_SIZE);
        assertEquals(BLOCK_SIZE * 10L, pool.totalMemory());
        assertEquals(BLOCK_SIZE, pool.pooledSegmentSize());
        pool.close();
    }

    public void testConstructorRejectsInvalidSize() {
        expectThrows(IllegalArgumentException.class, () -> new ByteBufferPool(BLOCK_SIZE * 10L + 1, BLOCK_SIZE));
    }
}

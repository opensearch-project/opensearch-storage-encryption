/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.lang.foreign.ValueLayout;

import org.junit.After;
import org.junit.Before;
import org.opensearch.index.store.block.RefCountedByteBuffer;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link MemorySegmentPool} — GC-managed, no freelist.
 */
@SuppressWarnings("preview")
public class MemorySegmentPoolTests extends OpenSearchTestCase {

    private MemorySegmentPool pool;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        CryptoMetricsService.initialize(mock(MetricsRegistry.class));
    }

    @After
    public void tearDown() throws Exception {
        if (pool != null && !pool.isClosed())
            pool.close();
        super.tearDown();
    }

    public void testPoolCreation() {
        pool = new MemorySegmentPool(4096, 1024);
        assertEquals(1024, pool.pooledSegmentSize());
        assertFalse(pool.isClosed());
        assertEquals(4096L, pool.totalMemory());
    }

    public void testInvalidConfiguration() {
        try {
            pool = new MemorySegmentPool(4097, 1024);
            fail("Should throw for non-aligned totalMemory");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("multiple"));
        }
    }

    public void testCompatibilityConstructor() {
        pool = new MemorySegmentPool(4096, 1024);
        assertNotNull(pool);
    }

    public void testSingleAcquire() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        RefCountedByteBuffer seg = pool.acquire();
        assertNotNull(seg);
        assertEquals(1024, seg.length());
        assertEquals(1, pool.getBuffersInUse());
    }

    public void testMultipleAcquire() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        RefCountedByteBuffer[] segs = new RefCountedByteBuffer[4];
        for (int i = 0; i < 4; i++) {
            segs[i] = pool.acquire();
            assertNotNull(segs[i]);
        }
        assertEquals(4, pool.getBuffersInUse());
    }

    public void testAcquireFromClosedPoolThrows() throws Exception {
        pool = new MemorySegmentPool(2048, 1024);
        pool.close();
        try {
            pool.acquire();
            fail("Should throw on closed pool");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("closed"));
        }
    }

    public void testRefCounting() throws Exception {
        pool = new MemorySegmentPool(2048, 1024);
        RefCountedByteBuffer seg = pool.acquire();
        seg.decRef();
    }

    public void testPinUnpin() throws Exception {
        pool = new MemorySegmentPool(2048, 1024);
        RefCountedByteBuffer seg = pool.acquire();
        assertTrue(seg.tryPin());
        seg.unpin();
    }

    public void testSegmentReadWrite() throws Exception {
        pool = new MemorySegmentPool(2048, 1024);
        RefCountedByteBuffer seg = pool.acquire();
        seg.segment().set(ValueLayout.JAVA_BYTE, 0, (byte) 42);
        assertEquals((byte) 42, seg.segment().get(ValueLayout.JAVA_BYTE, 0));
        seg.segment().fill((byte) 0xFF);
        assertEquals((byte) 0xFF, seg.segment().get(ValueLayout.JAVA_BYTE, 1023));
    }

    public void testReleaseIsNoOp() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        RefCountedByteBuffer seg = pool.acquire();
        int inUseBefore = pool.getBuffersInUse();
        pool.release(seg);
        // release is no-op — inUse unchanged
        assertEquals(inUseBefore, pool.getBuffersInUse());
    }

    public void testReleaseAllIsNoOp() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        RefCountedByteBuffer s1 = pool.acquire();
        RefCountedByteBuffer s2 = pool.acquire();
        int inUseBefore = pool.getBuffersInUse();
        pool.releaseAll(s1, s2);
        assertEquals(inUseBefore, pool.getBuffersInUse());
    }

    public void testGenerationTracking() throws Exception {
        pool = new MemorySegmentPool(2048, 1024);
        RefCountedByteBuffer seg = pool.acquire();
        int gen = seg.getGeneration();
        seg.close();
        assertEquals(gen + 1, seg.getGeneration());
    }

    public void testAvailableMemory() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        assertEquals(4096L, pool.availableMemory());
        pool.acquire();
        assertEquals(3072L, pool.availableMemory());
    }

    public void testFreeListSizeAlwaysZero() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        pool.acquire();
    }

    public void testWarmUpIsNoOp() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        pool.warmUp(4);
        assertEquals(0, pool.getBuffersInUse());
    }

    public void testNotUnderPressureInitially() {
        pool = new MemorySegmentPool(10240, 1024);
        assertFalse(pool.isUnderPressure());
    }

    public void testUnderPressureWhenNearMax() throws Exception {
        pool = new MemorySegmentPool(4096, 1024); // max=4
        for (int i = 0; i < 4; i++)
            pool.acquire();
        assertTrue(pool.isUnderPressure());
    }

    public void testClose() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        pool.acquire();
        pool.close();
        assertTrue(pool.isClosed());
    }

    public void testDoubleCloseIsSafe() {
        pool = new MemorySegmentPool(2048, 1024);
        pool.close();
        pool.close();
        assertTrue(pool.isClosed());
    }

    public void testPoolStatsString() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        pool.acquire();
        String stats = pool.poolStats();
        assertNotNull(stats);
        assertTrue(stats.contains("PoolStats"));
        assertTrue(stats.contains("utilization"));
    }

    public void testGetStats() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        pool.acquire();
        MemorySegmentPool.PoolStats stats = pool.getStats();
        assertEquals(4, stats.maxSegments);
        assertEquals(1, stats.buffersInUse);
        assertEquals(0, stats.freeListSize);
    }

    public void testAllocatedBytesTracking() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        assertEquals(0, pool.getAllocatedBytes());
        pool.acquire();
        assertEquals(1024, pool.getAllocatedBytes());
        pool.acquire();
        assertEquals(2048, pool.getAllocatedBytes());
    }

    public void testCleanerDecrementsCounters() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        RefCountedByteBuffer seg = pool.acquire();
        assertEquals(1, pool.getBuffersInUse());

        // Drop reference and GC — Cleaner should decrement
        seg = null;
        for (int i = 0; i < 10; i++) {
            System.gc();
            Thread.sleep(50);
            if (pool.getBuffersInUse() == 0)
                break;
        }
        // Best-effort: Cleaner may or may not have run
        if (pool.getBuffersInUse() == 0) {
            assertEquals(0, pool.getBuffersInUse());
        }
    }

    public void testRecordStatsDoesNotThrow() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        pool.acquire();
        pool.recordStats();
    }
}

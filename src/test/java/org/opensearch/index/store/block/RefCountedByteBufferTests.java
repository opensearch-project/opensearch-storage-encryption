/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.opensearch.test.OpenSearchTestCase;

public class RefCountedByteBufferTests extends OpenSearchTestCase {

    public void testValueReturnsSelf() {
        ByteBuffer buf = ByteBuffer.allocateDirect(8192);
        RefCountedByteBuffer rcb = new RefCountedByteBuffer(buf, 8192);
        assertSame(rcb, rcb.value());
    }

    public void testBufferReturnsUnderlyingBuffer() {
        ByteBuffer buf = ByteBuffer.allocateDirect(8192);
        RefCountedByteBuffer rcb = new RefCountedByteBuffer(buf, 8192);
        assertSame(buf, rcb.buffer());
    }

    public void testLength() {
        ByteBuffer buf = ByteBuffer.allocateDirect(8192);
        RefCountedByteBuffer rcb = new RefCountedByteBuffer(buf, 4096);
        assertEquals(4096, rcb.length());
    }

    public void testTryPinSucceedsWhenOpen() {
        RefCountedByteBuffer rcb = new RefCountedByteBuffer(ByteBuffer.allocateDirect(8192), 8192);
        assertTrue(rcb.tryPin());
    }

    public void testTryPinFailsAfterClose() {
        RefCountedByteBuffer rcb = new RefCountedByteBuffer(ByteBuffer.allocateDirect(8192), 8192);
        rcb.close();
        assertFalse(rcb.tryPin());
    }

    public void testDecRefClosesBuffer() {
        RefCountedByteBuffer rcb = new RefCountedByteBuffer(ByteBuffer.allocateDirect(8192), 8192);
        assertTrue(rcb.tryPin());
        rcb.decRef();
        assertFalse(rcb.tryPin());
    }

    public void testGenerationAlwaysZero() {
        RefCountedByteBuffer rcb = new RefCountedByteBuffer(ByteBuffer.allocateDirect(8192), 8192);
        assertEquals(0, rcb.getGeneration());
        rcb.close();
        assertEquals(0, rcb.getGeneration());
    }

    public void testAbsoluteReadsAreThreadSafe() throws Exception {
        ByteBuffer buf = ByteBuffer.allocateDirect(8192).order(ByteOrder.LITTLE_ENDIAN);
        // Write known data
        for (int i = 0; i < 8192; i += 8) {
            buf.putLong(i, i);
        }

        RefCountedByteBuffer rcb = new RefCountedByteBuffer(buf, 8192);

        // Multiple threads reading absolute positions concurrently
        Thread[] threads = new Thread[8];
        boolean[] results = new boolean[8];
        for (int t = 0; t < threads.length; t++) {
            final int threadIdx = t;
            threads[t] = new Thread(() -> {
                boolean ok = true;
                for (int i = 0; i < 8192; i += 8) {
                    if (rcb.buffer().getLong(i) != i) {
                        ok = false;
                        break;
                    }
                }
                results[threadIdx] = ok;
            });
            threads[t].start();
        }
        for (Thread thread : threads)
            thread.join();
        for (boolean result : results)
            assertTrue(result);
    }

    public void testCloseIsIdempotent() {
        RefCountedByteBuffer rcb = new RefCountedByteBuffer(ByteBuffer.allocateDirect(8192), 8192);
        rcb.close();
        rcb.close(); // should not throw
        assertFalse(rcb.tryPin());
    }
}

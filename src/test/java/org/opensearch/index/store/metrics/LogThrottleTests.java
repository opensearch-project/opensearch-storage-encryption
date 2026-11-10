/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.metrics;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.opensearch.test.OpenSearchTestCase;

public class LogThrottleTests extends OpenSearchTestCase {

    public void testFirstCallLogsThenSuppressesWithinInterval() {
        AtomicLong now = new AtomicLong(randomLong());
        LogThrottle throttle = new LogThrottle(1, TimeUnit.MINUTES, now::get);

        assertEquals(0, throttle.tryAcquire());
        assertEquals(-1, throttle.tryAcquire());
        now.addAndGet(TimeUnit.SECONDS.toNanos(59));
        assertEquals(-1, throttle.tryAcquire());
    }

    public void testReportsSuppressedCountOnceIntervalPasses() {
        AtomicLong now = new AtomicLong(randomLong());
        LogThrottle throttle = new LogThrottle(1, TimeUnit.MINUTES, now::get);

        assertEquals(0, throttle.tryAcquire());
        for (int i = 0; i < 5; i++) {
            assertEquals(-1, throttle.tryAcquire());
        }
        now.addAndGet(TimeUnit.MINUTES.toNanos(1));
        assertEquals(5, throttle.tryAcquire());
        assertEquals(-1, throttle.tryAcquire());
        now.addAndGet(TimeUnit.MINUTES.toNanos(2));
        assertEquals(1, throttle.tryAcquire());
    }

    public void testOnlyOneConcurrentCallerGetsThrough() throws Exception {
        LogThrottle throttle = new LogThrottle(1, TimeUnit.HOURS);
        AtomicLong passed = new AtomicLong();
        Thread[] threads = new Thread[8];
        for (int t = 0; t < threads.length; t++) {
            threads[t] = new Thread(() -> {
                for (int i = 0; i < 1000; i++) {
                    if (throttle.tryAcquire() >= 0) {
                        passed.incrementAndGet();
                    }
                }
            });
            threads[t].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        assertEquals(1, passed.get());
    }
}

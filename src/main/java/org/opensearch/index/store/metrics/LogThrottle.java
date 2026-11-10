/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.metrics;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Lets a log line through at most once per interval, for messages that can fire once per block or per
 * request while the node is under pressure. Calls in between are counted, so the next line that gets
 * through can say how many were suppressed.
 *
 * <pre>{@code
 * long suppressed = THROTTLE.tryAcquire();
 * if (suppressed >= 0) {
 *     LOGGER.warn("Pool exhausted ({} similar messages suppressed)", suppressed);
 * }
 * }</pre>
 */
public final class LogThrottle {

    private final long intervalNanos;
    private final LongSupplier nanoClock;
    private final AtomicLong nextAllowedNanos;
    private final AtomicLong suppressed = new AtomicLong();

    /**
     * @param interval minimum time between two lines that get through
     * @param unit unit of {@code interval}
     */
    public LogThrottle(long interval, TimeUnit unit) {
        this(interval, unit, System::nanoTime);
    }

    LogThrottle(long interval, TimeUnit unit, LongSupplier nanoClock) {
        this.intervalNanos = unit.toNanos(interval);
        this.nanoClock = nanoClock;
        // The first call always gets through
        this.nextAllowedNanos = new AtomicLong(nanoClock.getAsLong());
    }

    /**
     * Returns the number of calls suppressed since the last one that got through, or -1 if this call
     * should not log.
     */
    public long tryAcquire() {
        long now = nanoClock.getAsLong();
        long next = nextAllowedNanos.get();
        if (now - next >= 0 && nextAllowedNanos.compareAndSet(next, now + intervalNanos)) {
            return suppressed.getAndSet(0);
        }
        suppressed.incrementAndGet();
        return -1;
    }
}

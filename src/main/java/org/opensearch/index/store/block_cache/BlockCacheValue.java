/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

/**
 * A cache value that wraps a resource (e.g., a native memory block) with
 * reference-counting and retirement semantics.
 *
 * <h2>Lifecycle Contract</h2>
 * <ul>
 *   <li><b>Pin before use:</b> Call {@link #tryPin()} before accessing {@link #value()}.
 *       If it returns {@code true}, you <em>must</em> eventually call {@link #unpin()}
 *       (typically in a {@code finally} block).</li>
 *   <li><b>Retirement:</b> When the cache removes this entry, it will call {@link #close()} exactly once
 *       and then drop the cache’s reference. The underlying resource is freed only when the reference
 *       count reaches zero (i.e., after the last {@link #unpin()}).</li>
 *   <li><b>No unpinned access:</b> Callers must only read from {@link #value()} while holding a pin
 *       acquired via {@link #tryPin()}. If {@link #tryPin()} returns {@code false} or after
 *       {@link #close()} has been called, the value must not be accessed.</li>
 * </ul>
 *
 * <h2>Thread-safety</h2>
 * Implementations must be safe for concurrent {@link #tryPin()}, {@link #unpin()}, and a single
 * {@link #close()} call. Access to {@link #value()} must be safe for concurrent readers that each
 * hold a pin. Implementations should ensure {@link #tryPin()} fails (returns {@code false})
 * once the value has been retired.
 *
 * @param <T> The wrapped resource type (e.g., a read-only view like RefCountedMemorySegment)
 */
public interface BlockCacheValue<T> extends AutoCloseable {

    /**
     * Attempts to increment the pin/reference count for this value.
     *
     * @return {@code true} if the caller acquired a pin and may access {@link #value()},
     *         {@code false} if the value is retired or otherwise unavailable
     */
    boolean tryPin();

    /**
     * Releases a previously acquired pin.
     * <p>
     * Every successful {@link #tryPin()} call must be paired with exactly one {@code unpin()}.
     * When the last pin is released (including the cache’s own hold after {@link #close()}),
     * the underlying resource should be freed.
     */
    void unpin();

    /**
     * Returns the wrapped resource for read-only use while pinned.
     * <p>
     * Callers must only use the returned value while they hold a pin acquired via {@link #tryPin()}.
     */
    T value();

    /**
     * @return logical size of this value in bytes (e.g., block length). This is expected to be stable
     *         for the lifetime of the cache entry.
     */
    int length();

    /**
     * Retires this value from the cache and drops the cache's reference.
     * <p>
     * <b>Called exactly once</b> by the cache (e.g., via a removal listener). Implementations must:
     * <ol>
     *   <li>Mark the value as retired so that subsequent {@link #tryPin()} attempts fail.</li>
     *   <li>Drop the cache's reference (the underlying resource is freed only when the total
     *       reference count reaches zero).</li>
     * </ol>
     * Implementations should not throw checked exceptions.
     */
    @Override
    void close();

    /**
     * Drops a reference to this value without retirement semantics.
     * <p>
     * Use this when releasing ownership of a value that was never inserted into the cache
     * (e.g., duplicate loads in bulk operations). Unlike {@link #close()}, this does not
     * increment the generation counter or mark the value as retired.
     * <p>
     * When the reference count reaches zero, the underlying resource is freed/returned to pool.
     */
    void decRef();
}

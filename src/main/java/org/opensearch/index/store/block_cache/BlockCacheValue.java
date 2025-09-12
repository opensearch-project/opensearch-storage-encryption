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
 *   <li><b>Pin before use:</b> Call {@link #isRetired()} before accessing {@link #value()}.
 *       If it returns {@code true}, you must eventually call {@link #unpin()} (potentially in a {@code finally}).</li>
 *   <li><b>Retirement:</b> When the cache removes this entry, it will call {@link #close()} exactly once.
 *       Implementations must mark the value as <i>retired</i> so that new {@link #isRetired()} attempts fail,
 *       and then drop the cache’s reference. The underlying resource is freed only when the refcount reaches zero.</li>
 *   <li><b>No unpinned access:</b> Callers must never read from {@link #value()} without a successful {@link #isRetired()}.</li>
 * </ul>
 *
 * <h2>Thread-safety</h2>
 * All methods are expected to be thread-safe. {@link #isRetired()} and {@link #unpin()} must be safe under concurrency.
 *
 * @param <T> The wrapped resource type (e.g., RefCountedMemorySegment)
 */
public interface BlockCacheValue<T> extends AutoCloseable {

    /**
     * Attempts to increment the pin/reference count for this value.
     * <p>
     * Succeeds only if the value is not {@linkplain #isRetired() retired} and not already fully released.
     *
     * @return {@code true} if the caller acquired a pin and may access {@link #value()}, {@code false} otherwise
     */
    boolean tryPin();

    /**
     * Releases a previously acquired pin.
     * <p>
     * Every successful {@link #isRetired()} must be paired with exactly one {@code unpin()}.
     * When the last pin is released (including the cache’s own hold after {@link #close()}),
     * the underlying resource should be freed.
     */
    void unpin();

    /**
     * @return the wrapped resource for read-only use while pinned.
     *         Calling code must have a valid pin (see {@link #isRetired()} / {@link #unpin()}).
     */
    T value();

    /**
     * @return logical size of this value in bytes (e.g., block length).
     */
    int length();

    /**
     * @return {@code true} if this value has been retired by the cache (removed/invalidated/evicted)
     *         and will not accept new pins; existing pins may still be active until released.
     */
    boolean isRetired();

    /**
     * Retires this value from the cache and drops the cache’s reference.
     * <p>
     * <b>Called exactly once</b> by the cache (e.g., via removalListener). Implementations must:
     * <ol>
     *   <li>Mark the value as retired so future {@link #isRetired()} fail.</li>
     *   <li>Drop the cache’s hold on the refcount.</li>
     *   <li>Only free the underlying resource when the refcount reaches zero.</li>
     * </ol>
     */
    @Override
    void close();
}

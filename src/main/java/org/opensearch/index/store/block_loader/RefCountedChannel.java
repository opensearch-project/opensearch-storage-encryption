/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_loader;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A reference-counted wrapper around a {@link FileChannel} that guarantees
 * the underlying channel stays open for the duration of any in-flight I/O.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>Created with refCount=1 (the cache's "base" reference).</li>
 *   <li>Each in-flight I/O calls {@link #acquire()} which atomically increments
 *       the refCount and returns the FileChannel. Acquire never returns null —
 *       if the channel is already dead (refCount ≤ 0), it throws.</li>
 *   <li>When I/O completes, {@link #close()} (AutoCloseable) decrements the refCount.</li>
 *   <li>When the cache evicts this entry, it calls {@link #releaseBase()} to drop
 *       the base ref. If no I/O is in flight, the FileChannel closes immediately.
 *       Otherwise it closes when the last I/O calls {@link #close()}.</li>
 * </ol>
 *
 * <p>Usage:
 * <pre>{@code
 *   try (RefCountedChannel ref = fileChannelCache.acquire(path)) {
 *       FileChannel fc = ref.channel();
 *       fc.read(buf, offset);
 *   }
 * }</pre>
 */
public class RefCountedChannel implements AutoCloseable {

    private final FileChannel channel;
    private final AtomicInteger refCount;

    public RefCountedChannel(FileChannel channel) {
        this.channel = channel;
        this.refCount = new AtomicInteger(1);
    }

    /**
     * Increment refCount for an in-flight I/O operation and return this instance.
     * The caller MUST call {@link #close()} when done (typically via try-with-resources).
     *
     * @return this RefCountedChannel (never null)
     * @throws IllegalStateException if the channel is already dead (refCount ≤ 0)
     */
    public RefCountedChannel acquire() {
        while (true) {
            int current = refCount.get();
            if (current <= 0) {
                throw new IllegalStateException("RefCountedChannel already closed");
            }
            if (refCount.compareAndSet(current, current + 1)) {
                return this;
            }
        }
    }

    /**
     * Returns the underlying FileChannel. Guaranteed open while this ref is acquired.
     */
    public FileChannel channel() {
        return channel;
    }

    /**
     * Check if this channel is still alive (refCount > 0).
     */
    boolean isAlive() {
        return refCount.get() > 0;
    }

    /**
     * Release one I/O reference. Called automatically via try-with-resources.
     * Closes the FileChannel when the last reference (including the base) is released.
     */
    @Override
    public void close() {
        while (true) {
            int current = refCount.get();
            if (current <= 0) {
                return; // Already fully released — guard against double-release
            }
            if (refCount.compareAndSet(current, current - 1)) {
                if (current == 1) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                        // Best-effort close
                    }
                }
                return;
            }
        }
    }

    /**
     * Release the cache's base reference. Called by the cache eviction listener.
     * Same as {@link #close()} but named for clarity at the call site.
     */
    void releaseBase() {
        close();
    }
}

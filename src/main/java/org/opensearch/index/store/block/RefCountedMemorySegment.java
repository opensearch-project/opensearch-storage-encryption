/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.metrics.ErrorType;

/**
 * A reference-counted wrapper around a {@link MemorySegment} that implements {@link BlockCacheValue}.
 *
 * Packed-state design:
 *  - We store (generation, refCount) in a single volatile long, updated atomically via CAS.
 *  - This avoids any TOCTOU kinda race between reading generation and pinning:
 *      * tryPin() observes BOTH generation and refCount in one snapshot
 *      * CAS increments refCount only if generation is unchanged
 *  - close() atomically:
 *      * bumps generation
 *      * drops cache's ref (refCount--)
 *    in the same CAS, so readers never see a mix of old/new state.
 *
 * Layout (64-bit):
 *   [ generation: 32 bits ][ refCount: 32 bits ]
 *
 * Notes:
 *  - generation is treated as unsigned 32-bit (wraparound is fine in practice)
 *  - refCount must stay > 0 for pin to succeed; 0 means retired/released
 */
@SuppressWarnings("preview")
public final class RefCountedMemorySegment implements BlockCacheValue<RefCountedMemorySegment> {

    private static final Logger LOGGER = LogManager.getLogger(RefCountedMemorySegment.class);

    private final MemorySegment slicedSegment;
    private final int length;

    /** Called when refCount reaches 0. Returns segment to pool. */
    private final BlockReleaser<RefCountedMemorySegment> onFullyReleased;

    /**
     * Packed state: upper 32 bits generation, lower 32 bits refCount.
     *
     * Initial value: gen=0, refCount=1 (cache ownership).
     */
    @SuppressWarnings("unused") // accessed via VarHandle
    private volatile long state = packState(0, 1);

    private static final VarHandle STATE;
    static {
        try {
            STATE = MethodHandles.lookup().findVarHandle(RefCountedMemorySegment.class, "state", long.class);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new Error(e);
        }
    }

    public RefCountedMemorySegment(MemorySegment segment, int length, BlockReleaser<RefCountedMemorySegment> onFullyReleased) {
        if (segment == null || onFullyReleased == null) {
            throw new IllegalArgumentException("segment and onFullyReleased must not be null");
        }
        this.length = length;
        this.onFullyReleased = onFullyReleased;
        this.slicedSegment = (length < segment.byteSize()) ? segment.asSlice(0, length) : segment;
    }

    @Override
    public RefCountedMemorySegment value() {
        return this;
    }

    @Override
    public int length() {
        return length;
    }

    public MemorySegment segment() {
        return slicedSegment;
    }

    @Override
    public int getGeneration() {
        // Single volatile read of state; no separate generation field.
        long s = (long) STATE.getVolatile(this);
        return unpackGeneration(s);
    }

    public int getRefCount() {
        long s = (long) STATE.getVolatile(this);
        return unpackRefCount(s);
    }

    /**
     * Atomically increments refCount iff refCount > 0.
     *
     * Because generation is in the same word, callers can also do “pin-then-validate”
     * by snapshotting the packed state externally if needed. But most importantly:
     * close() cannot interleave generation bump with refcount drop in a way that
     * would allow pinning a recycled generation.
     */
    @Override
    public boolean tryPin() {
        long s = (long) STATE.getVolatile(this);

        for (;;) {
            final int rc = unpackRefCount(s);
            if (rc <= 0) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("tryPin failed: refCount=0");
                }
                return false;
            }

            // Increment refcount, here we keep generation unchanged.
            final long ns = packState(unpackGeneration(s), rc + 1);

            if (STATE.compareAndSet(this, s, ns)) {
                return true;
            }

            s = (long) STATE.getVolatile(this);
            Thread.onSpinWait();
        }
    }

    /**
     * Drops one ref. When it reaches 0, releases to pool.
     */
    @Override
    public void unpin() {
        decRef();
    }

    @Override
    public void decRef() {
        long s = (long) STATE.getVolatile(this);

        for (;;) {
            final int gen = unpackGeneration(s);
            final int rc = unpackRefCount(s);
            final int nrc = rc - 1;

            if (rc <= 0) {
                recordErrorMetric();
                throw new IllegalStateException("decRef underflow (refCount=" + nrc + ')');
            }

            final long ns = packState(gen, nrc);

            if (STATE.compareAndSet(this, s, ns)) {
                if (nrc == 0) {
                    onFullyReleased.release(this);
                }
                return;
            }

            s = (long) STATE.getVolatile(this);
            Thread.onSpinWait();
        }
    }

    /**
     * Internal-only: increments refcount for tests.
     * With packed-state, this still cannot revive if rc==0 (we keep that invariant).
     */
    public void incRef() {
        long s = (long) STATE.getVolatile(this);

        for (;;) {
            final int gen = unpackGeneration(s);
            final int rc = unpackRefCount(s);
            if (rc <= 0) {
                recordErrorMetric();
                throw new IllegalStateException("Attempted to revive a released segment (refCount=" + rc + ")");
            }

            final long ns = packState(gen, rc + 1);
            if (STATE.compareAndSet(this, s, ns)) {
                return;
            }

            s = (long) STATE.getVolatile(this);
            Thread.onSpinWait();
        }
    }

    /**
     * Resets this segment to a fresh state when reused from pool.
     * Must be called under pool lock, before publishing the segment.
     *
     * IMPORTANT: This resets refCount to 1 but does NOT touch generation.
     * Generation bump happens on close() (eviction), not on reset().
     */
    public void reset() {
        // Under pool lock; plain set is fine.
        final int gen = unpackGeneration((long) STATE.getVolatile(this));
        state = packState(gen, 1);
    }

    /**
     * Atomically:
     *  - bump generation
     *  - drop cache's reference (refCount--)
     *
     * Single CAS ensures readers never observe “new generation but old refCount”
     * or vice versa.
     *
     * Contract: called exactly once by removal listener.
     */
    @Override
    public void close() {
        long s = (long) STATE.getVolatile(this);

        for (;;) {
            final int gen = unpackGeneration(s);
            final int rc = unpackRefCount(s);

            // cache should always hold a ref while entry is alive
            if (rc <= 0) {
                recordErrorMetric();
                throw new IllegalStateException("close on already released segment (refCount=" + rc + ")");
            }

            final int ngen = gen + 1;   // bump epoch
            final int nrc = rc - 1;     // drop cache ref
            final long ns = packState(ngen, nrc);

            if (STATE.compareAndSet(this, s, ns)) {
                if (nrc == 0) {
                    onFullyReleased.release(this);
                }
                return;
            }

            s = (long) STATE.getVolatile(this);
            Thread.onSpinWait();
        }
    }

    private static long packState(int generation, int refCount) {
        // store both as unsigned 32-bit lanes
        return ((generation & 0xFFFF_FFFFL) << 32) | (refCount & 0xFFFF_FFFFL);
    }

    private static int unpackGeneration(long state) {
        return (int) (state >>> 32);
    }

    private static int unpackRefCount(long state) {
        return (int) state;
    }

    private static void recordErrorMetric() {
        try {
            CryptoMetricsService.getInstance().recordError(ErrorType.INTERNAL_ERROR);
        } catch (Throwable t) {
            // ignore metric exception
        }
    }
}

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
 * SAFE for concurrent access.
 *
 * WHY this exists -- 
 * We allocate chunks of native memory (off-heap) to cache decrypted file blocks.
 * Multiple threads can read the same block simultaneously. We need to know when nobody
 * is using a block anymore so we can safely return it to the pool for reuse.
 *
 * Think of it like a library book: refCount tracks how many people have it checked out.
 * When the count hits zero, the book goes back on the shelf (pool).
 *
 *
 * Packed-state design:
 *  - We store (generation, refCount) in a single volatile long, updated atomically via CAS.
 *  - This avoids TOCTOU between reading generation and pinning:
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
 * Important Notes:
 *  - generation is treated as unsigned 32-bit (wraparound is fine in practice)
 *  - refCount must stay > 0 for pin to succeed; 0 means retired/released
 *
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
        // Single volatile read of packed state.
        final long s = (long) STATE.getVolatile(this);
        return unpackGeneration(s);
    }

    public int getRefCount() {
        final long s = (long) STATE.getVolatile(this);
        return unpackRefCount(s);
    }

    /**
     * Atomically increments refCount iff refCount > 0.
     *
     * Hot-path is straight-line:
     *   - read packed state
     *   - check rc > 0
     *   - CAS state+1 (increment low 32 bits only)
     */
    @Override
    public boolean tryPin() {
        long s = (long) STATE.getVolatile(this);

        for (;;) {
            final int rc = unpackRefCount(s);
            if (rc <= 0) {
                return tryPinFailed();
            }

            // Increment refcount (lower 32 bits) only; generation (upper 32) unchanged.
            final long ns = s + 1L;

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
            final int rc = unpackRefCount(s);
            if (rc <= 0) {
                recordErrorMetric();
                throw new IllegalStateException("decRef underflow (refCount=" + (rc - 1) + ')');
            }

            // Decrement refcount (lower 32 bits) only; generation unchanged.
            final long ns = s - 1L;

            if (STATE.compareAndSet(this, s, ns)) {
                if (rc == 1) { // transitioned to 0
                    onFullyReleased.release(this);
                }
                return;
            }

            s = (long) STATE.getVolatile(this);
            Thread.onSpinWait();
        }
    }

    /**
     * Internal-only-for tests.
     */
    public void incRef() {
        long s = (long) STATE.getVolatile(this);

        for (;;) {
            final int rc = unpackRefCount(s);
            if (rc <= 0) {
                recordErrorMetric();
                throw new IllegalStateException("Attempted to revive a released segment (refCount=" + rc + ")");
            }

            final long ns = s + 1L;
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
     * IMPORTANT: resets refCount to 1 but does NOT touch generation.
     * Generation bump happens on close() (eviction), not on reset().
     */
    public void reset() {
        // Under pool lock; plain set is fine.
        final long s = (long) STATE.getVolatile(this);
        final int gen = unpackGeneration(s);
        state = packState(gen, 1);
    }

    /**
     * Atomically:
     *  - bump generation
     *  - drop cache's reference (refCount--)
     *
     * Done in a single CAS:
     *   ns = s + (1<<32) - 1
     */
    @Override
    public void close() {
        long s = (long) STATE.getVolatile(this);

        for (;;) {
            final int rc = unpackRefCount(s);

            // cache should always hold a ref while entry is alive
            if (rc <= 0) {
                recordErrorMetric();
                throw new IllegalStateException("close on already released segment (refCount=" + rc + ")");
            }

            // bump generation (upper 32) and drop one ref (lower 32)
            final long ns = s + (1L << 32) - 1L;

            if (STATE.compareAndSet(this, s, ns)) {
                if (rc == 1) { // transitioned to 0
                    onFullyReleased.release(this);
                }
                return;
            }

            s = (long) STATE.getVolatile(this);
            Thread.onSpinWait();
        }
    }

    /**
     * Optional helper: atomically pins only if the generation matches expected.
     * This reduces "pin then validate then unpin" churn in callers.
     */
    public boolean tryPinIfGeneration(int expectedGen) {
        long s = (long) STATE.getVolatile(this);

        for (;;) {
            final int rc = unpackRefCount(s);
            if (rc <= 0) {
                return false;
            }
            if (unpackGeneration(s) != expectedGen) {
                return false;
            }

            final long ns = s + 1L;
            if (STATE.compareAndSet(this, s, ns)) {
                return true;
            }

            s = (long) STATE.getVolatile(this);
            Thread.onSpinWait();
        }
    }

    /** Cold path: keep debug checks out of the hot loop. */
    private boolean tryPinFailed() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("tryPin failed: refCount=0");
        }
        return false;
    }

    /**
     * Pack generation and refCount into a single 64-bit long.
     *
     * Layout: [generation:32][refCount:32]
     *
     * Math breakdown:
     *   generation & 0xFFFF_FFFFL   - mask to 32 bits (treat as unsigned)
     *   << 32                        - shift left 32 bits (moves to upper half)
     *   refCount & 0xFFFF_FFFFL      - mask to 32 bits (treat as unsigned)
     *
     * Example: gen=5, refCount=3
     *   gen & 0xFFFF_FFFFL      = 0x0000_0005
     *   << 32                   = 0x0000_0005_0000_0000
     *   refCount & 0xFFFF_FFFFL = 0x0000_0003
     *   result                  = 0x0000_0005_0000_0003
     */
    private static long packState(int generation, int refCount) {
        return ((generation & 0xFFFF_FFFFL) << 32) | (refCount & 0xFFFF_FFFFL);
    }

    /**
     * Extract generation from packed state (upper 32 bits).
     *
     * Math: unsigned right shift by 32 bits drops the lower 32 bits (refCount),
     * leaving only generation. Cast to int to get the 32-bit value.
     *
     * Example: state = 0x0000_0005_0000_0003
     *   >>> 32  shifts: 0x0000_0000_0000_0005
     *   (int) cast:     5
     */
    private static int unpackGeneration(long state) {
        return (int) (state >>> 32);
    }

    /**
     * Extract refCount from packed state (lower 32 bits).
     *
     * Math: casting long to int simply drops the upper 32 bits. Its safe.
     * Java truncates to the lower 32 bits automatically.
     *
     * Example: state = 0x0000_0005_0000_0003
     *   (int) cast: 3
     */
    private static int unpackRefCount(long state) {
        // JLS 5.1.3: narrowing long->int keeps low 32 bits (mod 2^32)
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

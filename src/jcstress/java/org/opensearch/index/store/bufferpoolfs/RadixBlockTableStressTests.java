/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import java.nio.ByteBuffer;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.*;

/**
 * JCStress tests for {@link RadixBlockTable} using {@code ByteBuffer}
 * values — the actual type used in production (cache block buffers).
 *
 * <h2>What these tests demonstrate</h2>
 * <p>RadixBlockTable uses plain loads on the read path (no volatile, no
 * fences) for maximum throughput. The design explicitly accepts that a
 * reader may see stale {@code null} (an L1 miss) when another thread has
 * just written a value. These tests use JCStress to systematically explore
 * all possible interleavings and memory orderings to confirm:</p>
 * <ol>
 *   <li>Stale reads are the <em>only</em> anomaly — no torn references,
 *       no out-of-thin-air values, no crashes.</li>
 *   <li>The anomaly is bounded: the reader sees either the correct value
 *       or {@code null}, never a wrong non-null value.</li>
 *   <li>When a non-null ByteBuffer is observed, its content is intact —
 *       no partial writes, no corrupted bytes.</li>
 * </ol>
 *
 * <h2>Content validation</h2>
 * <p>Each ByteBuffer is filled with a deterministic magic byte derived from
 * its identity (e.g. 0xAA, 0xBB). Readers verify every byte matches the
 * expected pattern. A mismatch would indicate a torn reference (reader got
 * a reference to the wrong buffer) or memory corruption — both FORBIDDEN.</p>
 *
 * <h2>Outcome interpretation</h2>
 * <ul>
 *   <li>{@code ACCEPTABLE} — expected under sequential consistency</li>
 *   <li>{@code ACCEPTABLE_INTERESTING} — stale read (null when value was
 *       just written). Legal per JMM, demonstrates the relaxed-memory
 *       trade-off. On x86/TSO this is rare; on ARM/RISC-V more common.</li>
 *   <li>{@code FORBIDDEN} — would indicate a real bug (torn reference,
 *       wrong value, corrupted content)</li>
 * </ul>
 */
public class RadixBlockTableStressTests {

    /** Size of each test ByteBuffer — small but enough to catch corruption. */
    private static final int BUF_SIZE = 64;

    /** Creates a ByteBuffer filled entirely with the given magic byte. */
    private static ByteBuffer makeBuf(byte magic) {
        ByteBuffer buf = ByteBuffer.allocate(BUF_SIZE);
        for (int i = 0; i < BUF_SIZE; i++) {
            buf.put(i, magic);
        }
        return buf;
    }

    /**
     * Validates that every byte in the buffer matches the expected magic.
     * Returns true if content is intact, false if any byte is corrupted.
     */
    private static boolean validateContent(ByteBuffer buf, byte expectedMagic) {
        for (int i = 0; i < BUF_SIZE; i++) {
            if (buf.get(i) != expectedMagic) {
                return false;
            }
        }
        return true;
    }

    // ========================================================================
    // Test 1: Put-then-Get visibility (same inner array, slot 0)
    //
    // Actor 1: put(0, sentinel)
    // Actor 2: get(0) and validate content
    //
    // The inner array for outer=0 is pre-allocated, so this isolates the
    // plain store → plain load visibility of a single slot write.
    // ========================================================================

    @JCStressTest
    @Description("Put/Get visibility on a pre-allocated inner array slot. "
        + "Reader validates ByteBuffer content to catch torn references.")
    @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Reader sees the written value with correct content.")
    @Outcome(id = "0", expect = Expect.ACCEPTABLE_INTERESTING, desc = "Stale read: reader missed the write (L1 miss).")
    @Outcome(id = "-1", expect = Expect.FORBIDDEN, desc = "Corrupt content — torn reference or wrong buffer.")
    @Outcome(expect = Expect.FORBIDDEN, desc = "Unexpected value.")
    @State
    public static class PutGetVisibility {
        private static final byte SEED_MAGIC = (byte) 0x11;
        private static final byte SENTINEL_MAGIC = (byte) 0xAA;

        private final RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        private final ByteBuffer seed = makeBuf(SEED_MAGIC);
        private final ByteBuffer sentinel = makeBuf(SENTINEL_MAGIC);

        public PutGetVisibility() {
            table.put(0, seed);
        }

        @Actor
        public void writer() {
            table.put(0, sentinel);
        }

        @Actor
        public void reader(I_Result r) {
            ByteBuffer val = table.get(0);
            if (val == null) {
                r.r1 = 0; // stale null
            } else if (val == sentinel) {
                r.r1 = validateContent(val, SENTINEL_MAGIC) ? 1 : -1;
            } else if (val == seed) {
                r.r1 = validateContent(val, SEED_MAGIC) ? 0 : -1;
            } else {
                r.r1 = -1; // wrong reference entirely
            }
        }
    }

    // ========================================================================
    // Test 2: Inner array publication visibility
    //
    // Actor 1: put(blockId, sentinel) — triggers inner array allocation
    // Actor 2: get(blockId) and validate content
    // ========================================================================

    @JCStressTest
    @Description("Inner array allocation + slot write visibility. "
        + "Reader validates ByteBuffer content on freshly allocated inner array.")
    @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Reader sees the written value with correct content.")
    @Outcome(id = "0", expect = Expect.ACCEPTABLE_INTERESTING, desc = "Stale read: inner array or slot not yet visible.")
    @Outcome(id = "-1", expect = Expect.FORBIDDEN, desc = "Corrupt content or wrong buffer reference.")
    @Outcome(expect = Expect.FORBIDDEN, desc = "Unexpected value.")
    @State
    public static class InnerArrayPublicationVisibility {
        private static final byte MAGIC = (byte) 0xBB;
        private static final long BLOCK_ID = RadixBlockTable.PAGE_SIZE; // outer=1, slot=0

        private final RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        private final ByteBuffer sentinel = makeBuf(MAGIC);

        @Actor
        public void writer() {
            table.put(BLOCK_ID, sentinel);
        }

        @Actor
        public void reader(I_Result r) {
            ByteBuffer val = table.get(BLOCK_ID);
            if (val == null) {
                r.r1 = 0;
            } else if (val == sentinel) {
                r.r1 = validateContent(val, MAGIC) ? 1 : -1;
            } else {
                r.r1 = -1;
            }
        }
    }

    // ========================================================================
    // Test 3: Directory growth visibility
    //
    // Actor 1: put(blockId far beyond current directory) — triggers growDirectory
    // Actor 2: get(same blockId) and validate content
    // ========================================================================

    @JCStressTest
    @Description("Directory growth visibility. Writer triggers directory " + "expansion. Reader validates ByteBuffer content if visible.")
    @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Reader sees grown directory and correct content.")
    @Outcome(id = "0", expect = Expect.ACCEPTABLE_INTERESTING, desc = "Stale read: reader sees old directory (too small).")
    @Outcome(id = "-1", expect = Expect.FORBIDDEN, desc = "Corrupt content or wrong buffer.")
    @Outcome(expect = Expect.FORBIDDEN, desc = "Unexpected value.")
    @State
    public static class DirectoryGrowthVisibility {
        private static final byte MAGIC = (byte) 0xCC;
        private static final long BLOCK_ID = 5L * RadixBlockTable.PAGE_SIZE;

        private final RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
        private final ByteBuffer sentinel = makeBuf(MAGIC);

        @Actor
        public void writer() {
            table.put(BLOCK_ID, sentinel);
        }

        @Actor
        public void reader(I_Result r) {
            ByteBuffer val = table.get(BLOCK_ID);
            if (val == null) {
                r.r1 = 0;
            } else if (val == sentinel) {
                r.r1 = validateContent(val, MAGIC) ? 1 : -1;
            } else {
                r.r1 = -1;
            }
        }
    }

    // ========================================================================
    // Test 4: Remove/Get race — reclamation safety with content validation
    //
    // Pre-populate slot. Actor 1 removes it. Actor 2 reads and validates.
    // ========================================================================

    @JCStressTest
    @Description("Remove/Get race with inner array reclamation. " + "Reader validates ByteBuffer content to catch use-after-reclaim.")
    @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Reader sees the value with correct content before removal.")
    @Outcome(id = "0", expect = Expect.ACCEPTABLE, desc = "Reader sees null (removal visible or stale).")
    @Outcome(id = "-1", expect = Expect.FORBIDDEN, desc = "Corrupt content or wrong buffer — reclamation bug.")
    @Outcome(expect = Expect.FORBIDDEN, desc = "Unexpected value.")
    @State
    public static class RemoveGetReclamationSafety {
        private static final byte MAGIC = (byte) 0xDD;

        private final RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        private final ByteBuffer sentinel = makeBuf(MAGIC);

        public RemoveGetReclamationSafety() {
            table.put(0, sentinel);
        }

        @Actor
        public void remover() {
            table.remove(0);
        }

        @Actor
        public void reader(I_Result r) {
            ByteBuffer val = table.get(0);
            if (val == null) {
                r.r1 = 0;
            } else if (val == sentinel) {
                r.r1 = validateContent(val, MAGIC) ? 1 : -1;
            } else {
                r.r1 = -1;
            }
        }
    }

    // ========================================================================
    // Test 5: Concurrent put to same slot — last-writer-wins, content intact
    //
    // Two actors write different ByteBuffers (different magic bytes).
    // A third actor reads and validates whichever buffer it sees.
    // ========================================================================

    @JCStressTest
    @Description("Two concurrent writers to the same slot + one reader. " + "Reader validates content of whichever ByteBuffer it observes.")
    @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Reader sees writer A's buffer with correct content.")
    @Outcome(id = "2", expect = Expect.ACCEPTABLE, desc = "Reader sees writer B's buffer with correct content.")
    @Outcome(id = "0", expect = Expect.ACCEPTABLE_INTERESTING, desc = "Stale read — seed or null.")
    @Outcome(id = "-1", expect = Expect.FORBIDDEN, desc = "Corrupt content — torn reference between A and B.")
    @Outcome(expect = Expect.FORBIDDEN, desc = "Unexpected value.")
    @State
    public static class ConcurrentPutSameSlot {
        private static final byte SEED_MAGIC = (byte) 0x00;
        private static final byte MAGIC_A = (byte) 0xAA;
        private static final byte MAGIC_B = (byte) 0xBB;

        private final RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        private final ByteBuffer seed = makeBuf(SEED_MAGIC);
        private final ByteBuffer bufA = makeBuf(MAGIC_A);
        private final ByteBuffer bufB = makeBuf(MAGIC_B);

        public ConcurrentPutSameSlot() {
            table.put(0, seed);
        }

        @Actor
        public void writerA() {
            table.put(0, bufA);
        }

        @Actor
        public void writerB() {
            table.put(0, bufB);
        }

        @Actor
        public void reader(I_Result r) {
            ByteBuffer val = table.get(0);
            if (val == null) {
                r.r1 = 0;
            } else if (val == bufA) {
                r.r1 = validateContent(val, MAGIC_A) ? 1 : -1;
            } else if (val == bufB) {
                r.r1 = validateContent(val, MAGIC_B) ? 2 : -1;
            } else if (val == seed) {
                r.r1 = validateContent(val, SEED_MAGIC) ? 0 : -1;
            } else {
                r.r1 = -1;
            }
        }
    }

    // ========================================================================
    // Test 6: Put + Remove on same slot — arbiter validates final state
    // ========================================================================

    @JCStressTest
    @Description("Concurrent put and remove on the same slot. " + "Arbiter validates final ByteBuffer content is consistent.")
    @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Put won: buffer present with correct content.")
    @Outcome(id = "0", expect = Expect.ACCEPTABLE, desc = "Remove won: slot is null.")
    @Outcome(id = "-1", expect = Expect.FORBIDDEN, desc = "Corrupt content or wrong buffer.")
    @Outcome(expect = Expect.FORBIDDEN, desc = "Unexpected value.")
    @State
    public static class PutRemoveSameSlot {
        private static final byte SEED_MAGIC = (byte) 0x11;
        private static final byte SENTINEL_MAGIC = (byte) 0xEE;

        private final RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(4);
        private final ByteBuffer seed = makeBuf(SEED_MAGIC);
        private final ByteBuffer sentinel = makeBuf(SENTINEL_MAGIC);

        public PutRemoveSameSlot() {
            table.put(0, seed);
        }

        @Actor
        public void putter() {
            table.put(0, sentinel);
        }

        @Actor
        public void remover() {
            table.remove(0);
        }

        @Arbiter
        public void arbiter(I_Result r) {
            ByteBuffer val = table.get(0);
            if (val == null) {
                r.r1 = 0;
            } else if (val == sentinel) {
                r.r1 = validateContent(val, SENTINEL_MAGIC) ? 1 : -1;
            } else {
                r.r1 = -1;
            }
        }
    }

    // ========================================================================
    // Test 7: Directory growth + concurrent read of existing entry
    //
    // Pre-populate blockId=0. Actor 1 triggers directory growth.
    // Actor 2 reads blockId=0 and validates content is intact.
    // ========================================================================

    @JCStressTest
    @Description("Directory growth must preserve existing entries. " + "Reader validates ByteBuffer content of pre-existing entry.")
    @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Reader sees pre-existing entry with correct content.")
    @Outcome(id = "0", expect = Expect.ACCEPTABLE_INTERESTING, desc = "Stale read of old directory — entry appears missing.")
    @Outcome(id = "-1", expect = Expect.FORBIDDEN, desc = "Corrupt content — growth corrupted existing data.")
    @Outcome(expect = Expect.FORBIDDEN, desc = "Unexpected value.")
    @State
    public static class DirectoryGrowthPreservesExisting {
        private static final byte EXISTING_MAGIC = (byte) 0x55;
        private static final byte FAR_MAGIC = (byte) 0x66;

        private final RadixBlockTable<ByteBuffer> table = new RadixBlockTable<>(1);
        private final ByteBuffer existing = makeBuf(EXISTING_MAGIC);
        private final ByteBuffer farAway = makeBuf(FAR_MAGIC);

        public DirectoryGrowthPreservesExisting() {
            table.put(0, existing);
        }

        @Actor
        public void grower() {
            table.put(10L * RadixBlockTable.PAGE_SIZE, farAway);
        }

        @Actor
        public void reader(I_Result r) {
            ByteBuffer val = table.get(0);
            if (val == null) {
                r.r1 = 0;
            } else if (val == existing) {
                r.r1 = validateContent(val, EXISTING_MAGIC) ? 1 : -1;
            } else {
                r.r1 = -1;
            }
        }
    }
}

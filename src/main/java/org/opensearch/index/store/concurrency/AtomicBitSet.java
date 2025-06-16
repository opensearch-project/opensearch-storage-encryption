/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.concurrency;

import java.util.concurrent.atomic.AtomicLongArray;

public class AtomicBitSet {
    private final AtomicLongArray bits;
    private final long size;

    public AtomicBitSet(long size) {
        this.size = size;
        this.bits = new AtomicLongArray((int) ((size + 63) / 64));
    }

    public boolean getAndSet(long index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }
        int wordIndex = (int) (index >>> 6); // divide by 64
        long bitMask = 1L << (index & 63);  // mod 64

        while (true) {
            long oldValue = bits.get(wordIndex);
            if ((oldValue & bitMask) != 0) {
                return true; // Already set
            }
            if (bits.compareAndSet(wordIndex, oldValue, oldValue | bitMask)) {
                return false; // We set it
            }
        }
    }

    public boolean get(long index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }
        int wordIndex = (int) (index >>> 6); // divide by 64
        long bitMask = 1L << (index & 63);  // mod 64

        long currentValue = bits.get(wordIndex);
        return (currentValue & bitMask) != 0;
    }

    public void clear(long index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }
        int wordIndex = (int) (index >>> 6);
        long bitMask = 1L << (index & 63);

        while (true) {
            long oldValue = bits.get(wordIndex);
            if (bits.compareAndSet(wordIndex, oldValue, oldValue & ~bitMask)) {
                return;
            }
        }
    }
}

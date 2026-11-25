/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.opensearch.index.store.CryptoTestDirectoryFactory.createBufferPoolDirectory;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NativeFSLockFactory;
import org.opensearch.common.lucene.store.OpenSearchIndexInputTestCase;
import org.opensearch.index.store.CaffeineThreadLeakFilter;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

/**
 * Fuzz-style tests for {@link CachedMemorySegmentIndexInput} using
 * {@link OpenSearchIndexInputTestCase#randomReadAndSlice} which randomly
 * interleaves readByte, readBytes, slice, seek, clone, and concurrent
 * clone operations. Exercises the full encryption stack (no mocks).
 */
@ThreadLeakFilters(filters = CaffeineThreadLeakFilter.class)
public class CachedMemorySegmentIndexInputRandomReadTests extends OpenSearchIndexInputTestCase {

    private BufferPoolDirectory createDirectory(Path path) throws IOException {
        return createBufferPoolDirectory(path, NativeFSLockFactory.INSTANCE);
    }

    private IndexInput writeAndOpen(BufferPoolDirectory dir, String name, byte[] data) throws IOException {
        try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }
        return dir.openInput(name, IOContext.DEFAULT);
    }

    /**
     * Small file that fits in a single 8192-byte cache block.
     */
    public void testRandomReadAndSliceSingleBlock() throws IOException {
        Path path = createTempDir("singleblock");
        try (BufferPoolDirectory dir = createDirectory(path)) {
            byte[] expected = new byte[4096];
            random().nextBytes(expected);

            try (IndexInput input = writeAndOpen(dir, "single.dat", expected)) {
                byte[] actual = randomReadAndSlice(input, expected.length);
                assertArrayEquals(expected, actual);
            }
        }
    }

    /**
     * File spanning exactly two blocks (16384 bytes = 2 x 8192).
     * Tests block-boundary transitions during random operations.
     */
    public void testRandomReadAndSliceTwoBlocks() throws IOException {
        Path path = createTempDir("twoblocks");
        try (BufferPoolDirectory dir = createDirectory(path)) {
            byte[] expected = new byte[16384];
            random().nextBytes(expected);

            try (IndexInput input = writeAndOpen(dir, "two.dat", expected)) {
                byte[] actual = randomReadAndSlice(input, expected.length);
                assertArrayEquals(expected, actual);
            }
        }
    }

    /**
     * File at block boundary minus one byte (8191). The last byte of block 0
     * is the last byte of the file — stresses boundary arithmetic.
     */
    public void testRandomReadAndSliceBlockBoundaryMinusOne() throws IOException {
        Path path = createTempDir("boundary-1");
        try (BufferPoolDirectory dir = createDirectory(path)) {
            byte[] expected = new byte[8191];
            random().nextBytes(expected);

            try (IndexInput input = writeAndOpen(dir, "bm1.dat", expected)) {
                byte[] actual = randomReadAndSlice(input, expected.length);
                assertArrayEquals(expected, actual);
            }
        }
    }

    /**
     * File at block boundary plus one byte (8193). The first byte of block 1
     * is the only byte in that block — tests partial last block handling.
     */
    public void testRandomReadAndSliceBlockBoundaryPlusOne() throws IOException {
        Path path = createTempDir("boundary+1");
        try (BufferPoolDirectory dir = createDirectory(path)) {
            byte[] expected = new byte[8193];
            random().nextBytes(expected);

            try (IndexInput input = writeAndOpen(dir, "bp1.dat", expected)) {
                byte[] actual = randomReadAndSlice(input, expected.length);
                assertArrayEquals(expected, actual);
            }
        }
    }

    /**
     * Multi-block file with random size spanning 3-10 blocks.
     * Higher probability of hitting diverse block/position combinations.
     */
    public void testRandomReadAndSliceMultiBlock() throws IOException {
        Path path = createTempDir("multiblock");
        try (BufferPoolDirectory dir = createDirectory(path)) {
            int numBlocks = randomIntBetween(3, 10);
            byte[] expected = new byte[numBlocks * 8192 + randomIntBetween(0, 8191)];
            random().nextBytes(expected);

            try (IndexInput input = writeAndOpen(dir, "multi.dat", expected)) {
                byte[] actual = randomReadAndSlice(input, expected.length);
                assertArrayEquals(expected, actual);
            }
        }
    }

    /**
     * Runs randomReadAndSlice on a slice of the input (not the whole file).
     * Tests that absoluteBaseOffset arithmetic composes correctly when
     * randomReadAndSlice creates nested slices internally.
     */
    public void testRandomReadAndSliceOnSlice() throws IOException {
        Path path = createTempDir("sliceslice");
        try (BufferPoolDirectory dir = createDirectory(path)) {
            byte[] fullData = new byte[8192 * 5];
            random().nextBytes(fullData);

            try (IndexInput input = writeAndOpen(dir, "full.dat", fullData)) {
                // Take a slice starting mid-block, spanning multiple blocks
                int sliceOffset = 8192 + 1000; // starts 1000 bytes into block 1
                int sliceLen = 8192 * 2 + 500; // spans ~2.5 blocks
                try (IndexInput slice = input.slice("test-slice", sliceOffset, sliceLen)) {
                    byte[] actual = randomReadAndSlice(slice, sliceLen);

                    byte[] expected = new byte[sliceLen];
                    System.arraycopy(fullData, sliceOffset, expected, 0, sliceLen);
                    assertArrayEquals(expected, actual);
                }
            }
        }
    }

    /**
     * Runs randomReadAndSlice on a clone. Tests that clones have
     * independent position tracking while sharing the block cache.
     */
    public void testRandomReadAndSliceOnClone() throws IOException {
        Path path = createTempDir("clone");
        try (BufferPoolDirectory dir = createDirectory(path)) {
            byte[] expected = new byte[8192 * 3 + 2000];
            random().nextBytes(expected);

            try (IndexInput input = writeAndOpen(dir, "clone.dat", expected)) {
                // Advance original to mid-file
                input.seek(8192 + 500);

                // Clone starts at same position, but randomReadAndSlice needs pos=0
                try (IndexInput clone = input.clone()) {
                    clone.seek(0);
                    byte[] actual = randomReadAndSlice(clone, expected.length);
                    assertArrayEquals(expected, actual);
                }

                // Original position should be unaffected
                assertEquals(8192 + 500, input.getFilePointer());
            }
        }
    }

    /**
     * Small file (< 1 block) run multiple times with different random seeds.
     * The randomized testing framework varies the seed per run, so this
     * exercises many different operation sequences over repeated CI runs.
     */
    public void testRandomReadAndSliceRepeated() throws IOException {
        Path path = createTempDir("repeated");
        try (BufferPoolDirectory dir = createDirectory(path)) {
            int size = randomIntBetween(1, 8192);
            byte[] expected = new byte[size];
            random().nextBytes(expected);

            try (IndexInput input = writeAndOpen(dir, "rep.dat", expected)) {
                for (int i = 0; i < 3; i++) {
                    input.seek(0);
                    byte[] actual = randomReadAndSlice(input, expected.length);
                    assertArrayEquals("Iteration " + i, expected, actual);
                }
            }
        }
    }

    /**
     * Single byte file — minimum viable input.
     * Tests that randomReadAndSlice handles the degenerate case where
     * slice/seek operations have almost no room to maneuver.
     */
    public void testRandomReadAndSliceSingleByte() throws IOException {
        Path path = createTempDir("singlebyte");
        try (BufferPoolDirectory dir = createDirectory(path)) {
            byte[] expected = new byte[] { (byte) 0xAB };

            try (IndexInput input = writeAndOpen(dir, "one.dat", expected)) {
                byte[] actual = randomReadAndSlice(input, 1);
                assertArrayEquals(expected, actual);
            }
        }
    }
}

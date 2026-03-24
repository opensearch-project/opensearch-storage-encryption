/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.niofs;

import static org.opensearch.index.store.CryptoTestDirectoryFactory.createCryptoNIOFSDirectory;

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
 * Fuzz-style tests for {@link CryptoNIOFSDirectory} using
 * {@link OpenSearchIndexInputTestCase#randomReadAndSlice} which randomly
 * interleaves readByte, readBytes, slice, seek, clone, and concurrent
 * clone operations. Exercises the full NIO encryption stack (no mocks).
 */
@ThreadLeakFilters(filters = CaffeineThreadLeakFilter.class)
public class CryptoNIOFSDirectoryRandomReadTests extends OpenSearchIndexInputTestCase {

    private CryptoNIOFSDirectory createDirectory(Path path) throws IOException {
        return createCryptoNIOFSDirectory(path, NativeFSLockFactory.INSTANCE);
    }

    private IndexInput writeAndOpen(CryptoNIOFSDirectory dir, String name, byte[] data) throws IOException {
        try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }
        return dir.openInput(name, IOContext.DEFAULT);
    }

    public void testRandomReadAndSliceSingleBlock() throws IOException {
        Path path = createTempDir("singleblock");
        try (CryptoNIOFSDirectory dir = createDirectory(path)) {
            byte[] expected = new byte[4096];
            random().nextBytes(expected);

            try (IndexInput input = writeAndOpen(dir, "single.dat", expected)) {
                byte[] actual = randomReadAndSlice(input, expected.length);
                assertArrayEquals(expected, actual);
            }
        }
    }

    public void testRandomReadAndSliceTwoBlocks() throws IOException {
        Path path = createTempDir("twoblocks");
        try (CryptoNIOFSDirectory dir = createDirectory(path)) {
            byte[] expected = new byte[16384];
            random().nextBytes(expected);

            try (IndexInput input = writeAndOpen(dir, "two.dat", expected)) {
                byte[] actual = randomReadAndSlice(input, expected.length);
                assertArrayEquals(expected, actual);
            }
        }
    }

    public void testRandomReadAndSliceBlockBoundaryMinusOne() throws IOException {
        Path path = createTempDir("boundary-1");
        try (CryptoNIOFSDirectory dir = createDirectory(path)) {
            byte[] expected = new byte[8191];
            random().nextBytes(expected);

            try (IndexInput input = writeAndOpen(dir, "bm1.dat", expected)) {
                byte[] actual = randomReadAndSlice(input, expected.length);
                assertArrayEquals(expected, actual);
            }
        }
    }

    public void testRandomReadAndSliceBlockBoundaryPlusOne() throws IOException {
        Path path = createTempDir("boundary+1");
        try (CryptoNIOFSDirectory dir = createDirectory(path)) {
            byte[] expected = new byte[8193];
            random().nextBytes(expected);

            try (IndexInput input = writeAndOpen(dir, "bp1.dat", expected)) {
                byte[] actual = randomReadAndSlice(input, expected.length);
                assertArrayEquals(expected, actual);
            }
        }
    }

    public void testRandomReadAndSliceMultiBlock() throws IOException {
        Path path = createTempDir("multiblock");
        try (CryptoNIOFSDirectory dir = createDirectory(path)) {
            int numBlocks = randomIntBetween(3, 10);
            byte[] expected = new byte[numBlocks * 8192 + randomIntBetween(0, 8191)];
            random().nextBytes(expected);

            try (IndexInput input = writeAndOpen(dir, "multi.dat", expected)) {
                byte[] actual = randomReadAndSlice(input, expected.length);
                assertArrayEquals(expected, actual);
            }
        }
    }

    public void testRandomReadAndSliceOnSlice() throws IOException {
        Path path = createTempDir("sliceslice");
        try (CryptoNIOFSDirectory dir = createDirectory(path)) {
            byte[] fullData = new byte[8192 * 5];
            random().nextBytes(fullData);

            try (IndexInput input = writeAndOpen(dir, "full.dat", fullData)) {
                int sliceOffset = 8192 + 1000;
                int sliceLen = 8192 * 2 + 500;
                try (IndexInput slice = input.slice("test-slice", sliceOffset, sliceLen)) {
                    byte[] actual = randomReadAndSlice(slice, sliceLen);

                    byte[] expected = new byte[sliceLen];
                    System.arraycopy(fullData, sliceOffset, expected, 0, sliceLen);
                    assertArrayEquals(expected, actual);
                }
            }
        }
    }

    public void testRandomReadAndSliceOnClone() throws IOException {
        Path path = createTempDir("clone");
        try (CryptoNIOFSDirectory dir = createDirectory(path)) {
            byte[] expected = new byte[8192 * 3 + 2000];
            random().nextBytes(expected);

            try (IndexInput input = writeAndOpen(dir, "clone.dat", expected)) {
                input.seek(8192 + 500);

                try (IndexInput clone = input.clone()) {
                    clone.seek(0);
                    byte[] actual = randomReadAndSlice(clone, expected.length);
                    assertArrayEquals(expected, actual);
                }

                assertEquals(8192 + 500, input.getFilePointer());
            }
        }
    }

    public void testRandomReadAndSliceSingleByte() throws IOException {
        Path path = createTempDir("singlebyte");
        try (CryptoNIOFSDirectory dir = createDirectory(path)) {
            byte[] expected = new byte[] { (byte) 0xAB };

            try (IndexInput input = writeAndOpen(dir, "one.dat", expected)) {
                byte[] actual = randomReadAndSlice(input, 1);
                assertArrayEquals(expected, actual);
            }
        }
    }
}

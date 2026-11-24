/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.hamcrest.Matchers.containsString;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.common.lucene.store.OpenSearchIndexInputTestCase;

public abstract class BaseIndexInputTests extends OpenSearchIndexInputTestCase {

    protected abstract IndexInput getIndexInput(byte[] bytes) throws IOException;

    public void testRandomReads() throws IOException {

        for (int i = 0; i < 100; i++) {
            byte[] input = randomUnicodeOfLength(randomIntBetween(7 * 1024 + 7, 7 * 1024 + 113)).getBytes(StandardCharsets.UTF_8);
            IndexInput indexInput = getIndexInput(input);
            long length = indexInput.length();
            assertEquals(input.length, length);
            assertEquals(0, indexInput.getFilePointer());
            byte[] output = randomReadAndSlice(indexInput, (int) length);
            compareArrays(input, output);
        }
    }

    public void testRandomReadsConcurrent() throws IOException {
        int numReaders = 32;
        ExecutorService readers = Executors.newFixedThreadPool(16);
        for (int i = 0; i < 1000; i++) {
            int start = randomIntBetween(7, 7 * 23);
            int end = start + randomIntBetween(7, 7 * 23);
            byte[] input = randomUnicodeOfLength(randomIntBetween(1024 + start, 1024 + end)).getBytes(StandardCharsets.UTF_8);
            IndexInput indexInput = getIndexInput(input);
            long length = indexInput.length();
            assertEquals(input.length, length);
            assertEquals(0, indexInput.getFilePointer());
            CountDownLatch countDownLatch = new CountDownLatch(numReaders);
            for (int readerIndex = 0; readerIndex < numReaders; readerIndex++) {
                int sliceOffset = randomIntBetween(0, (int) length - 1);
                int sliceLength = randomIntBetween(0, (int) length - sliceOffset);
                IndexInput readerCone = indexInput.slice("slice-reader-" + readerIndex, sliceOffset, sliceLength);
                readers.submit(() -> {
                    try {
                        byte[] output = randomReadAndSlice(readerCone, (int) sliceLength);
                        compareArrays(input, output);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {}

        }
        readers.shutdown();
    }

    private void compareArrays(byte[] arr1, byte[] arr2) {
        assertEquals("Array lengths differ", arr1.length, arr2.length);

        for (int i = 0; i < arr1.length; i++) {
            if (arr1[i] != arr2[i]) {
                fail("Arrays differ at index " + i + " expected=" + arr1[i] + " actual=" + arr2[i]);
            }
        }
    }

    public void testRandomOverflow() throws IOException {
        for (int i = 0; i < 100; i++) {
            byte[] input = randomUnicodeOfLength(randomIntBetween(1, 1000)).getBytes(StandardCharsets.UTF_8);
            IndexInput indexInput = getIndexInput(input);
            int firstReadLen = randomIntBetween(0, input.length - 1);
            randomReadAndSlice(indexInput, firstReadLen);
            int bytesLeft = input.length - firstReadLen;
            try {
                // read using int size
                int secondReadLen = bytesLeft + randomIntBetween(1, 100);
                indexInput.readBytes(new byte[secondReadLen], 0, secondReadLen);
                fail();
            } catch (IOException ex) {
                assertThat(ex.getMessage(), containsString("EOF"));
            }
        }
    }

    public void testSeekOverflow() throws IOException {
        for (int i = 0; i < 100; i++) {
            byte[] input = randomUnicodeOfLength(randomIntBetween(1, 1000)).getBytes(StandardCharsets.UTF_8);
            IndexInput indexInput = getIndexInput(input);
            int firstReadLen = randomIntBetween(0, input.length - 1);
            randomReadAndSlice(indexInput, firstReadLen);
            try {
                switch (randomIntBetween(0, 2)) {
                    case 0:
                        indexInput.seek(Integer.MAX_VALUE + 4L);
                        break;
                    case 1:
                        indexInput.seek(-randomIntBetween(1, 10));
                        break;
                    case 2:
                        int seek = input.length + randomIntBetween(1, 100);
                        indexInput.seek(seek);
                        break;
                    default:
                        fail();
                }
                fail();
            } catch (IOException ex) {
                assertThat(ex.getMessage(), containsString("EOF"));
            } catch (IllegalArgumentException ex) {
                assertThat(ex.getMessage(), containsString("negative position"));
            }
        }
    }

    public void testReadBytesWithSlice() throws IOException {
        int inputLength = randomIntBetween(1024 * 50, 1024 * 100) + randomIntBetween(3, 7);

        byte[] input = randomUnicodeOfLength(inputLength).getBytes(StandardCharsets.UTF_8);
        IndexInput indexInput = getIndexInput(input);

        int sliceOffset = randomIntBetween(1, inputLength - 10);
        int sliceLength = randomIntBetween(2, inputLength - sliceOffset);
        IndexInput slice = indexInput.slice("slice", sliceOffset, sliceLength);

        if (slice instanceof CachedMemorySegmentIndexInput) {
            CachedMemorySegmentIndexInput cachedMemorySegmentIndexInput = (CachedMemorySegmentIndexInput) slice;
            byte b = cachedMemorySegmentIndexInput.readByte();
            assertEquals(input[sliceOffset], b);

            int offset = randomIntBetween(0, sliceLength - 1);
            byte randomRead = cachedMemorySegmentIndexInput.readByte(offset);
            assertEquals(input[sliceOffset + offset], randomRead);

            // read few more bytes into a byte array
            int bytesToRead = randomIntBetween(1, sliceLength - 1);
            cachedMemorySegmentIndexInput.readBytes(new byte[bytesToRead], 0, bytesToRead);

            // now try to read beyond the boundary of the slice, but within the
            // boundary of the original IndexInput. We've already read few bytes
            // so this is expected to fail
            expectThrows(EOFException.class, () -> slice.readBytes(new byte[sliceLength], 0, sliceLength));

            // seek to EOF and then try to read
            slice.seek(sliceLength);
            expectThrows(EOFException.class, () -> slice.readBytes(new byte[1], 0, 1));
            expectThrows(EOFException.class, slice::readByte);
        }
        // read a byte from sliced index input and verify if the read value is correct

    }

    /**
     * Every read that would run past the end of a slice must fail, even though the bytes after
     * the slice exist in the underlying file. Otherwise a reader of one compound-file entry
     * silently gets the next entry's bytes.
     */
    public void testReadsPastSliceEndThrow() throws IOException {
        int inputLength = randomIntBetween(1024 * 20, 1024 * 40);
        byte[] input = new byte[inputLength];
        random().nextBytes(input);
        IndexInput indexInput = getIndexInput(input);

        int sliceOffset = randomIntBetween(1, inputLength / 2);
        int sliceLength = randomIntBetween(16, inputLength - sliceOffset - 16);
        IndexInput slice = indexInput.slice("slice", sliceOffset, sliceLength);

        // Sequential reads that straddle the end of the slice.
        for (int width : new int[] { Byte.BYTES, Short.BYTES, Integer.BYTES, Long.BYTES }) {
            slice.seek(sliceLength - width + 1);
            switch (width) {
                case Byte.BYTES -> expectThrows(EOFException.class, slice::readByte);
                case Short.BYTES -> expectThrows(EOFException.class, slice::readShort);
                case Integer.BYTES -> expectThrows(EOFException.class, slice::readInt);
                default -> expectThrows(EOFException.class, slice::readLong);
            }
        }
        slice.seek(sliceLength - 1);
        expectThrows(EOFException.class, () -> slice.readInts(new int[1], 0, 1));
        expectThrows(EOFException.class, () -> slice.readLongs(new long[1], 0, 1));
        expectThrows(EOFException.class, () -> slice.readFloats(new float[1], 0, 1));

        // Reads that end exactly at the slice end still succeed.
        slice.seek(sliceLength - Long.BYTES);
        slice.readLong();
        assertEquals(sliceLength, slice.getFilePointer());

        if (slice instanceof RandomAccessInput random) {
            expectThrows(EOFException.class, () -> random.readByte(sliceLength));
            expectThrows(EOFException.class, () -> random.readShort(sliceLength - 1));
            expectThrows(EOFException.class, () -> random.readInt(sliceLength - 3));
            expectThrows(EOFException.class, () -> random.readLong(sliceLength - 7));
            expectThrows(IllegalArgumentException.class, () -> random.readByte(-1));
            assertEquals(input[sliceOffset + sliceLength - 1], random.readByte(sliceLength - 1));
        }
    }
}

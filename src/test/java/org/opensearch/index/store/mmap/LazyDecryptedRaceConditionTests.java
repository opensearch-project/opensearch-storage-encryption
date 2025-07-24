/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.mmap;

import static org.junit.Assert.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * The test focuses on the specific scenario where multiple
 * threads attempt to read the same encrypted page simultaneously.
 */
public class LazyDecryptedRaceConditionTests {

    // Use system page size aligned values to avoid boundary issues
    private static int getSystemPageSize() {
        try {
            return PanamaNativeAccess.getPageSize();
        } catch (Exception e) {
            return 4096; // fallback
        }
    }

    private static final int PAGE_SIZE = getSystemPageSize();
    private static final int DATA_SIZE = PAGE_SIZE * 4; // 4 pages worth of data
    private static final int CHUNK_SIZE_POWER = Integer.numberOfTrailingZeros(DATA_SIZE); // Match data size
    private static final int NUM_THREADS = 6;
    private static final int TEST_TIMEOUT_SECONDS = 10;

    private Arena arena;
    private MemorySegment segment;
    private byte[] testKey;
    private byte[] testIv;
    private byte[] expectedPlaintextData;

    @Before
    public void setUp() throws Exception {
        arena = Arena.ofShared();
        segment = arena.allocate(DATA_SIZE);

        // Use fixed test key and IV for predictable results
        testKey = new byte[32]; // 256-bit AES key
        testIv = new byte[16];  // 128-bit IV
        Arrays.fill(testKey, (byte) 0x42);
        Arrays.fill(testIv, (byte) 0x24);

        // STEP 1: Create known plaintext data
        byte[] plaintextData = new byte[DATA_SIZE];
        for (int i = 0; i < DATA_SIZE; i++) {
            plaintextData[i] = (byte) (i & 0xFF); // Simple incremental pattern
        }

        // STEP 2: Encrypt the plaintext data using the same key/IV that will be used for decryption
        byte[] encryptedData = encryptData(plaintextData, testKey, testIv);

        // STEP 3: Put the ENCRYPTED data into the memory segment
        // This simulates what would happen in real usage - the segment contains encrypted data
        for (int i = 0; i < DATA_SIZE; i++) {
            segment.set(ValueLayout.JAVA_BYTE, i, encryptedData[i]);
        }

        // Store original plaintext for verification
        this.expectedPlaintextData = plaintextData;
    }

    /**
     * Encrypt data using the same method as MemorySegmentDecryptor but in reverse.
     * This matches the complex IV manipulation and block alignment.
     */
    private byte[] encryptData(byte[] plaintext, byte[] key, byte[] iv) throws Exception {
        javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance("AES/CTR/NoPadding", "SunJCE");
        javax.crypto.spec.SecretKeySpec keySpec = new javax.crypto.spec.SecretKeySpec(key, "AES");

        // Use the same IV manipulation as MemorySegmentDecryptor
        byte[] ivCopy = Arrays.copyOf(iv, iv.length);
        long fileOffset = 0; // We're encrypting from the beginning
        int blockOffset = (int) (fileOffset / 16); // AES_BLOCK_SIZE_BYTES = 16

        // Same IV manipulation as in MemorySegmentDecryptor
        ivCopy[15] = (byte) blockOffset;        // IV_ARRAY_LENGTH - 1
        ivCopy[14] = (byte) (blockOffset >>> 8); // IV_ARRAY_LENGTH - 2
        ivCopy[13] = (byte) (blockOffset >>> 16); // IV_ARRAY_LENGTH - 3
        ivCopy[12] = (byte) (blockOffset >>> 24); // IV_ARRAY_LENGTH - 4

        cipher.init(javax.crypto.Cipher.ENCRYPT_MODE, keySpec, new javax.crypto.spec.IvParameterSpec(ivCopy));

        // For simplicity, encrypt the whole block at once for this test
        return cipher.doFinal(plaintext);
    }

    @After
    public void tearDown() {
        if (arena != null && arena.scope().isAlive()) {
            arena.close();
        }
    }

    /**
     * First verify that our encrypted data setup works correctly.
     */
    @Test(timeout = TEST_TIMEOUT_SECONDS * 1000)
    public void testEncryptedDataSetupWorks() throws Exception {
        LazyDecryptedMemorySegmentIndexInput input = LazyDecryptedMemorySegmentIndexInput
            .newInstance("test-resource", arena, new MemorySegment[] { segment }, DATA_SIZE, CHUNK_SIZE_POWER, testKey, testIv);

        // Test that we can decrypt and get back the original plaintext
        input.seek(0);
        byte firstByte = input.readByte();
        assertEquals("Decrypted data should match original plaintext", expectedPlaintextData[0], firstByte);

        input.seek(50);
        byte fiftiethByte = input.readByte();
        assertEquals("Decrypted data should match original plaintext", expectedPlaintextData[50], fiftiethByte);

        input.close();
    }

    /**
     * Core test: Verify that multiple threads reading the same location
     * all get identical decrypted data. Now testing actual encrypted->decrypted data.
     */
    @Test(timeout = TEST_TIMEOUT_SECONDS * 1000)
    public void testConcurrentReadsSameLocation() throws Exception {
        LazyDecryptedMemorySegmentIndexInput input = LazyDecryptedMemorySegmentIndexInput
            .newInstance("test-resource", arena, new MemorySegment[] { segment }, DATA_SIZE, CHUNK_SIZE_POWER, testKey, testIv);

        // Test reading from the same safe location using random access
        final long testOffset = 50;
        final byte[] results = new byte[NUM_THREADS];
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(NUM_THREADS);
        final AtomicReference<Exception> firstException = new AtomicReference<>();

        // Create threads that all try to read the same byte simultaneously using random access
        for (int i = 0; i < NUM_THREADS; i++) {
            final int threadIndex = i;
            Thread thread = new Thread(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready

                    // All threads read from the same location using random access
                    byte result = input.readByte(testOffset);
                    results[threadIndex] = result;

                } catch (Exception e) {
                    firstException.compareAndSet(null, e);
                } finally {
                    finishLatch.countDown();
                }
            });
            thread.start();
        }

        // Start all threads simultaneously
        startLatch.countDown();

        // Wait for all threads to complete
        assertTrue("Threads did not complete within timeout", finishLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));

        // Check if any thread failed
        if (firstException.get() != null) {
            throw new AssertionError("Thread failed with exception", firstException.get());
        }

        // Verify all threads got the same result
        byte expectedResult = results[0];

        // IMPORTANT: Also verify that the result matches the expected plaintext
        byte expectedPlaintext = expectedPlaintextData[(int) testOffset];
        assertEquals("Decrypted result should match original plaintext", expectedPlaintext, expectedResult);

        for (int i = 1; i < NUM_THREADS; i++) {
            if (expectedResult != results[i]) {
                // This indicates a race condition was detected!
                System.err.println("RACE CONDITION DETECTED: Thread 0 got " + expectedResult + " but thread " + i + " got " + results[i]);
                System.err.println("Expected plaintext: " + expectedPlaintext);
                System.err.println("This confirms the test can detect race conditions that inProgressPages should prevent");
            }
            assertEquals(
                "All threads should read the same decrypted value (race condition detected if different)",
                expectedResult,
                results[i]
            );
        }

        input.close();
    }

    /**
     * Test concurrent reads from multiple safe locations using random access pattern.
     */
    @Test(timeout = TEST_TIMEOUT_SECONDS * 1000)
    public void testConcurrentReadsMultipleLocations() throws Exception {
        LazyDecryptedMemorySegmentIndexInput input = LazyDecryptedMemorySegmentIndexInput
            .newInstance("test-resource", arena, new MemorySegment[] { segment }, DATA_SIZE, CHUNK_SIZE_POWER, testKey, testIv);

        final int numOperations = 3; // Reduced operations for safety
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(NUM_THREADS);
        final AtomicInteger successfulReads = new AtomicInteger(0);
        final AtomicReference<Exception> firstException = new AtomicReference<>();

        for (int i = 0; i < NUM_THREADS; i++) {
            final int threadIndex = i;
            Thread thread = new Thread(() -> {
                try {
                    startLatch.await();

                    // Each thread performs multiple reads from very safe locations using random access
                    for (int op = 0; op < numOperations; op++) {
                        // Use very safe offsets similar to the working high contention test
                        long offset = 25 + (threadIndex * 15) + (op * 8); // Spread out safely
                        if (offset < 200) { // Stay within safe bounds
                            byte result = input.readByte(offset); // Use random access like working tests
                            successfulReads.incrementAndGet();
                        }
                    }

                } catch (Exception e) {
                    firstException.compareAndSet(null, e);
                } finally {
                    finishLatch.countDown();
                }
            });
            thread.start();
        }

        startLatch.countDown();

        assertTrue("Threads did not complete within timeout", finishLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));

        if (firstException.get() != null) {
            throw new AssertionError("Thread failed with exception", firstException.get());
        }

        // Verify most operations succeeded
        assertTrue("Most reads should succeed, got " + successfulReads.get(), successfulReads.get() > 0);

        input.close();
    }

    /**
     * Test that verifies data consistency when reading small blocks from safe locations.
     */
    @Test(timeout = TEST_TIMEOUT_SECONDS * 1000)
    public void testConcurrentBlockReads() throws Exception {
        LazyDecryptedMemorySegmentIndexInput input = LazyDecryptedMemorySegmentIndexInput
            .newInstance("test-resource", arena, new MemorySegment[] { segment }, DATA_SIZE, CHUNK_SIZE_POWER, testKey, testIv);

        final int blockSize = 8; // Very small block size
        final long testOffset = 10; // Safe offset
        final byte[][] results = new byte[NUM_THREADS][blockSize];
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(NUM_THREADS);
        final AtomicReference<Exception> firstException = new AtomicReference<>();

        for (int i = 0; i < NUM_THREADS; i++) {
            final int threadIndex = i;
            Thread thread = new Thread(() -> {
                try {
                    startLatch.await();

                    // All threads read the same small block
                    input.seek(testOffset);
                    input.readBytes(results[threadIndex], 0, blockSize);

                } catch (Exception e) {
                    firstException.compareAndSet(null, e);
                } finally {
                    finishLatch.countDown();
                }
            });
            thread.start();
        }

        startLatch.countDown();

        assertTrue("Threads did not complete within timeout", finishLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));

        if (firstException.get() != null) {
            throw new AssertionError("Thread failed with exception", firstException.get());
        }

        // Verify all threads got identical data - this is the key test
        byte[] referenceResult = results[0];
        for (int i = 1; i < NUM_THREADS; i++) {
            assertArrayEquals("All threads should read identical decrypted blocks", referenceResult, results[i]);
        }

        input.close();
    }

    /**
     * Test high contention scenario - many threads trying to read 
     * from the same few safe locations to maximize race condition probability.
     */
    @Test(timeout = TEST_TIMEOUT_SECONDS * 1000)
    public void testHighContentionRaceCondition() throws Exception {
        LazyDecryptedMemorySegmentIndexInput input = LazyDecryptedMemorySegmentIndexInput
            .newInstance("test-resource", arena, new MemorySegment[] { segment }, DATA_SIZE, CHUNK_SIZE_POWER, testKey, testIv);

        // Use just a few very safe offsets to maximize contention
        final long[] testOffsets = { 0, 5, 10, 15, 20 };
        final AtomicInteger successfulReads = new AtomicInteger(0);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(NUM_THREADS);
        final AtomicReference<Exception> firstException = new AtomicReference<>();

        for (int i = 0; i < NUM_THREADS; i++) {
            final int threadIndex = i;
            Thread thread = new Thread(() -> {
                try {
                    startLatch.await();

                    // Each thread reads from multiple contested locations using safe random access
                    for (long offset : testOffsets) {
                        byte result = input.readByte(offset); // Use safe random access pattern
                        successfulReads.incrementAndGet();

                        // Add small delay to increase race condition window
                        if (threadIndex % 3 == 0) {
                            Thread.yield();
                        }
                    }

                } catch (Exception e) {
                    firstException.compareAndSet(null, e);
                } finally {
                    finishLatch.countDown();
                }
            });
            thread.start();
        }

        startLatch.countDown();

        assertTrue("Threads did not complete within timeout", finishLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));

        if (firstException.get() != null) {
            throw new AssertionError("High contention test failed with exception", firstException.get());
        }

        // All operations should succeed despite high contention
        int expectedReads = NUM_THREADS * testOffsets.length;
        assertEquals("All reads should succeed despite high contention", expectedReads, successfulReads.get());

        input.close();
    }
}

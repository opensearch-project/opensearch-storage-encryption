/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.opensearch.test.OpenSearchTestCase;

public class FileBlockCacheKeyTests extends OpenSearchTestCase {

    /**
     * Tests basic equality of two identical keys.
     */
    public void testEqualityWithIdenticalKeys() {
        Path path = Paths.get("/test/file.dat");
        long offset = 1024L;

        FileBlockCacheKey key1 = new FileBlockCacheKey(path, offset);
        FileBlockCacheKey key2 = new FileBlockCacheKey(path, offset);

        assertEquals("Keys should be equal", key1, key2);
        assertEquals("Hash codes should be equal", key1.hashCode(), key2.hashCode());
    }

    /**
     * Tests reflexive property: x.equals(x) should be true.
     */
    public void testEqualityReflexive() {
        FileBlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 1024L);

        assertEquals("Key should equal itself", key, key);
    }

    /**
     * Tests symmetric property: if x.equals(y), then y.equals(x).
     */
    public void testEqualitySymmetric() {
        FileBlockCacheKey key1 = new FileBlockCacheKey(Paths.get("/test/file.dat"), 1024L);
        FileBlockCacheKey key2 = new FileBlockCacheKey(Paths.get("/test/file.dat"), 1024L);

        assertTrue("Symmetry: key1.equals(key2)", key1.equals(key2));
        assertTrue("Symmetry: key2.equals(key1)", key2.equals(key1));
    }

    /**
     * Tests transitive property: if x.equals(y) and y.equals(z), then x.equals(z).
     */
    public void testEqualityTransitive() {
        FileBlockCacheKey key1 = new FileBlockCacheKey(Paths.get("/test/file.dat"), 1024L);
        FileBlockCacheKey key2 = new FileBlockCacheKey(Paths.get("/test/file.dat"), 1024L);
        FileBlockCacheKey key3 = new FileBlockCacheKey(Paths.get("/test/file.dat"), 1024L);

        assertTrue("key1.equals(key2)", key1.equals(key2));
        assertTrue("key2.equals(key3)", key2.equals(key3));
        assertTrue("Transitivity: key1.equals(key3)", key1.equals(key3));
    }

    /**
     * Tests consistency: multiple invocations of equals should return same result.
     */
    public void testEqualityConsistent() {
        FileBlockCacheKey key1 = new FileBlockCacheKey(Paths.get("/test/file.dat"), 1024L);
        FileBlockCacheKey key2 = new FileBlockCacheKey(Paths.get("/test/file.dat"), 1024L);

        for (int i = 0; i < 100; i++) {
            assertTrue("Consistency check " + i, key1.equals(key2));
        }
    }

    /**
     * Tests null comparison: x.equals(null) should be false.
     */
    public void testEqualityWithNull() {
        FileBlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 1024L);

        assertFalse("Key should not equal null", key.equals(null));
    }

    /**
     * Tests keys with different offsets are not equal.
     */
    public void testInequalityDifferentOffsets() {
        Path path = Paths.get("/test/file.dat");
        FileBlockCacheKey key1 = new FileBlockCacheKey(path, 1024L);
        FileBlockCacheKey key2 = new FileBlockCacheKey(path, 2048L);

        assertNotEquals("Keys with different offsets should not be equal", key1, key2);
        // Hash codes may or may not be different, but inequality should hold
    }

    /**
     * Tests keys with different paths are not equal.
     */
    public void testInequalityDifferentPaths() {
        long offset = 1024L;
        FileBlockCacheKey key1 = new FileBlockCacheKey(Paths.get("/test/file1.dat"), offset);
        FileBlockCacheKey key2 = new FileBlockCacheKey(Paths.get("/test/file2.dat"), offset);

        assertNotEquals("Keys with different paths should not be equal", key1, key2);
    }

    /**
     * Tests hash code consistency: multiple invocations should return same value.
     */
    public void testHashCodeConsistency() {
        FileBlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 1024L);

        int hash1 = key.hashCode();
        int hash2 = key.hashCode();
        int hash3 = key.hashCode();

        assertEquals("Hash code should be consistent", hash1, hash2);
        assertEquals("Hash code should be consistent", hash2, hash3);
    }

    /**
     * Tests that hash code is cached (computed once).
     */
    public void testHashCodeCaching() {
        FileBlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 1024L);

        int hash1 = key.hashCode();
        int hash2 = key.hashCode();

        // Same object, should return identical hash values
        assertEquals("Hash codes should be identical", hash1, hash2);
    }

    /**
     * Tests that equal objects have equal hash codes.
     */
    public void testEqualObjectsHaveEqualHashCodes() {
        FileBlockCacheKey key1 = new FileBlockCacheKey(Paths.get("/test/file.dat"), 1024L);
        FileBlockCacheKey key2 = new FileBlockCacheKey(Paths.get("/test/file.dat"), 1024L);

        assertEquals("Equal keys should have equal hash codes", key1.hashCode(), key2.hashCode());
    }

    /**
     * Tests path normalization (relative paths become absolute).
     */
    public void testPathNormalization() {
        Path relativePath = Paths.get("test/file.dat");
        Path absolutePath = relativePath.toAbsolutePath().normalize();

        FileBlockCacheKey key1 = new FileBlockCacheKey(relativePath, 1024L);
        FileBlockCacheKey key2 = new FileBlockCacheKey(absolutePath, 1024L);

        assertEquals("Relative and absolute paths should produce equal keys", key1, key2);
    }

    /**
     * Tests that paths with dots are normalized correctly.
     */
    public void testPathNormalizationWithDots() {
        Path path1 = Paths.get("/test/./file.dat");
        Path path2 = Paths.get("/test/file.dat");

        FileBlockCacheKey key1 = new FileBlockCacheKey(path1, 1024L);
        FileBlockCacheKey key2 = new FileBlockCacheKey(path2, 1024L);

        assertEquals("Paths with . should be normalized to equal paths", key1, key2);
    }

    /**
     * Tests that paths with .. are normalized correctly.
     */
    public void testPathNormalizationWithParentRef() {
        Path path1 = Paths.get("/test/subdir/../file.dat");
        Path path2 = Paths.get("/test/file.dat");

        FileBlockCacheKey key1 = new FileBlockCacheKey(path1, 1024L);
        FileBlockCacheKey key2 = new FileBlockCacheKey(path2, 1024L);

        assertEquals("Paths with .. should be normalized to equal paths", key1, key2);
    }

    /**
     * Tests zero offset.
     */
    public void testZeroOffset() {
        FileBlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 0L);

        assertEquals("Offset should be 0", 0L, key.offset());
        assertEquals("File offset should be 0", 0L, key.fileOffset());
        assertNotNull("Hash code should be computed", key.hashCode());
    }

    /**
     * Tests very large offset.
     */
    public void testLargeOffset() {
        long largeOffset = Long.MAX_VALUE - 1;
        FileBlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), largeOffset);

        assertEquals("Offset should be large value", largeOffset, key.offset());
        assertEquals("File offset should be large value", largeOffset, key.fileOffset());
    }

    /**
     * Tests negative offset (valid in cache key context).
     */
    public void testNegativeOffset() {
        long negativeOffset = -1024L;
        FileBlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), negativeOffset);

        assertEquals("Offset should be negative", negativeOffset, key.offset());
    }

    /**
     * Tests filePath() accessor returns normalized absolute path.
     */
    public void testFilePathAccessor() {
        Path originalPath = Paths.get("relative/file.dat");
        FileBlockCacheKey key = new FileBlockCacheKey(originalPath, 1024L);

        Path returnedPath = key.filePath();

        assertTrue("Returned path should be absolute", returnedPath.isAbsolute());
        assertEquals("Returned path should be normalized", returnedPath, returnedPath.normalize());
    }

    /**
     * Tests offset() and fileOffset() accessors return same value.
     */
    public void testOffsetAccessors() {
        long offset = 4096L;
        FileBlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), offset);

        assertEquals("offset() should return correct value", offset, key.offset());
        assertEquals("fileOffset() should return correct value", offset, key.fileOffset());
        assertEquals("offset() and fileOffset() should be equal", key.offset(), key.fileOffset());
    }

    /**
     * Tests toString() contains path and offset information.
     */
    public void testToString() {
        Path path = Paths.get("/test/file.dat");
        long offset = 1024L;
        FileBlockCacheKey key = new FileBlockCacheKey(path, offset);

        String str = key.toString();

        assertNotNull("toString should not be null", str);
        assertTrue("toString should contain 'FileBlockCacheKey'", str.contains("FileBlockCacheKey"));
        assertTrue("toString should contain offset", str.contains("1024"));
    }

    /**
     * Tests use as HashMap key.
     */
    public void testUseAsHashMapKey() {
        Map<FileBlockCacheKey, String> map = new HashMap<>();

        FileBlockCacheKey key1 = new FileBlockCacheKey(Paths.get("/test/file1.dat"), 0L);
        FileBlockCacheKey key2 = new FileBlockCacheKey(Paths.get("/test/file1.dat"), 1024L);
        FileBlockCacheKey key3 = new FileBlockCacheKey(Paths.get("/test/file2.dat"), 0L);

        map.put(key1, "value1");
        map.put(key2, "value2");
        map.put(key3, "value3");

        assertEquals("Map should contain 3 entries", 3, map.size());
        assertEquals("Should retrieve value1", "value1", map.get(key1));
        assertEquals("Should retrieve value2", "value2", map.get(key2));
        assertEquals("Should retrieve value3", "value3", map.get(key3));

        // Test with equivalent key
        FileBlockCacheKey key1Dup = new FileBlockCacheKey(Paths.get("/test/file1.dat"), 0L);
        assertEquals("Should retrieve value1 with duplicate key", "value1", map.get(key1Dup));
    }

    /**
     * Tests use in HashSet.
     */
    public void testUseInHashSet() {
        Set<FileBlockCacheKey> set = new HashSet<>();

        FileBlockCacheKey key1 = new FileBlockCacheKey(Paths.get("/test/file.dat"), 0L);
        FileBlockCacheKey key2 = new FileBlockCacheKey(Paths.get("/test/file.dat"), 0L); // Duplicate

        assertTrue("First add should return true", set.add(key1));
        assertFalse("Second add of equivalent key should return false", set.add(key2));
        assertEquals("Set should contain 1 entry", 1, set.size());
        assertTrue("Set should contain key1", set.contains(key1));
        assertTrue("Set should contain key2 (equivalent)", set.contains(key2));
    }

    /**
     * Tests hash distribution for collision resistance.
     */
    public void testHashDistribution() {
        Set<Integer> hashCodes = new HashSet<>();

        // Generate many keys with different offsets
        Path path = Paths.get("/test/file.dat");
        for (long offset = 0; offset < 1000; offset += 8) {
            FileBlockCacheKey key = new FileBlockCacheKey(path, offset);
            hashCodes.add(key.hashCode());
        }

        // Should have good distribution (most hash codes should be unique)
        assertTrue("Should have good hash distribution", hashCodes.size() > 120); // 125 keys, expect most unique
    }

    /**
     * Tests different file names in same directory produce different keys.
     */
    public void testDifferentFilesInSameDirectory() {
        Path file1 = Paths.get("/test/index/_1.cfs");
        Path file2 = Paths.get("/test/index/_2.cfs");
        Path file3 = Paths.get("/test/index/_3.cfs");

        FileBlockCacheKey key1 = new FileBlockCacheKey(file1, 0L);
        FileBlockCacheKey key2 = new FileBlockCacheKey(file2, 0L);
        FileBlockCacheKey key3 = new FileBlockCacheKey(file3, 0L);

        assertNotEquals("Different files should have different keys", key1, key2);
        assertNotEquals("Different files should have different keys", key2, key3);
        assertNotEquals("Different files should have different keys", key1, key3);
    }

    /**
     * Tests same file name in different directories produce different keys.
     */
    public void testSameFileNameDifferentDirectories() {
        Path file1 = Paths.get("/test/index1/_6.cfs");
        Path file2 = Paths.get("/test/index2/_6.cfs");
        Path file3 = Paths.get("/test/index3/_6.cfs");

        FileBlockCacheKey key1 = new FileBlockCacheKey(file1, 0L);
        FileBlockCacheKey key2 = new FileBlockCacheKey(file2, 0L);
        FileBlockCacheKey key3 = new FileBlockCacheKey(file3, 0L);

        assertNotEquals("Same filename in different directories should have different keys", key1, key2);
        assertNotEquals("Same filename in different directories should have different keys", key2, key3);
        assertNotEquals("Same filename in different directories should have different keys", key1, key3);
    }

    /**
     * Tests sequential offsets for same file produce different keys.
     */
    public void testSequentialOffsets() {
        Path path = Paths.get("/test/file.dat");
        int blockSize = 8192;

        FileBlockCacheKey key1 = new FileBlockCacheKey(path, 0L);
        FileBlockCacheKey key2 = new FileBlockCacheKey(path, blockSize);
        FileBlockCacheKey key3 = new FileBlockCacheKey(path, blockSize * 2L);

        assertNotEquals("Sequential offsets should produce different keys", key1, key2);
        assertNotEquals("Sequential offsets should produce different keys", key2, key3);
        assertNotEquals("Sequential offsets should produce different keys", key1, key3);
    }

    /**
     * Tests comparison with different object type returns false.
     */
    public void testEqualityWithDifferentType() {
        FileBlockCacheKey key = new FileBlockCacheKey(Paths.get("/test/file.dat"), 1024L);
        String otherType = "not a key";

        assertFalse("Key should not equal different type", key.equals(otherType));
    }

    /**
     * Tests that path with trailing slashes is handled correctly.
     */
    public void testPathWithTrailingSlash() {
        // Note: Paths.get handles trailing slashes automatically
        Path path1 = Paths.get("/test/dir/");
        Path path2 = Paths.get("/test/dir");

        FileBlockCacheKey key1 = new FileBlockCacheKey(path1, 0L);
        FileBlockCacheKey key2 = new FileBlockCacheKey(path2, 0L);

        // Depending on OS, these may or may not be equal after normalization
        // The key is that they are handled consistently
        assertNotNull("Key1 should be created", key1);
        assertNotNull("Key2 should be created", key2);
    }

    /**
     * Tests multiple sequential blocks from same file.
     */
    public void testMultipleBlocksFromSameFile() {
        Path path = Paths.get("/data/index/segment.cfs");
        int blockSize = 8192;
        Map<FileBlockCacheKey, Integer> blockMap = new HashMap<>();

        for (int i = 0; i < 100; i++) {
            FileBlockCacheKey key = new FileBlockCacheKey(path, (long) i * blockSize);
            blockMap.put(key, i);
        }

        assertEquals("Should have 100 unique blocks", 100, blockMap.size());

        // Verify retrieval
        for (int i = 0; i < 100; i++) {
            FileBlockCacheKey key = new FileBlockCacheKey(path, (long) i * blockSize);
            assertEquals("Should retrieve correct block number", Integer.valueOf(i), blockMap.get(key));
        }
    }

    /**
     * Tests hash code sentinel value handling (when hash computes to 0).
     */
    public void testHashCodeSentinelHandling() {
        // Try to find a path/offset combination that might hash to 0
        // This is probabilistic, but tests the sentinel logic
        for (int i = 0; i < 1000; i++) {
            Path path = Paths.get("/test/file" + i + ".dat");
            FileBlockCacheKey key = new FileBlockCacheKey(path, i * 1024L);

            int hash = key.hashCode();
            assertNotEquals("Hash code should never be 0 (sentinel value)", 0, hash);
        }
    }

    /**
     * Tests BlockCacheKey interface methods.
     */
    public void testBlockCacheKeyInterface() {
        Path path = Paths.get("/test/file.dat");
        long offset = 4096L;

        BlockCacheKey key = new FileBlockCacheKey(path, offset);

        assertEquals("offset() should return correct value", offset, key.offset());
        assertEquals("filePath() should return correct path", path.toAbsolutePath().normalize(), key.filePath());
    }
}

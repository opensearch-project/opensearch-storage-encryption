/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;

/**
 * Runs Lucene's full directory contract test suite against CryptoNIOFSDirectory.
 */
public class CryptoDirectoryTests extends OpenSearchBaseDirectoryTestCase {

    @Override
    protected Directory getDirectory(Path file) throws IOException {
        return CryptoTestDirectoryFactory.createCryptoNIOFSDirectory(file, FSLockFactory.getDefault());
    }

    @Override
    public void testCreateTempOutput() throws Throwable {
        try (Directory dir = getDirectory(createTempDir())) {
            CryptoTestDirectoryFactory.assertTempOutputRoundTrip(dir, atLeast(50), () -> newIOContext(random()));
        }
    }

    @Override
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/opensearch-storage-encryption/issues/47")
    public void testThreadSafetyInListAll() {}

    public void testRandomAccessWithCryptoOutput() throws Exception {
        try (Directory dir = getDirectory(createTempDir())) {
            String fileName = "test-random-access";
            int blockSize = 16;
            int dataSize = blockSize * 3;

            // Generate predictable random data
            byte[] testData = new byte[dataSize];
            java.util.Random rnd = new java.util.Random(42); // Fixed seed for predictability
            rnd.nextBytes(testData);

            // Write data using CryptoOutput
            try (IndexOutput output = dir.createOutput(fileName, newIOContext(random()))) {
                output.writeBytes(testData, testData.length);
            }

            // Read randomly at different positions
            try (IndexInput input = dir.openInput(fileName, newIOContext(random()))) {
                // Test reading from start
                input.seek(0);
                assertEquals(testData[0], input.readByte());

                // Test reading from middle of first block
                input.seek(8);
                assertEquals(testData[8], input.readByte());

                // Test reading from start of second block
                input.seek(blockSize);
                assertEquals(testData[blockSize], input.readByte());

                // Test reading from middle of second block
                input.seek(blockSize + 8);
                assertEquals(testData[blockSize + 8], input.readByte());

                // Test reading from start of third block
                input.seek(blockSize * 2);
                assertEquals(testData[blockSize * 2], input.readByte());

                // Test reading multiple bytes at random position
                input.seek(5);
                byte[] buffer = new byte[10];
                input.readBytes(buffer, 0, 10);
                for (int i = 0; i < 10; i++) {
                    assertEquals(testData[5 + i], buffer[i]);
                }
            }
        }
    }

    @Override
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/opensearch-storage-encryption/issues/47")
    public void testSliceOutOfBounds() {}

    // ==================== Enable/Disable Flag Tests ====================

    /**
     * Test that plugin is enabled when setting is true.
     */
    public void testPluginEnabledWhenSettingIsTrue() {
        org.opensearch.common.settings.Settings settings = org.opensearch.common.settings.Settings
            .builder()
            .put(CryptoDirectoryPlugin.CRYPTO_PLUGIN_ENABLED, true)
            .build();
        CryptoDirectoryPlugin plugin = new CryptoDirectoryPlugin(settings);
        assertFalse("Plugin should not be disabled when enabled setting is true", plugin.isDisabled());
    }

    /**
     * Test that plugin is disabled by default.
     */
    public void testPluginDisabledByDefault() {
        CryptoDirectoryPlugin plugin = new CryptoDirectoryPlugin(org.opensearch.common.settings.Settings.EMPTY);
        assertTrue("Plugin should be disabled by default", plugin.isDisabled());
    }

    /**
     * Test that no directory factories are registered when plugin is disabled.
     */
    public void testNoDirectoryFactoriesWhenDisabled() {
        org.opensearch.common.settings.Settings settings = org.opensearch.common.settings.Settings
            .builder()
            .put(CryptoDirectoryPlugin.CRYPTO_PLUGIN_ENABLED, false)
            .build();
        CryptoDirectoryPlugin plugin = new CryptoDirectoryPlugin(settings);
        assertTrue("Directory factories should be empty when disabled", plugin.getDirectoryFactories().isEmpty());
    }

    /**
     * Test that directory factory is registered when plugin is enabled.
     */
    public void testDirectoryFactoryRegisteredWhenEnabled() {
        org.opensearch.common.settings.Settings settings = org.opensearch.common.settings.Settings
            .builder()
            .put(CryptoDirectoryPlugin.CRYPTO_PLUGIN_ENABLED, true)
            .build();
        CryptoDirectoryPlugin plugin = new CryptoDirectoryPlugin(settings);
        assertFalse("Directory factories should not be empty when enabled", plugin.getDirectoryFactories().isEmpty());
        assertNotNull("CryptoFS factory should be registered", plugin.getDirectoryFactories().get("cryptofs"));
    }

    /**
     * Test that enabled setting is included in plugin settings.
     */
    public void testEnabledSettingIncluded() {
        CryptoDirectoryPlugin plugin = new CryptoDirectoryPlugin(org.opensearch.common.settings.Settings.EMPTY);
        assertTrue(
            "Settings should contain enabled setting",
            plugin.getSettings().contains(CryptoDirectoryPlugin.CRYPTO_PLUGIN_ENABLED_SETTING)
        );
    }

    /**
     * Test that enabled setting has correct default value (false - disabled by default).
     */
    public void testEnabledSettingDefault() {
        assertEquals(
            "Enabled setting default should be false (disabled by default)",
            Boolean.FALSE,
            CryptoDirectoryPlugin.CRYPTO_PLUGIN_ENABLED_SETTING.getDefault(org.opensearch.common.settings.Settings.EMPTY)
        );
    }
}

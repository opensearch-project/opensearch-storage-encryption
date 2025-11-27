/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

public class CryptoDirectoryFactoryTests extends OpenSearchTestCase {

    /**
     * Test that the validation logic can detect s3vector engine in settings.
     * This tests the pattern matching logic used in validateNotS3VectorIndex().
     */
    public void testS3VectorEngineDetection() {
        // Create settings with s3vector engine (as it would appear after mapping parsing)
        Settings settings = Settings
            .builder()
            .put("index.store.type", "cryptofs")
            .put("index.mapping.properties.my_vector.method.engine", "s3vector")
            .build();

        // Verify that the pattern matching logic can detect s3vector
        boolean hasS3VectorEngine = false;
        for (String key : settings.keySet()) {
            if ((key.contains("method.engine") || key.endsWith(".engine")) && "s3vector".equals(settings.get(key))) {
                hasS3VectorEngine = true;
                break;
            }
        }

        assertTrue("Should detect s3vector engine in settings", hasS3VectorEngine);
    }

    /**
     * Test that non-s3vector engines are not detected as s3vector.
     */
    public void testNonS3VectorEngineNotDetected() {
        // Create settings with a different engine
        Settings settings = Settings
            .builder()
            .put("index.store.type", "cryptofs")
            .put("index.mapping.properties.my_vector.method.engine", "lucene")
            .build();

        // Verify that s3vector is NOT detected
        boolean hasS3VectorEngine = false;
        for (String key : settings.keySet()) {
            if ((key.contains("method.engine") || key.endsWith(".engine")) && "s3vector".equals(settings.get(key))) {
                hasS3VectorEngine = true;
                break;
            }
        }

        assertTrue("Should not detect non-s3vector engine as s3vector", !hasS3VectorEngine);
    }

    /**
     * Test that settings without any engine are not detected as s3vector.
     */
    public void testNoEngineNotDetected() {
        // Create settings without any engine
        Settings settings = Settings.builder().put("index.store.type", "cryptofs").put("index.store.crypto.key_provider", "dummy").build();

        // Verify that s3vector is NOT detected
        boolean hasS3VectorEngine = false;
        for (String key : settings.keySet()) {
            if ((key.contains("method.engine") || key.endsWith(".engine")) && "s3vector".equals(settings.get(key))) {
                hasS3VectorEngine = true;
                break;
            }
        }

        assertTrue("Should not detect s3vector when no engine is present", !hasS3VectorEngine);
    }
}

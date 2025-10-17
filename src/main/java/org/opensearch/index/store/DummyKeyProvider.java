/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import java.util.Collections;
import java.util.Map;

import org.opensearch.common.Randomness;
import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;

/**
 * Utility class providing a dummy MasterKeyProvider implementation for testing.
 * This is used by yamlRestTests and integration tests to avoid requiring
 * a real KMS plugin during testing.
 * 
 * <p><b>WARNING:</b> This is for testing purposes only and should never be 
 * used in production environments. The dummy provider:
 * <ul>
 * <li>Generates random keys without actual encryption</li>
 * <li>Returns encrypted keys as-is during decryption (no actual decryption)</li>
 * <li>Provides no real security</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class DummyKeyProvider {

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private DummyKeyProvider() {
        throw new AssertionError("Utility class should not be instantiated");
    }

    /**
     * Creates a dummy MasterKeyProvider for testing purposes.
     * This provider generates random keys and returns encrypted keys as-is.
     * 
     * @return a mock MasterKeyProvider suitable for testing
     */
    public static MasterKeyProvider create() {
        return new MasterKeyProvider() {
            @Override
            public DataKeyPair generateDataPair() {
                byte[] rawKey = new byte[32];
                byte[] encryptedKey = new byte[32];
                java.util.Random rnd = Randomness.get();
                rnd.nextBytes(rawKey);
                rnd.nextBytes(encryptedKey);
                return new DataKeyPair(rawKey, encryptedKey);
            }

            @Override
            public byte[] decryptKey(byte[] encryptedKey) {
                // For mock/testing purposes, just return the input as-is
                return encryptedKey;
            }

            @Override
            public String getKeyId() {
                return "builtin-mock-key-id";
            }

            @Override
            public Map<String, String> getEncryptionContext() {
                return Collections.emptyMap();
            }

            @Override
            public void close() {
                // Nothing to close for mock implementation
            }
        };
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.util.Collections;
import java.util.Map;
import java.util.Random;

import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;

/**
 * A deterministic {@link MasterKeyProvider} for JMH benchmarks.
 *
 * <p>Unlike {@code DummyKeyProvider}, this avoids {@code Randomness.get()} which
 * requires the {@code RandomizedRunner} test context (not available in JMH).
 * Uses a fixed-seed {@link Random} for reproducible key generation.
 */
public final class BenchmarkKeyProvider {

    private BenchmarkKeyProvider() {}

    public static MasterKeyProvider create() {
        return new MasterKeyProvider() {
            private final Random rng = new Random(0xBE4C8L);

            @Override
            public DataKeyPair generateDataPair() {
                byte[] rawKey = new byte[32];
                byte[] encryptedKey = new byte[32];
                rng.nextBytes(rawKey);
                rng.nextBytes(encryptedKey);
                return new DataKeyPair(rawKey, encryptedKey);
            }

            @Override
            public byte[] decryptKey(byte[] encryptedKey) {
                // Identity function — benchmarks don't need real decryption
                return encryptedKey;
            }

            @Override
            public String getKeyId() {
                return "benchmark-key-id";
            }

            @Override
            public Map<String, String> getEncryptionContext() {
                return Collections.emptyMap();
            }

            @Override
            public void close() {}
        };
    }
}

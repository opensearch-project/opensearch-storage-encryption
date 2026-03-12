/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import java.security.Key;

import javax.crypto.spec.SecretKeySpec;

import org.opensearch.index.store.key.KeyResolver;

/**
 * A test-only {@link KeyResolver} that returns a deterministic AES key
 * without requiring {@code NodeLevelKeyCache} or a real {@code MasterKeyProvider}.
 *
 * <p>This avoids the full node initialization that {@code DefaultKeyResolver} requires,
 * making it suitable for unit tests that need a real {@code BufferPoolDirectory}.
 */
public class TestKeyResolver implements KeyResolver {

    private final Key dataKey;

    /**
     * Creates a TestKeyResolver with a deterministic 32-byte AES key derived from a fixed seed.
     */
    public TestKeyResolver() {
        // Deterministic weak key for testing only — NOT for production use
        java.util.Random rng = new java.util.Random(0xDEADBEEFL);
        byte[] keyBytes = new byte[32];
        rng.nextBytes(keyBytes);
        this.dataKey = new SecretKeySpec(keyBytes, "AES");
    }

    @Override
    public Key getDataKey() {
        return dataKey;
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.cipher;

import java.security.Provider;

import javax.crypto.Cipher;

/**
 * Enumeration of supported encryption algorithms
 *
 * @opensearch.internal
 */
public enum EncryptionAlgorithm {

    AES_256_GCM_CTR((short) 1, "AES-256-GCM-CTR");

    private final short algorithmId;
    private final String algorithmName;

    EncryptionAlgorithm(short algorithmId, String algorithmName) {
        this.algorithmId = algorithmId;
        this.algorithmName = algorithmName;
    }

    public short getAlgorithmId() {
        return algorithmId;
    }

    public String getAlgorithmName() {
        return algorithmName;
    }

    /**
     * Get encryption cipher for this algorithm
     */
    public Cipher getEncryptionCipher(Provider provider) {
        switch (this) {
            case AES_256_GCM_CTR:
                return AesGcmCipherFactory.getCipher(provider);
            default:
                throw new IllegalArgumentException("Unsupported algorithm: " + this);
        }
    }

    /**
     * Get a pooled CTR decryption cipher for this algorithm using the given provider.
     *
     * @param provider the JCE provider to use
     * @return a pooled AES/CTR cipher bound to {@code provider}
     */
    public Cipher getDecryptionCipher(Provider provider) {
        switch (this) {
            case AES_256_GCM_CTR:
                return AesCipherFactory.getPooledCipher(provider);
            default:
                throw new IllegalArgumentException("Unsupported algorithm: " + this);
        }
    }

    /**
     * Get algorithm by ID
     */
    public static EncryptionAlgorithm fromId(short algorithmId) {
        for (EncryptionAlgorithm algo : values()) {
            if (algo.algorithmId == algorithmId) {
                return algo;
            }
        }
        throw new IllegalArgumentException("Unknown algorithm ID: " + algorithmId);
    }
}

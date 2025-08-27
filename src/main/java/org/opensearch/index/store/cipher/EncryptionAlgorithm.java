/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.cipher;

import javax.crypto.Cipher;
import java.security.Provider;

/**
 * Enumeration of supported encryption algorithms
 *
 * @opensearch.internal
 */
public enum EncryptionAlgorithm {
    
    AES_256_GCM_CTR((short)1, "AES-256-GCM-CTR");
    
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
     * Get decryption cipher for this algorithm
     */
    public Cipher getDecryptionCipher() {
        switch (this) {
            case AES_256_GCM_CTR:
                return AesCipherFactory.CIPHER_POOL.get(); // CTR for reading
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
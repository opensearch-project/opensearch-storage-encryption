/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.iv;

import java.security.Key;

/**
 * An abstraction for resolving the symmetric encryption key and initialization vector (IV)
 * used for encrypting and decrypting index files in an OpenSearch Directory implementation.
 *
 * Implementations of this interface are responsible for securely retrieving or generating
 * the key and IV used in symmetric encryption (e.g., AES-CTR).
 *
 * @opensearch.internal
 */
public interface KeyIvResolver {

    /**
     * Returns the symmetric encryption key used for cipher operations.
     *
     * @return the decrypted symmetric {@link Key}, typically AES
     */
    Key getDataKey();

    /**
     * Returns the raw initialization vector (IV) used with the cipher.
     * The IV should typically be 12 or 16 bytes, depending on the encryption mode.
     *
     * @return the IV as a byte array
     */
    byte[] getIvBytes();
}

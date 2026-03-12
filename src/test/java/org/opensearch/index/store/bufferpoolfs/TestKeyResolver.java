/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import java.security.Key;
import java.security.Provider;

import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.store.Directory;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.index.store.key.DefaultKeyResolver;
import org.opensearch.index.store.key.KeyCacheException;

public class TestKeyResolver extends DefaultKeyResolver {
    /**
     * Constructs a new {@link DefaultKeyResolver} and ensures the key is initialized.
     *
     * @param indexUuid   the unique identifier for the index
     * @param indexName   the index name
     * @param directory   the Lucene directory to read/write metadata files
     * @param provider    the JCE provider used for cipher operations
     * @param keyProvider the master key provider used to encrypt/decrypt data keys
     * @param shardId     the shard ID
     * @throws KeyCacheException if an I/O error occurs while reading or writing key metadata
     */
    public TestKeyResolver(
        String indexUuid,
        String indexName,
        Directory directory,
        Provider provider,
        MasterKeyProvider keyProvider,
        int shardId
    )
        throws KeyCacheException {
        super(indexUuid, indexName, directory, provider, keyProvider, shardId);
    }

    @Override
    public Key getDataKey() {
        byte[] masterKey = new byte[32];
        for (int i = 0; i < 32; i++) {
            masterKey[i] = (byte) (i % 2);
        }
        return new SecretKeySpec(masterKey, "AES");
    }
}

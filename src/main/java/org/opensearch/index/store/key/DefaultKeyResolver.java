/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.key;

import java.io.IOException;
import java.security.Key;
import java.security.Provider;

import javax.crypto.spec.SecretKeySpec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.metrics.ErrorType;

/**
 * Default implementation of {@link KeyResolver} responsible for managing
 * the encryption key used in encrypting and decrypting Lucene index files.
 *
 * Uses node-level cache for TTL-based key management with automatic refresh.
 * Always returns the last known key if Master Key Provider is unavailable to ensure operations can continue.
 *
 * Metadata files:
 * - "keyfile" stores the encrypted data key
 * - IVs are derived using HKDF
 *
 * @opensearch.internal
 */
public class DefaultKeyResolver implements KeyResolver {
    private static final Logger LOGGER = LogManager.getLogger(DefaultKeyResolver.class);

    private final String indexUuid;
    private final Directory directory;
    private final MasterKeyProvider keyProvider;
    private final int shardId;

    private Key dataKey;

    private static final String KEY_FILE = "keyfile";

    /**
     * Constructs a new {@link DefaultKeyResolver} and ensures the key is initialized.
     *
     * @param indexUuid   the unique identifier for the index
     * @param directory   the Lucene directory to read/write metadata files
     * @param provider    the JCE provider used for cipher operations
     * @param keyProvider the master key provider used to encrypt/decrypt data keys
     * @param shardId
     * @throws IOException if an I/O error occurs while reading or writing key metadata
     */
    public DefaultKeyResolver(String indexUuid, Directory directory, Provider provider, MasterKeyProvider keyProvider, int shardId)
        throws IOException {
        this.indexUuid = indexUuid;
        this.directory = directory;
        this.keyProvider = keyProvider;
        this.shardId = shardId;
        initialize(shardId);
    }

    /**
     * Attempts to load the encrypted key from the directory.
     * If not present, it generates and persists new values.
     */
    private void initialize(int shardId) throws IOException {
        try {
            dataKey = new SecretKeySpec(keyProvider.decryptKey(readByteArrayFile(KEY_FILE)), "AES");
        } catch (java.nio.file.NoSuchFileException e) {
            try {
                initNewKey(shardId);
            } catch (Exception ex) {
                throw new IOException("Failed to initialize key for index: " + indexUuid, ex);
            }
        } catch (Exception e) {
            CryptoMetricsService.getInstance().recordError(ErrorType.KMS_KEY_ERROR, this.indexUuid);
            throw new IOException("Failed to initialize key for index: " + indexUuid, e);
        }
    }

    private void initNewKey(int shardId) throws IOException {
        DataKeyPair pair = keyProvider.generateDataPair();
        byte[] masterKey = pair.getRawKey();
        byte[] indexKey = HkdfKeyDerivation.deriveIndexKey(masterKey, indexUuid);
        byte[] directoryKey = HkdfKeyDerivation.deriveDirectoryKey(indexKey, shardId);
        dataKey = new SecretKeySpec(directoryKey, "AES");
        writeByteArrayFile(KEY_FILE, pair.getEncryptedKey());
    }

    /**
     * Reads a byte array from the specified file in the directory.
     */
    private byte[] readByteArrayFile(String fileName) throws IOException {
        try (IndexInput in = directory.openInput(fileName, IOContext.READONCE)) {
            int size = in.readInt();
            byte[] bytes = new byte[size];
            in.readBytes(bytes, 0, size);
            return bytes;
        }
    }

    /**
     * Writes a byte array to the specified file in the directory.
     */
    private void writeByteArrayFile(String fileName, byte[] data) throws IOException {
        try (IndexOutput out = directory.createOutput(fileName, IOContext.DEFAULT)) {
            out.writeInt(data.length);
            out.writeBytes(data, 0, data.length);
        }
    }

    /**
     * Loads key from Master Key provider by decrypting the stored encrypted key.
     * This method is called by the node-level cache.
     * Exceptions are allowed to bubble up - the cache will handle fallback to old value.
     */
    Key loadKeyFromMasterKeyProvider() throws Exception {
        // Attempt decryption
        try {
            byte[] encryptedKey = readByteArrayFile(KEY_FILE);
            byte[] masterKey = keyProvider.decryptKey(encryptedKey);
            byte[] indexKey = HkdfKeyDerivation.deriveIndexKey(masterKey, indexUuid);
            byte[] directoryKey = HkdfKeyDerivation.deriveDirectoryKey(indexKey, shardId);
            return new SecretKeySpec(directoryKey, "AES");
        } catch (Exception e) {
            CryptoMetricsService.getInstance().recordError(ErrorType.KMS_KEY_ERROR, this.indexUuid);
            throw e;
        }

    }

    /**
     * {@inheritDoc}
     * Returns the data key for all operations.
     * The cache handles MasterKey Provider failures by returning the last known key.
     * Passes this resolver directly to the cache to eliminate registry lookup race conditions.
     */
    @Override
    public Key getDataKey() {
        try {
            return NodeLevelKeyCache.getInstance().get(indexUuid, shardId);
        } catch (Exception ex) {
            throw new RuntimeException("No Node Level Key Cache available for {}", ex);
        }

    }

}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.key;

import java.io.IOException;
import java.security.Key;
import java.security.Provider;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.spec.SecretKeySpec;

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

    private final String indexUuid;
    private final String indexName;
    private final Directory directory;
    private final MasterKeyProvider keyProvider;
    private final int shardId;

    private static final String KEY_FILE = "keyfile";

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
    public DefaultKeyResolver(
        String indexUuid,
        String indexName,
        Directory directory,
        Provider provider,
        MasterKeyProvider keyProvider,
        int shardId
    )
        throws KeyCacheException {
        this.indexUuid = indexUuid;
        this.indexName = indexName;
        this.directory = directory;
        this.keyProvider = keyProvider;
        this.shardId = shardId;
        initialize(shardId);
    }

    /**
     * Gets the index name for this resolver.
     * 
     * @return the index name
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * Attempts to load the encrypted key from the directory.
     * If not present, it generates and persists new values.
     */
    private void initialize(int shardId) throws KeyCacheException {
        try {
            keyProvider.decryptKey(readByteArrayFile(KEY_FILE));
        } catch (java.nio.file.NoSuchFileException e) {
            // Key file doesn't exist, generate new one
            try {
                initNewKey(shardId);
            } catch (Exception ex) {
                CryptoMetricsService.getInstance().recordError(ErrorType.KMS_KEY_ERROR, getMetricKey(ex));
                String rootCause = KeyCacheException.extractRootCauseMessage(ex);
                throw new KeyCacheException(
                    "Error encountered for index '" + indexName + "' (UUID: " + indexUuid + "): " + rootCause,
                    ex,
                    true  // suppress stack trace
                );
            }
        } catch (Exception e) {
            CryptoMetricsService.getInstance().recordError(ErrorType.KMS_KEY_ERROR, getMetricKey(e));
            String rootCause = KeyCacheException.extractRootCauseMessage(e);
            throw new KeyCacheException("Error encountered for index '" + indexName + "' (UUID: " + indexUuid + "): " + rootCause, e, true);
        }
    }

    private void initNewKey(int shardId) throws IOException {
        DataKeyPair pair = keyProvider.generateDataPair();
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
     * Loads master key from Master Key provider by decrypting the stored encrypted key.
     * This method is called by the node-level cache.
     * Exceptions are allowed to bubble up - the cache will handle fallback to old value.
     */
    Key loadKeyFromMasterKeyProvider() throws Exception {
        // Attempt decryption
        try {
            byte[] encryptedKey = readByteArrayFile(KEY_FILE);
            byte[] masterKey = keyProvider.decryptKey(encryptedKey);
            return new SecretKeySpec(masterKey, "AES");
        } catch (Exception e) {
            CryptoMetricsService.getInstance().recordError(ErrorType.KMS_KEY_ERROR, getMetricKey(e));
            throw e;
        }

    }

    /**
     * {@inheritDoc}
     * Returns the master key. File-specific keys are derived on-demand from master key + messageId.
     * The cache handles MasterKey Provider failures by returning the last known key.
     */
    @Override
    public Key getDataKey() {
        try {
            return NodeLevelKeyCache.getInstance().get(indexUuid, shardId, indexName);
        } catch (Exception e) {
            // If it's already a KeyCacheException with clean message, just rethrow
            if (e instanceof KeyCacheException) {
                throw (KeyCacheException) e;
            }
            // Only wrap unexpected exceptions
            throw new KeyCacheException("Failed to get encryption key for index: " + indexName, e, true);
        }
    }

    private String getMetricKey(Exception e) {
        String kmsKey = extractKmsKey(e);
        return this.indexName + ":" + kmsKey;
    }

    private String extractKmsKey(Exception e) {
        String message = e.getMessage();
        if (message != null) {
            Pattern pattern = Pattern.compile("arn:aws:kms:[^:]+:[^:]+:key/([^\\s]+)");
            Matcher matcher = pattern.matcher(message);
            if (matcher.find()) {
                return matcher.group(1); // Just the key ID, not full ARN
            }
        }
        return "unknown";
    }

}

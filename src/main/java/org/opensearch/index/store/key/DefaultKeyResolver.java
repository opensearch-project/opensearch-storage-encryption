/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.key;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.security.Key;
import java.security.Provider;
import java.util.List;

import javax.crypto.spec.SecretKeySpec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
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

    private static final Logger logger = LogManager.getLogger(DefaultKeyResolver.class);

    private final String indexUuid;
    private final String indexName;
    private volatile Directory directory;
    private final MasterKeyProvider keyProvider;
    private final int shardId;

    /**
     * The encrypted data key this resolver read from or wrote to the keyfile. Kept so the keyfile can be
     * written back if the node deletes the index directory and later receives a shard of the same index.
     */
    private volatile byte[] encryptedKey;

    private static final String KEY_FILE = "keyfile";
    private static final String KEY_FILE_TEMP_SUFFIX = "tmp";

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
            byte[] storedKey = readByteArrayFile(KEY_FILE);
            keyProvider.decryptKey(storedKey);
            encryptedKey = storedKey;
        } catch (java.nio.file.NoSuchFileException e) {
            // Key file doesn't exist, generate new one
            try {
                initNewKey(shardId);
            } catch (Exception ex) {
                CryptoMetricsService.getInstance().recordError(ErrorType.KMS_KEY_ERROR, indexName);
                String rootCause = KeyCacheException.extractRootCauseMessage(ex);
                throw new KeyCacheException(
                    "Error encountered for index '" + indexName + "' (UUID: " + indexUuid + "): " + rootCause,
                    ex,
                    true  // suppress stack trace
                );
            }
        } catch (Exception e) {
            CryptoMetricsService.getInstance().recordError(ErrorType.KMS_KEY_ERROR, indexName);
            String rootCause = KeyCacheException.extractRootCauseMessage(e);
            throw new KeyCacheException("Error encountered for index '" + indexName + "' (UUID: " + indexUuid + "): " + rootCause, e, true);
        }
    }

    private void initNewKey(int shardId) throws IOException {
        DataKeyPair pair = keyProvider.generateDataPair();
        writeByteArrayFile(directory, KEY_FILE, pair.getEncryptedKey());
        encryptedKey = pair.getEncryptedKey();
    }

    /**
     * Makes sure this resolver reads its keyfile from a directory that has one.
     *
     * <p>When the last shard of an index leaves a node, OpenSearch deletes the index directory, keyfile
     * included, but this resolver stays registered. If a shard of the index is later allocated to the node
     * again, the registry hands back this resolver, so {@link #initialize(int)} does not run again and every
     * key load would fail on the missing keyfile. In that case this writes the same encrypted data key into
     * the directory the new shard uses (when it has no keyfile) and reads from that directory from then on.
     * Keeping the same data key keeps the key the node already has cached, and any files it wrote with it,
     * valid.
     *
     * <p>Does nothing while this resolver's keyfile exists, or when {@code shardIndexDirectory} does not exist.
     *
     * @param shardIndexDirectory the index-level directory of the shard being created
     * @return true if this resolver moved to {@code shardIndexDirectory}
     * @throws IOException if the keyfile cannot be written
     */
    boolean ensureKeyFile(Directory shardIndexDirectory) throws IOException {
        if (keyFileExists(directory)) {
            return false;
        }
        if (shardIndexDirectory instanceof FSDirectory fsDirectory && Files.isDirectory(fsDirectory.getDirectory()) == false) {
            return false;
        }
        byte[] key = encryptedKey;
        if (key == null) {
            return false;
        }
        if (keyFileExists(shardIndexDirectory) == false) {
            writeByteArrayFile(shardIndexDirectory, KEY_FILE, key);
            logger.info("Restored missing keyfile for index {} (UUID: {}), shard {}", indexName, indexUuid, shardId);
        }
        directory = shardIndexDirectory;
        return true;
    }

    private static boolean keyFileExists(Directory dir) throws IOException {
        try {
            dir.fileLength(KEY_FILE);
            return true;
        } catch (NoSuchFileException | FileNotFoundException e) {
            return false;
        }
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
     * Durably and atomically writes a byte array to the specified file in the directory:
     * the data goes to a temp file which is fsynced, then renamed over the target, and the
     * directory metadata is fsynced so the rename survives a crash.
     */
    private static void writeByteArrayFile(Directory directory, String fileName, byte[] data) throws IOException {
        String tempName = null;
        boolean success = false;
        try {
            try (IndexOutput out = directory.createTempOutput(fileName, KEY_FILE_TEMP_SUFFIX, IOContext.DEFAULT)) {
                tempName = out.getName();
                out.writeInt(data.length);
                out.writeBytes(data, 0, data.length);
            }
            directory.sync(List.of(tempName));
            directory.rename(tempName, fileName);
            directory.syncMetaData();
            success = true;
        } finally {
            if (success == false && tempName != null) {
                IOUtils.deleteFilesIgnoringExceptions(directory, tempName);
            }
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
            CryptoMetricsService.getInstance().recordError(ErrorType.KMS_KEY_ERROR, indexName);
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

}

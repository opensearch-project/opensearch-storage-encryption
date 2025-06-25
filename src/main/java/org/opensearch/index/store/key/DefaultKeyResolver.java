/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.key;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.security.Key;
import java.security.Provider;
import java.util.concurrent.ConcurrentHashMap;

import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;

public class DefaultKeyResolver implements KeyResolver {

    private final Directory directory;
    private final MasterKeyProvider keyProvider;

    /**
     * Per-directory in-memory cache of file encryption keys.
     * <p>
     * This map stores the derived encryption key for each file name (typically segment files) within the current
     * {@link Directory}. This avoids re-reading the enc key trailer from disk repeatedly for the same file.
     * </p>
     * <p>
     * Keys are populated during the first call to {@link #getFileEncryptionKey(Path, String)} and are removed
     * if {@link #invalidateFileEncrytionKeyStore(String)} is called.
     * </p>
     */

    /// TODO include file creation time as the cache key for stronger gurantees in case of file name.
    private final ConcurrentHashMap<String, Key> fileEncryptionKeyStore;

    private Key dataKey;

    private static final String KEY_FILE = "enc.data.key";

    public DefaultKeyResolver(Directory directory, Provider provider, MasterKeyProvider keyProvider) throws IOException {
        this.directory = directory;
        this.keyProvider = keyProvider;
        this.fileEncryptionKeyStore = new ConcurrentHashMap<>();
        initialize();
    }

    private void initialize() throws IOException {
        try {
            dataKey = new SecretKeySpec(keyProvider.decryptKey(readByteArrayFile(KEY_FILE)), "AES");
        } catch (java.nio.file.NoSuchFileException e) {
            initNewDataKey();
        }
    }

    private void initNewDataKey() throws IOException {
        DataKeyPair pair = keyProvider.generateDataPair();
        dataKey = new SecretKeySpec(pair.getRawKey(), "AES");
        writeByteArrayFile(KEY_FILE, pair.getEncryptedKey());
    }

    private byte[] readByteArrayFile(String fileName) throws IOException {
        try (IndexInput in = directory.openInput(fileName, IOContext.READONCE)) {
            int size = in.readInt();
            byte[] bytes = new byte[size];
            in.readBytes(bytes, 0, size);
            return bytes;
        }
    }

    private void writeByteArrayFile(String fileName, byte[] data) throws IOException {
        try (IndexOutput out = directory.createOutput(fileName, IOContext.DEFAULT)) {
            out.writeInt(data.length);
            out.writeBytes(data, 0, data.length);
        }
    }

    @Override
    public Key getDataKey() {
        return dataKey;
    }

    @Override
    public Key deriveNewFileEncryptionKey(String name) throws IOException {
        Key key = FileEncryptionKeyResolver.generateEncryptionKey(dataKey);
        fileEncryptionKeyStore.put(name, key);
        return key;
    }

    @Override
    public Key getFileEncryptionKey(Path filePath, String name) throws IOException {
        try {
            return fileEncryptionKeyStore.computeIfAbsent(name, n -> {
                try {
                    return FileEncryptionKeyResolver.readEncryptionKey(filePath);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    @Override
    public void invalidateFileEncrytionKeyStore(String name) {
        fileEncryptionKeyStore.remove(name);
    }
}

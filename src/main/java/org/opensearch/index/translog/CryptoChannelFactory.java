/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.translog;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.opensearch.index.store.key.KeyResolver;

/**
 * A ChannelFactory implementation that creates FileChannels with transparent
 * AES-CTR encryption/decryption for translog files.
 *
 * This factory determines whether to apply encryption based on the file extension:
 * - .tlog files: Encrypted using AES-256-CTR keyed by file offset (see TranslogChunkManager)
 * - .ckp files: Not encrypted (checkpoint metadata)
 *
 * Updated to use unified KeyResolver (same as index files) for consistent
 * key management across all encrypted components.
 *
 * The factory also tracks the current writer's wrapper to enable cipher finalization
 * before remote upload (decrypt-before-upload flow).
 *
 * @opensearch.internal
 */
public class CryptoChannelFactory implements ChannelFactory {

    private final KeyResolver keyResolver;
    private final String translogUUID;
    private final Map<Path, CryptoFileChannelWrapper> wrappers = new ConcurrentHashMap<>();

    /**
     * Creates a new CryptoChannelFactory.
     *
     * @param keyResolver the key and IV resolver for encryption keys (unified with index files)
     * @param translogUUID the translog UUID for exact header size calculation
     */
    public CryptoChannelFactory(KeyResolver keyResolver, String translogUUID) {
        if (translogUUID == null) {
            throw new IllegalArgumentException("translogUUID is required for exact header size calculation");
        }
        this.keyResolver = keyResolver;
        this.translogUUID = translogUUID;
    }

    @Override
    public FileChannel open(Path path, OpenOption... options) throws IOException {
        FileChannel baseChannel = FileChannel.open(path, options);

        if (!path.getFileName().toString().endsWith(".tlog")) {
            return baseChannel;
        }

        Set<OpenOption> optionsSet = Set.of(options);
        CryptoFileChannelWrapper wrapper = new CryptoFileChannelWrapper(baseChannel, keyResolver, path, optionsSet, translogUUID);

        // Track wrapper by path for later finalization
        wrappers.put(path, wrapper);
        return wrapper;
    }

    /**
     * Finalizes the cipher for a specific file path.
     * This writes authentication tags to disk so the file can be decrypted.
     * 
     * This is for the decrypt-before-upload flow:
     * 1. Called before upload for the specific file being uploaded
     * 2. Writes authentication tags to complete encryption
     * 3. Enables successful decryption during snapshot read
     * 
     * @param path the path of the file to finalize
     * @throws IOException if finalization fails
     */
    public void finalizeForPath(Path path) throws IOException {
        CryptoFileChannelWrapper wrapper = wrappers.get(path);
        if (wrapper != null) {
            wrapper.getChunkManager().close();
        }
    }

    /**
     * Removes a wrapper from tracking when it's no longer needed.
     * 
     * @param path the path of the wrapper to remove
     */
    public void removeWrapper(Path path) {
        wrappers.remove(path);
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.translog;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.opensearch.index.store.iv.KeyIvResolver;

/**
 * A ChannelFactory implementation that creates FileChannels with transparent
 * AES-GCM encryption/decryption for translog files.
 *
 * This factory determines whether to apply encryption based on the file extension:
 * - .tlog files: Encrypted using AES-GCM with 8KB authenticated chunks
 * - .ckp files: Not encrypted (checkpoint metadata)
 *
 * Updated to use unified KeyIvResolver (same as index files) for consistent
 * key management across all encrypted components.
 *
 * @opensearch.internal
 */
public class CryptoChannelFactory implements ChannelFactory {

    private final KeyIvResolver keyIvResolver;
    private final String translogUUID;

    /**
     * Creates a new CryptoChannelFactory.
     *
     * @param keyIvResolver the key and IV resolver for encryption keys (unified with index files)
     * @param translogUUID the translog UUID for exact header size calculation
     */
    public CryptoChannelFactory(KeyIvResolver keyIvResolver, String translogUUID) {
        if (translogUUID == null) {
            throw new IllegalArgumentException("translogUUID is required for exact header size calculation");
        }
        this.keyIvResolver = keyIvResolver;
        this.translogUUID = translogUUID;
    }

    @Override
    public FileChannel open(Path path, OpenOption... options) throws IOException {
        // Create the base FileChannel
        FileChannel baseChannel = FileChannel.open(path, options);

        // Determine if this file should be encrypted
        String fileName = path.getFileName().toString();
        boolean shouldEncrypt = fileName.endsWith(".tlog");

        if (shouldEncrypt) {
            // Wrap with crypto functionality using unified key resolver and exact UUID
            Set<OpenOption> optionsSet = new HashSet<>(Arrays.asList(options));
            return new CryptoFileChannelWrapper(baseChannel, keyIvResolver, path, optionsSet, translogUUID);
        } else {
            // Return unwrapped channel for non-encrypted files
            return baseChannel;
        }
    }

    /**
     * Gets the key IV resolver used by this factory.
     *
     * @return the key IV resolver
     */
    public KeyIvResolver getKeyIvResolver() {
        return keyIvResolver;
    }
}

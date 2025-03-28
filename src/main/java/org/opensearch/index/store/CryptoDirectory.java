/* * SPDX-License-Identifier: Apache-2.0 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.Randomness;
import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.util.io.IOUtils;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A hybrid directory implementation that encrypts files
 * to be stored based on a user supplied key
 *
 * @opensearch.internal
 */
public final class CryptoDirectory extends NIOFSDirectory {

    private Path location;
    private Key dataKey;
    private String iv;
    private final Provider provider;

    private final AtomicLong nextTempFileCounter = new AtomicLong();

    CryptoDirectory(LockFactory lockFactory, Path location, Provider provider, MasterKeyProvider keyProvider) throws IOException {
        super(location, lockFactory);
        this.location = location;
        this.provider = provider;

        try {
            // Load existing IV and key
            try (IndexInput in = super.openInput("ivFile", new IOContext())) {
                iv = in.readString();
            }
            dataKey = new SecretKeySpec(keyProvider.decryptKey(getWrappedKey()), "AES");
        } catch (java.nio.file.NoSuchFileException nsfe) {
            // Initialize new IV and key
            initializeNewIvAndKey(keyProvider);
        } catch (Exception e) {
            throw new RuntimeException("Unexpected error initializing CryptoDirectory", e);
        }
    }

    private void initializeNewIvAndKey(MasterKeyProvider keyProvider) throws IOException {
        DataKeyPair dataKeyPair = keyProvider.generateDataPair();
        dataKey = new SecretKeySpec(dataKeyPair.getRawKey(), "AES");
        storeWrappedKey(dataKeyPair.getEncryptedKey());

        // Generate new IV
        SecureRandom random = Randomness.createSecure();
        byte[] ivBytes = new byte[CipherFactory.IV_ARRAY_LENGTH];
        random.nextBytes(ivBytes);
        iv = Base64.getEncoder().encodeToString(ivBytes);
        storeIV();
    }

    private void storeIV() throws IOException {
        try (IndexOutput out = super.createOutput("ivFile", new IOContext())) {
            out.writeString(iv);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void storeWrappedKey(byte[] wrappedKey) {
        try (IndexOutput out = super.createOutput("keyfile", new IOContext())) {
            out.writeInt(wrappedKey.length);
            out.writeBytes(wrappedKey, 0, wrappedKey.length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] getWrappedKey() {
        try (IndexInput in = super.openInput("keyfile", new IOContext())) {
            int size = in.readInt();
            byte[] ret = new byte[size];
            in.readBytes(ret, 0, ret.length);
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        super.rename(source, dest);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        if (name.contains("segments_") || name.endsWith(".si")) {
            return super.openInput(name, context);
        }

        ensureOpen();
        ensureCanRead(name);
        Path path = getDirectory().resolve(name);
        FileChannel fc = FileChannel.open(path, StandardOpenOption.READ);
        boolean success = false;

        try {
            Cipher cipher = CipherFactory.getCipher(provider);
            byte[] ivBytes = Base64.getDecoder().decode(iv);
            CipherFactory.initCipher(cipher, this, Optional.of(ivBytes), Cipher.DECRYPT_MODE, 0);

            final IndexInput indexInput = new CryptoBufferedIndexInput(
                    "CryptoBufferedIndexInput(path=\"" + path + "\")",
                    fc,
                    context,
                    cipher,
                    this
            );
            success = true;
            return indexInput;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(fc);
            }
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        try {
            if (name.contains("segments_") || name.endsWith(".si")) return super.createOutput(name, context);

            ensureOpen();
            Path path = directory.resolve(name);
            OutputStream fos = Files.newOutputStream(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);

            Cipher cipher = CipherFactory.getCipher(provider);
            if (dataKey == null) {
                throw new RuntimeException("dataKey is null!");
            }

            byte[] ivBytes = Base64.getDecoder().decode(iv);
            CipherFactory.initCipher(cipher, this, Optional.of(ivBytes), Cipher.ENCRYPT_MODE, 0);

            return new CryptoIndexOutput(name, path, fos, cipher);
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        if (prefix.contains("segments_") || prefix.endsWith(".si")) {
            return super.createTempOutput(prefix, suffix, context);
        }
        ensureOpen();
        String name = getTempFileName(prefix, suffix, nextTempFileCounter.getAndIncrement());
        Path path = directory.resolve(name);
        OutputStream fos = Files.newOutputStream(directory.resolve(name), StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        Cipher cipher = CipherFactory.getCipher(provider);
        byte[] ivBytes = Base64.getDecoder().decode(iv);
        CipherFactory.initCipher(cipher, this, Optional.of(ivBytes), Cipher.ENCRYPT_MODE, 0);

        return new CryptoIndexOutput(name, path,  fos, cipher);
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            isOpen = false;
            deletePendingFiles();
            dataKey = null;
        } catch (java.nio.file.NoSuchFileException fnfe) {
            // Handle exception if needed
        }
    }

    static class CipherFactory {
        static final int AES_BLOCK_SIZE_BYTES = 16;
        static final int COUNTER_SIZE_BYTES = 4;
        static final int IV_ARRAY_LENGTH = 16;

        public static Cipher getCipher(Provider provider) {
            try {
                return Cipher.getInstance("AES/CTR/NoPadding", provider);
            } catch (NoSuchPaddingException | NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        public static void initCipher(Cipher cipher, CryptoDirectory directory, Optional<byte[]> ivarray, int opmode, long newPosition) {
            try {
                byte[] iv = ivarray.isPresent() ? ivarray.get() : cipher.getIV();
                if (newPosition == 0) {
                    Arrays.fill(iv, IV_ARRAY_LENGTH - COUNTER_SIZE_BYTES, IV_ARRAY_LENGTH, (byte) 0);
                } else {
                    int counter = (int) (newPosition / AES_BLOCK_SIZE_BYTES);
                    for (int i = IV_ARRAY_LENGTH - 1; i >= IV_ARRAY_LENGTH - COUNTER_SIZE_BYTES; i--) {
                        iv[i] = (byte) counter;
                        counter = counter >>> Byte.SIZE;
                    }
                }
                IvParameterSpec spec = new IvParameterSpec(iv);
                cipher.init(opmode, directory.dataKey, spec);
                int bytesToRead = (int) (newPosition % AES_BLOCK_SIZE_BYTES);
                if (bytesToRead > 0) {
                    cipher.update(new byte[bytesToRead]);
                }
            } catch (InvalidAlgorithmParameterException | InvalidKeyException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
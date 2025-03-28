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

import org.apache.lucene.store.OutputStreamIndexOutput;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

/**
 * An IndexOutput implementation that encrypts data before writing
 *
 * @opensearch.internal
 */
public final class CryptoIndexOutput extends OutputStreamIndexOutput {
    /**
     * The maximum chunk size is 8192 bytes, because file channel mallocs a native buffer outside of
     * stack if the write buffer size is larger.
     */
    static final int CHUNK_SIZE = 8192;

    /**
     * Creates a new CryptoIndexOutput
     *
     * @param name The name of the output
     * @param path The path to write to
     * @param os The output stream
     * @param cipher The cipher to use for encryption
     * @throws IOException If there is an I/O error
     */
    public CryptoIndexOutput(String name, Path path, OutputStream os, Cipher cipher) throws IOException {
        super("FSIndexOutput(path=\"" + path + "\")", name, new FilterOutputStream(os) {

            /**
             * {@inheritDoc}
             */
            @Override
            public void close() throws IOException {
                try {
                    out.write(cipher.doFinal());
                } catch (IllegalBlockSizeException | BadPaddingException e) {
                    throw new RuntimeException(e);
                }
                super.close();
            }

            /**
            * {@inheritDoc}
            */
            @Override
            public void write(byte[] b, int offset, int length) throws IOException {
                int count = 0;
                byte[] res;
                while (length > 0) {
                    count++;
                    final int chunk = Math.min(length, CHUNK_SIZE);
                    try {
                        res = cipher.update(b, offset, chunk);
                        if (res != null) out.write(res);
                    } catch (IllegalStateException e) {
                        throw new IllegalStateException("count is " + count + " " + e.getMessage());
                    }
                    length -= chunk;
                    offset += chunk;
                }
            }
        }, CHUNK_SIZE);
    }
}

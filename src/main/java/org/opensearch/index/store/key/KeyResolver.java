/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.key;

import java.io.IOException;
import java.nio.file.Path;
import java.security.Key;

public interface KeyResolver {

    Key getDataKey();

    Key getFileEncryptionKey(Path filePath, String name) throws IOException;

    Key deriveNewFileEncryptionKey(String name) throws IOException;

    void invalidateFileEncrytionKeyStore(String name);
}

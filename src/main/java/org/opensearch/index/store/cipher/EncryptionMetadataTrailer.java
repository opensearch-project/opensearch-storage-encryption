/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.cipher;

import java.nio.charset.StandardCharsets;

public class EncryptionMetadataTrailer {

    public static final String ENCRYPTION_MAGIC_STRING = "ENC_KEY";
    public static final byte[] ENCRYPTION_MAGIC_BYTES = ENCRYPTION_MAGIC_STRING.getBytes(StandardCharsets.UTF_8);
    public static final int ENCRYPTION_KEY_FORMAT_VERSION = 0;

    public static final int ENCRYPTION_KEY_SIZE = 32;
    public static final int ENCRYPTION_MAGIC_LENGTH = ENCRYPTION_MAGIC_STRING.getBytes(StandardCharsets.UTF_8).length; // = 7

    public static final int ENCRYPTION_METADATA_TRAILER_SIZE = ENCRYPTION_MAGIC_LENGTH + Integer.BYTES + ENCRYPTION_KEY_SIZE;
}

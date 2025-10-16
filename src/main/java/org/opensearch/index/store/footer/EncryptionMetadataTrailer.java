/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.store.footer;

import java.nio.charset.StandardCharsets;

/**
 * Constants and utilities for encryption metadata footer format.
 * 
 * Footer format (read backwards from end of file):
 * [FooterAuthTag][GCMTagList][FrameCount][FrameSize][MessageId][KeyMetadata][KeyMetadataLength][AlgorithmId][FooterLength][MagicBytes]
 * 
 * Features:
 * - Frame-based encryption to overcome GCM 64GB limit
 * - HKDF key derivation using MessageId
 * - Footer authentication to prevent tampering
 * - Algorithm agility for future evolution
 * - Variable KeyMetadata for different key providers
 * 
 * @opensearch.internal
 */
public class EncryptionMetadataTrailer {

    // Magic bytes at the very end for reliable footer detection
    public static final String ENCRYPTION_MAGIC_STRING = "OSEF"; // OpenSearch Encrypted File
    public static final byte[] MAGIC = ENCRYPTION_MAGIC_STRING.getBytes(StandardCharsets.UTF_8);
    
    // Field sizes in bytes
    public static final int MESSAGE_ID_SIZE = 16;
    public static final int TAG_COUNT_SIZE = 4;
    public static final int FRAME_SIZE_SIZE = 8;
    public static final int FRAME_COUNT_SIZE = 4;
    public static final int KEY_METADATA_LENGTH_SIZE = 2;
    public static final int ALGORITHM_ID_SIZE = 2;
    public static final int FOOTER_LENGTH_SIZE = 4;
    public static final int FOOTER_AUTH_TAG_SIZE = 16;
    public static final int GCM_TAG_SIZE = 16;
    
    // Calculated sizes
    public static final int FIXED_FOOTER_SIZE = MESSAGE_ID_SIZE + KEY_METADATA_LENGTH_SIZE + ALGORITHM_ID_SIZE + FOOTER_LENGTH_SIZE + MAGIC.length;
    public static final int MIN_FOOTER_SIZE = FIXED_FOOTER_SIZE + TAG_COUNT_SIZE + FRAME_SIZE_SIZE + FRAME_COUNT_SIZE + FOOTER_AUTH_TAG_SIZE;
    
    // Frame constants for large file support
    public static final int DEFAULT_FRAME_SIZE_POWER = 35; // 2^35 = 32GB per frame
    public static final long DEFAULT_FRAME_SIZE = 1L << DEFAULT_FRAME_SIZE_POWER;
    public static final int MAX_FRAMES_PER_FILE = Integer.MAX_VALUE;
    public static final String FRAME_CONTEXT_PREFIX = "frame-";
    
    // Algorithm IDs for future extensibility
    public static final int ALGORITHM_AES_256_GCM = 1;
    
    // Special IV for footer authentication (won't collide with frame IVs)
    public static final byte[] FOOTER_AUTH_IV = new byte[]{
        (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF,
        (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF,
        (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF,
        (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF
    };
    
    /**
     * Calculate total footer size including variable parts
     * @param frameCount number of frames in the file
     * @param keyMetadataLength length of key metadata
     * @return total footer size in bytes
     */
    public static int calculateFooterSize(int frameCount, int keyMetadataLength) {
        return MIN_FOOTER_SIZE + (frameCount * GCM_TAG_SIZE) + keyMetadataLength;
    }
}
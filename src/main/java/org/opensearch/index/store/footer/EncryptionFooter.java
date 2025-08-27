/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.store.footer;

import org.opensearch.index.store.cipher.AesGcmCipherFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Footer format: [FooterAuthTag(16)][GcmTags...][TagCount(4)][FrameSize(8)][FrameCount(4)][MessageId(16)][KeyMetadata(0)]
 * [KeyMetadataLength(2)][AlgorithmId(2)][FooterLength(4)][Magic(4)]
 *
 * Field Name   Size in Bytes
 * ------------  -------------
 * FooterAuthTag      16
 * GCMTagList         16 * FrameCount
 * FrameCount         2
 * FrameSize          6
 * MessageId          16
 * KeyMetadata        Variable // currently empty
 * KeyMetadataLength  2
 * AlgorithmId        2
 * FooterLength       4
 * MagicBytes         4
 *
 */
public class EncryptionFooter {
    
    private final byte[] messageId;
    private final List<byte[]> gcmTags;
    private final long frameSize;
    private final short algorithmId;
    private final byte[] keyMetadata; // Currently empty - key data retrieved from keyfile
    private byte[] footerAuthTag; // 16-byte GCM auth tag for footer authentication
    private int frameCount;
    
    public EncryptionFooter(byte[] messageId, long frameSize, short algorithmId) {
        if (messageId.length != EncryptionMetadataTrailer.MESSAGE_ID_SIZE) {
            throw new IllegalArgumentException("MessageId must be 16 bytes");
        }
        this.messageId = Arrays.copyOf(messageId, EncryptionMetadataTrailer.MESSAGE_ID_SIZE);
        this.gcmTags = new ArrayList<>();
        this.frameSize = frameSize;
        this.algorithmId = algorithmId;
        this.keyMetadata = new byte[0]; // Empty - currently using keyfile for key data
        this.frameCount = 0;
    }
    
    public static EncryptionFooter generateNew(long frameSize, short algorithmId) {
        byte[] messageId = new byte[EncryptionMetadataTrailer.MESSAGE_ID_SIZE];
        new SecureRandom().nextBytes(messageId);
        return new EncryptionFooter(messageId, frameSize, algorithmId);
    }


    public byte[] serialize(byte[] fileKey) throws IOException {
        // Build footer data without auth tag
        byte[] footerData = buildFooterDataWithoutAuthTag();
        
        // Generate auth tag
        this.footerAuthTag = generateFooterAuthTag(fileKey, footerData);
        
        // Prepend auth tag to footer: [AuthTag(16)][FooterData...]
        ByteBuffer buffer = ByteBuffer.allocate(footerData.length + EncryptionMetadataTrailer.FOOTER_AUTH_TAG_SIZE);
        buffer.put(footerAuthTag);
        buffer.put(footerData);
        return buffer.array();
    }
    
    private byte[] buildFooterDataWithoutAuthTag() {
        int footerSize = EncryptionMetadataTrailer.MIN_FOOTER_SIZE - EncryptionMetadataTrailer.FOOTER_AUTH_TAG_SIZE + (gcmTags.size() * AesGcmCipherFactory.GCM_TAG_LENGTH) + keyMetadata.length;
        ByteBuffer buffer = ByteBuffer.allocate(footerSize);
        
        // Write GCM tags
        for (byte[] tag : gcmTags) {
            buffer.put(tag);
        }
        
        // Write tag count
        buffer.putInt(gcmTags.size());
        
        // Write frame size
        buffer.putLong(frameSize);
        
        // Write frame count
        buffer.putInt(frameCount);
        
        // Write MessageId
        buffer.put(messageId);
        
        // Write KeyMetadata (empty - using keyfile)
        buffer.put(keyMetadata);
        
        // Write KeyMetadataLength
        buffer.putShort((short) keyMetadata.length);
        
        // Write algorithm ID
        buffer.putShort(algorithmId);
        
        // Write footer length
        buffer.putInt(footerSize + EncryptionMetadataTrailer.FOOTER_AUTH_TAG_SIZE);
        
        // Write magic
        buffer.put(EncryptionMetadataTrailer.MAGIC);
        
        return buffer.array();
    }
    
    public static EncryptionFooter deserialize(byte[] fileBytes, byte[] fileKey) throws IOException {
        if (fileBytes.length < EncryptionMetadataTrailer.MIN_FOOTER_SIZE) {
            throw new IOException("Invalid footer size: " + fileBytes.length);
        }
        
        // Extract auth tag from beginning
        byte[] authTag = Arrays.copyOfRange(fileBytes, 0, EncryptionMetadataTrailer.FOOTER_AUTH_TAG_SIZE);
        byte[] footerData = Arrays.copyOfRange(fileBytes, EncryptionMetadataTrailer.FOOTER_AUTH_TAG_SIZE, fileBytes.length);
        
        // Verify auth tag
        if (!verifyFooterAuthTag(fileKey, footerData, authTag)) {
            throw new IOException("Footer authentication failed");
        }
        
        int pos = footerData.length;
        
        // Read magic
        pos -= EncryptionMetadataTrailer.MAGIC.length;
        byte[] magic = Arrays.copyOfRange(footerData, pos, pos + EncryptionMetadataTrailer.MAGIC.length);
        if (!Arrays.equals(magic, EncryptionMetadataTrailer.MAGIC)) {
            throw new IOException("Invalid footer magic");
        }
        
        // Read footer length
        pos -= EncryptionMetadataTrailer.FOOTER_LENGTH_SIZE;
        int footerLength = ByteBuffer.wrap(footerData, pos, EncryptionMetadataTrailer.FOOTER_LENGTH_SIZE).getInt();
        
        // Read algorithm ID
        pos -= EncryptionMetadataTrailer.ALGORITHM_ID_SIZE;
        short algorithmId = ByteBuffer.wrap(footerData, pos, EncryptionMetadataTrailer.ALGORITHM_ID_SIZE).getShort();
        
        // Read KeyMetadataLength
        pos -= EncryptionMetadataTrailer.KEY_METADATA_LENGTH_SIZE;
        short keyMetadataLength = ByteBuffer.wrap(footerData, pos, EncryptionMetadataTrailer.KEY_METADATA_LENGTH_SIZE).getShort();
        
        // Read KeyMetadata (currently empty)
        pos -= keyMetadataLength;
        byte[] keyMetadata = Arrays.copyOfRange(footerData, pos, pos + keyMetadataLength);
        
        // Read MessageId
        pos -= EncryptionMetadataTrailer.MESSAGE_ID_SIZE;
        byte[] messageId = Arrays.copyOfRange(footerData, pos, pos + EncryptionMetadataTrailer.MESSAGE_ID_SIZE);
        
        // Read frame count
        pos -= EncryptionMetadataTrailer.FRAME_COUNT_SIZE;
        int frameCount = ByteBuffer.wrap(footerData, pos, EncryptionMetadataTrailer.FRAME_COUNT_SIZE).getInt();
        
        // Read frame size
        pos -= EncryptionMetadataTrailer.FRAME_SIZE_SIZE;
        long frameSize = ByteBuffer.wrap(footerData, pos, EncryptionMetadataTrailer.FRAME_SIZE_SIZE).getLong();
        
        // Read tag count
        pos -= EncryptionMetadataTrailer.TAG_COUNT_SIZE;
        int tagCount = ByteBuffer.wrap(footerData, pos, EncryptionMetadataTrailer.TAG_COUNT_SIZE).getInt();
        
        // Validate footer length
        int expectedLength = EncryptionMetadataTrailer.MIN_FOOTER_SIZE + (tagCount * AesGcmCipherFactory.GCM_TAG_LENGTH) + keyMetadataLength;
        if (footerLength != expectedLength) {
            throw new IOException("Footer length mismatch: expected " + expectedLength + ", got " + footerLength);
        }
        
        // Create footer and read GCM tags
        EncryptionFooter footer = new EncryptionFooter(messageId, frameSize, algorithmId);
        footer.frameCount = frameCount;
        footer.footerAuthTag = authTag;
        int tagStartPos = footerData.length - (footerLength - EncryptionMetadataTrailer.FOOTER_AUTH_TAG_SIZE);
        
        for (int i = 0; i < tagCount; i++) {
            int tagPos = tagStartPos + (i * AesGcmCipherFactory.GCM_TAG_LENGTH);
            byte[] tag = Arrays.copyOfRange(footerData, tagPos, tagPos + AesGcmCipherFactory.GCM_TAG_LENGTH);
            footer.addGcmTag(tag);
        }
        
        return footer;
    }
    
    public static int calculateFooterLength(byte[] footerBytes) throws IOException {
        if (footerBytes.length < EncryptionMetadataTrailer.MIN_FOOTER_SIZE) {
            throw new IOException("Footer too small: " + footerBytes.length);
        }
        
        // Read footer length from end: [FooterLength(4)][Magic(4)]
        int pos = footerBytes.length - EncryptionMetadataTrailer.MAGIC.length - EncryptionMetadataTrailer.FOOTER_LENGTH_SIZE;
        return ByteBuffer.wrap(footerBytes, pos, EncryptionMetadataTrailer.FOOTER_LENGTH_SIZE).getInt();
    }
    
    public byte[] getMessageId() {
        return Arrays.copyOf(messageId, EncryptionMetadataTrailer.MESSAGE_ID_SIZE);
    }
    
    public void addGcmTag(byte[] tag) throws IOException {
        if (tag.length != AesGcmCipherFactory.GCM_TAG_LENGTH) {
            throw new IOException("Invalid GCM tag length: " + tag.length);
        }
        gcmTags.add(Arrays.copyOf(tag, tag.length));
    }
    
    public long getFrameSize() {
        return frameSize;
    }
    
    public int getFrameCount() {
        return frameCount;
    }
    
    public void setFrameCount(int frameCount) {
        this.frameCount = frameCount;
    }
    
    public short getAlgorithmId() {
        return algorithmId;
    }
    
    public byte[] getKeyMetadata() {
        return Arrays.copyOf(keyMetadata, keyMetadata.length);
    }
    
    private static byte[] generateFooterAuthTag(byte[] fileKey, byte[] footerData) throws IOException {
        try {
            byte[] footerIV = new byte[12];  // GCM uses 12-byte IV
            Arrays.fill(footerIV, 0, 8, (byte) 0xFF);  // 0xFFFFFFFFFFFFFFFF0000
            
            // Use default provider instead of null
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            GCMParameterSpec spec = new GCMParameterSpec(128, footerIV);
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(fileKey, "AES"), spec);
            
            // Encrypt empty data to generate auth tag for footerData as AAD
            cipher.updateAAD(footerData);
            byte[] result = cipher.doFinal();  // Empty ciphertext + 16-byte tag
            
            // Extract the 16-byte GCM tag from the end
            return Arrays.copyOfRange(result, result.length - 16, result.length);
        } catch (Exception e) {
            throw new IOException("Failed to generate footer auth tag", e);
        }
    }
    
    private static boolean verifyFooterAuthTag(byte[] fileKey, byte[] footerData, byte[] expectedTag) {
        try {
            byte[] computedTag = generateFooterAuthTag(fileKey, footerData);
            return Arrays.equals(computedTag, expectedTag);
        } catch (Exception e) {
            return false;
        }
    }
}
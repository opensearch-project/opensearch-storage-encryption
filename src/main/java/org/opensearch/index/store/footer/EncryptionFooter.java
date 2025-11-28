/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.footer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.cipher.AesGcmCipherFactory;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.key.HkdfKeyDerivation;

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

    private static final Logger LOGGER = LogManager.getLogger(EncryptionFooter.class);

    private final byte[] messageId;
    private final List<byte[]> gcmTags;
    private final long frameSize;
    private final int frameSizePower;
    private final short algorithmId;
    private final byte[] keyMetadata; // Currently empty - key data retrieved from keyfile
    private byte[] footerAuthTag; // 16-byte GCM auth tag for footer authentication
    private int frameCount;
    private int footerLength;

    public EncryptionFooter(byte[] messageId, long frameSize, short algorithmId) {
        if (messageId.length != EncryptionMetadataTrailer.MESSAGE_ID_SIZE) {
            throw new IllegalArgumentException("MessageId must be 16 bytes");
        }
        if ((frameSize & (frameSize - 1)) != 0 || frameSize <= 0) {
            throw new IllegalArgumentException("frameSize must be a power of 2");
        }
        this.messageId = Arrays.copyOf(messageId, EncryptionMetadataTrailer.MESSAGE_ID_SIZE);
        this.gcmTags = new ArrayList<>();
        this.frameSize = frameSize;
        this.frameSizePower = Long.numberOfTrailingZeros(frameSize);
        this.algorithmId = algorithmId;
        this.keyMetadata = new byte[0]; // Empty - currently using keyfile for key data
        this.frameCount = 0;
        this.footerLength = 0;
    }

    public static EncryptionFooter generateNew(long frameSize, short algorithmId) {
        byte[] messageId = new byte[EncryptionMetadataTrailer.MESSAGE_ID_SIZE];
        new SecureRandom().nextBytes(messageId);
        return new EncryptionFooter(messageId, frameSize, algorithmId);
    }

    public void setFooterLength(int footerLength) {
        this.footerLength = footerLength;
    }

    public int getFooterLength() {
        return footerLength;
    }

    public byte[] serialize(Path filePath, byte[] fileKey) throws IOException {

        // Build footer data without auth tag
        byte[] footerData = buildFooterDataWithoutAuthTag();

        // Set footer length (includes auth tag)
        this.setFooterLength(footerData.length + EncryptionMetadataTrailer.FOOTER_AUTH_TAG_SIZE);

        // Generate auth tag
        this.footerAuthTag = generateFooterAuthTag(fileKey, footerData);

        // Prepend auth tag to footer: [AuthTag(16)][FooterData...]
        ByteBuffer buffer = ByteBuffer.allocate(footerData.length + EncryptionMetadataTrailer.FOOTER_AUTH_TAG_SIZE);
        buffer.put(footerAuthTag);
        buffer.put(footerData);
        return buffer.array();
    }

    private byte[] buildFooterDataWithoutAuthTag() {
        int footerSize = EncryptionMetadataTrailer.MIN_FOOTER_SIZE - EncryptionMetadataTrailer.FOOTER_AUTH_TAG_SIZE + (gcmTags.size()
            * AesGcmCipherFactory.GCM_TAG_LENGTH) + keyMetadata.length;
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
        int expectedLength = EncryptionMetadataTrailer.MIN_FOOTER_SIZE + (tagCount * AesGcmCipherFactory.GCM_TAG_LENGTH)
            + keyMetadataLength;
        if (footerLength != expectedLength) {
            throw new IOException("Footer length mismatch: expected " + expectedLength + ", got " + footerLength);
        }

        // Create footer and read GCM tags
        EncryptionFooter footer = new EncryptionFooter(messageId, frameSize, algorithmId);
        footer.frameCount = frameCount;
        footer.footerAuthTag = authTag;
        footer.setFooterLength(footerLength);
        int tagStartPos = footerData.length - (footerLength - EncryptionMetadataTrailer.FOOTER_AUTH_TAG_SIZE);

        for (int i = 0; i < tagCount; i++) {
            int tagPos = tagStartPos + (i * AesGcmCipherFactory.GCM_TAG_LENGTH);
            byte[] tag = Arrays.copyOfRange(footerData, tagPos, tagPos + AesGcmCipherFactory.GCM_TAG_LENGTH);
            footer.addGcmTag(tag);
        }

        return footer;
    }

    /**
     * Calculate footer length from byte array starting at offset
     * 
     * @param footerBytes byte array containing footer data
     * @param offset starting position in the array
     * @return footer length in bytes
     * @throws IOException if buffer is too small
     */
    public static int calculateFooterLength(byte[] footerBytes, int offset) throws IOException {
        int availableBytes = footerBytes.length - offset;
        if (availableBytes < EncryptionMetadataTrailer.MIN_FOOTER_SIZE) {
            throw new IOException(
                "Footer too small: " + availableBytes + " bytes available, need " + EncryptionMetadataTrailer.MIN_FOOTER_SIZE
            );
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

    public int getFrameSizePower() {
        return frameSizePower;
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

    private static boolean verifyFooterAuthTag(byte[] fileKey, byte[] footerData, byte[] expectedTag) throws IOException {
        try {
            byte[] computedTag = generateFooterAuthTag(fileKey, footerData);
            return Arrays.equals(computedTag, expectedTag);
        } catch (IOException e) {
            LOGGER.error("Encountered error while verify footer auth tag", e);
            throw e;
        }
    }

    /**
     * Maximum footer size to read in single I/O operation.
     * Covers files up to ~16TB with 32GB frames (512 frames × 16 bytes/tag + overhead)
     */
    private static final int MAX_FOOTER_READ_SIZE = 8192; // 8KB

    /**
     * Read and deserialize footer from a FileChannel
     *
     * @param normalizedFilePath normalized file path string
     * @param channel FileChannel to read from
     * @param masterKey Master key for file key derivation and footer authentication
     * @param encryptionMetadataCache cache for encryption metadata
     * @return Deserialized EncryptionFooter
     * @throws IOException If reading or deserialization fails
     * @throws NotOSEFFileException If file is not a valid OSEF format
     */
    public static EncryptionFooter readViaFileChannel(
        String normalizedFilePath,
        java.nio.channels.FileChannel channel,
        byte[] masterKey,
        EncryptionMetadataCache encryptionMetadataCache
    ) throws IOException {

        EncryptionFooter cachedFooter = encryptionMetadataCache.getFooter(normalizedFilePath);
        if (cachedFooter != null) {
            return cachedFooter;
        }

        long fileSize = channel.size();
        if (fileSize < EncryptionMetadataTrailer.MIN_FOOTER_SIZE) {
            throw new NotOSEFFileException("File too small to contain encryption footer: " + normalizedFilePath);
        }

        // Read MAX_FOOTER_READ_SIZE or entire file if smaller
        int readSize = (int) Math.min(MAX_FOOTER_READ_SIZE, fileSize);
        long readPosition = fileSize - readSize;

        ByteBuffer buffer = ByteBuffer.allocate(readSize);
        int bytesRead = channel.read(buffer, readPosition);

        if (bytesRead != readSize) {
            throw new IOException("Failed to read footer: expected " + readSize + " bytes, got " + bytesRead);
        }

        // Reset position for reading
        buffer.flip();

        // Extract bytes for validation - read from end of buffer
        byte[] bufferArray = buffer.array();
        int minFooterStart = bufferArray.length - EncryptionMetadataTrailer.MIN_FOOTER_SIZE;

        // Validate OSEF magic bytes from the last MIN_FOOTER_SIZE bytes
        if (!isValidOSEFFile(bufferArray, minFooterStart)) {
            throw new NotOSEFFileException("File does not contain valid OSEF magic bytes: " + normalizedFilePath);
        }

        // Calculate actual footer length from the buffer
        int footerLength = calculateFooterLength(bufferArray, minFooterStart);

        // Validate we read enough data
        if (footerLength > readSize) {
            throw new IOException(
                "Footer length "
                    + footerLength
                    + " exceeds max read size "
                    + MAX_FOOTER_READ_SIZE
                    + ". File may have an unusually large footer."
            );
        }

        // Extract actual footer bytes from buffer
        int footerStart = bufferArray.length - footerLength;
        byte[] footerBytes = Arrays.copyOfRange(bufferArray, footerStart, bufferArray.length);

        // Extract messageId to derive file key for authentication
        byte[] messageId = extractMessageId(footerBytes);
        byte[] fileKey = HkdfKeyDerivation.deriveFileKey(masterKey, messageId);

        // Deserialize footer with file key for authentication
        EncryptionFooter footer = deserialize(footerBytes, fileKey);
        encryptionMetadataCache.getOrLoadMetadata(normalizedFilePath, footer, masterKey);
        return footer;
    }

    /**
     * Extract messageId from footer bytes without full deserialization or authentication.
     * This is needed to derive the file key before we can verify the footer's auth tag.
     *
     * @param footerBytes complete footer bytes including auth tag
     * @return 16-byte messageId
     * @throws IOException if footer format is invalid
     */
    private static byte[] extractMessageId(byte[] footerBytes) throws IOException {
        if (footerBytes.length < EncryptionMetadataTrailer.MIN_FOOTER_SIZE) {
            throw new IOException("Footer too small: " + footerBytes.length);
        }

        // Skip auth tag at beginning
        byte[] footerData = Arrays.copyOfRange(footerBytes, EncryptionMetadataTrailer.FOOTER_AUTH_TAG_SIZE, footerBytes.length);

        // Read KeyMetadataLength to know how much to skip
        int keyMetadataLengthPos = footerData.length - EncryptionMetadataTrailer.MAGIC.length - EncryptionMetadataTrailer.FOOTER_LENGTH_SIZE
            - EncryptionMetadataTrailer.ALGORITHM_ID_SIZE - EncryptionMetadataTrailer.KEY_METADATA_LENGTH_SIZE;
        short keyMetadataLength = ByteBuffer
            .wrap(footerData, keyMetadataLengthPos, EncryptionMetadataTrailer.KEY_METADATA_LENGTH_SIZE)
            .getShort();

        // MessageId is before KeyMetadata
        int messageIdPos = footerData.length - EncryptionMetadataTrailer.MAGIC.length - EncryptionMetadataTrailer.FOOTER_LENGTH_SIZE
            - EncryptionMetadataTrailer.ALGORITHM_ID_SIZE - EncryptionMetadataTrailer.KEY_METADATA_LENGTH_SIZE - keyMetadataLength
            - EncryptionMetadataTrailer.MESSAGE_ID_SIZE;

        return Arrays.copyOfRange(footerData, messageIdPos, messageIdPos + EncryptionMetadataTrailer.MESSAGE_ID_SIZE);
    }

    /**
     * Check if file has valid OSEF magic bytes starting at offset
     * 
     * @param footerBytes byte array containing footer data
     * @param offset starting position in the array
     * @return true if valid OSEF magic bytes are present
     */
    private static boolean isValidOSEFFile(byte[] footerBytes, int offset) {
        int availableBytes = footerBytes.length - offset;
        if (availableBytes < EncryptionMetadataTrailer.MIN_FOOTER_SIZE) {
            return false;
        }

        int magicOffset = footerBytes.length - EncryptionMetadataTrailer.MAGIC.length;
        for (int i = 0; i < EncryptionMetadataTrailer.MAGIC.length; i++) {
            if (footerBytes[magicOffset + i] != EncryptionMetadataTrailer.MAGIC[i]) {
                return false;
            }
        }
        return true;
    }

    public static class NotOSEFFileException extends IOException {

        public NotOSEFFileException(String message) {
            super(message);
        }

        public NotOSEFFileException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

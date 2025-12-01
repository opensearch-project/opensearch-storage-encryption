/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import java.util.regex.Pattern;

import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;

/**
 * Validates crypto-related index settings at index creation time.
 * This allows failing early with clear error messages rather than waiting for shard allocation.
 */
public class CryptoIndexSettingsValidator {

    /**
     * Pattern for validating AWS KMS ARN format.
     * Accepts standard AWS, China (aws-cn), and GovCloud (aws-us-gov) partitions.
     * Limits content after "kms:" to 1-2000 (just a safe limit) characters.
     */
    private static final Pattern KMS_ARN_PATTERN = Pattern.compile("^arn:aws(-[^:]+)?:kms:.{1,2000}$");

    /**
     * Pattern for validating encryption context format.
     * Format: key1=value1,key2=value2 (no spaces around = or ,)
     */
    private static final Pattern ENCRYPTION_CONTEXT_PATTERN = Pattern.compile("^[a-zA-Z0-9_-]+=[^,=]+(,[a-zA-Z0-9_-]+=[^,=]+)*$");

    private static final String AWS_KMS_PROVIDER = "aws-kms";

    /**
     * Validates all crypto-related index settings.
     *
     * @param indexSettings the index settings to validate
     * @throws IllegalArgumentException if any setting is invalid
     */
    public static void validate(Settings indexSettings) {
        String storeType = indexSettings.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey());

        if (!CryptoDirectoryFactory.STORE_TYPE.equals(storeType)) {
            return;
        }

        validateKeyProvider(indexSettings);
        validateKmsSettings(indexSettings);
        validateKmsEncryptionContext(indexSettings);
    }

    /**
     * Validates the crypto key provider setting.
     */
    private static void validateKeyProvider(Settings indexSettings) {
        String keyProvider = indexSettings.get(CryptoDirectoryFactory.INDEX_KEY_PROVIDER_SETTING.getKey());

        if (keyProvider == null || keyProvider.isEmpty()) {
            throw new IllegalArgumentException("index.store.crypto.key_provider must be specified when index.store.type is 'cryptofs'");
        }
    }

    /**
     * Validates KMS-related settings when key provider is aws-kms.
     */
    private static void validateKmsSettings(Settings indexSettings) {
        String keyProvider = indexSettings.get(CryptoDirectoryFactory.INDEX_KEY_PROVIDER_SETTING.getKey());

        if (!AWS_KMS_PROVIDER.equals(keyProvider)) {
            return;
        }

        String kmsArn = indexSettings.get(CryptoDirectoryFactory.INDEX_KMS_ARN_SETTING.getKey());

        if (kmsArn == null || kmsArn.isEmpty()) {
            throw new IllegalArgumentException("index.store.crypto.kms.key_arn must be specified when key_provider is 'aws-kms'");
        }

        if (!KMS_ARN_PATTERN.matcher(kmsArn).find()) {
            throw new IllegalArgumentException("Invalid KMS ARN format: " + kmsArn + ". Expected format: arn:aws[-partition]:kms:...");
        }
    }

    /**
     * Validates encryption context format if provided.
     * Format: key1=value1,key2=value2
     */
    private static void validateKmsEncryptionContext(Settings indexSettings) {
        String keyProvider = indexSettings.get(CryptoDirectoryFactory.INDEX_KEY_PROVIDER_SETTING.getKey());

        if (!AWS_KMS_PROVIDER.equals(keyProvider)) {
            return;
        }

        String encryptionContext = indexSettings.get(CryptoDirectoryFactory.INDEX_KMS_ENC_CTX_SETTING.getKey());

        if (encryptionContext == null || encryptionContext.isEmpty()) {
            return; // Encryption context is optional
        }

        if (!ENCRYPTION_CONTEXT_PATTERN.matcher(encryptionContext).matches()) {
            throw new IllegalArgumentException(
                "Invalid encryption context format: " + encryptionContext + ". Expected format: key1=value1,key2=value2 (no spaces)"
            );
        }
    }
}

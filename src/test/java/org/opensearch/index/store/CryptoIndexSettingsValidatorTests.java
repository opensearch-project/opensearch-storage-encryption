/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link CryptoIndexSettingsValidator}.
 */
public class CryptoIndexSettingsValidatorTests extends OpenSearchTestCase {

    public void testValidateNonCryptoStoreType() {
        Settings settings = Settings.builder().put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "niofs").build();

        // Should not throw any exception for non-cryptofs store type
        CryptoIndexSettingsValidator.validate(settings);
    }

    public void testValidateMissingKeyProvider() {
        Settings settings = Settings.builder().put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "cryptofs").build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> CryptoIndexSettingsValidator.validate(settings)
        );

        assertTrue(exception.getMessage().contains("index.store.crypto.key_provider must be specified"));
    }

    public void testValidateMissingKmsArnForAwsKms() {
        Settings settings = Settings
            .builder()
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "cryptofs")
            .put(CryptoDirectoryFactory.INDEX_KEY_PROVIDER_SETTING.getKey(), "aws-kms")
            .build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> CryptoIndexSettingsValidator.validate(settings)
        );

        assertTrue(exception.getMessage().contains("index.store.crypto.kms.key_arn must be specified"));
    }

    public void testValidateInvalidKmsArnFormat() {
        String[] invalidArns = {
            "invalid-arn",
            "arn:aws:s3:us-west-2:123456789012:key/abc123", // s3 instead of kms
            "arn:gcp:kms:us-west-2:123456789012:key/abc123", // gcp instead of aws
            "arn:aws:kms:", // empty after kms:
            "arn:aws:kms:" + "a".repeat(2001) // exceeds 2000 char limit
        };

        for (String invalidArn : invalidArns) {
            Settings settings = Settings
                .builder()
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "cryptofs")
                .put(CryptoDirectoryFactory.INDEX_KEY_PROVIDER_SETTING.getKey(), "aws-kms")
                .put(CryptoDirectoryFactory.INDEX_KMS_ARN_SETTING.getKey(), invalidArn)
                .build();

            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> CryptoIndexSettingsValidator.validate(settings)
            );

            assertTrue("Expected error for invalid ARN: " + invalidArn, exception.getMessage().contains("Invalid KMS ARN format"));
        }
    }

    public void testValidateValidKmsArn() {
        String[] validArns = {
            "arn:aws:kms:us-west-2:248189931838:key/eb9f247c-304a-4078-becb-2219e596c40d",
            "arn:aws-cn:kms:cn-north-1:123456789012:key/12345678-1234-1234-1234-123456789012",
            "arn:aws-us-gov:kms:us-gov-west-1:123456789012:key/12345678-1234-1234-1234-123456789012",
            "arn:aws:kms:us-west-2:123456789012:alias/my-key", // alias format
            "arn:aws:kms::123456789012:key/abc123" // missing region (lenient)
        };

        for (String validArn : validArns) {
            Settings settings = Settings
                .builder()
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "cryptofs")
                .put(CryptoDirectoryFactory.INDEX_KEY_PROVIDER_SETTING.getKey(), "aws-kms")
                .put(CryptoDirectoryFactory.INDEX_KMS_ARN_SETTING.getKey(), validArn)
                .build();

            // Should not throw any exception
            CryptoIndexSettingsValidator.validate(settings);
        }
    }

    public void testValidateInvalidEncryptionContextFormat() {
        String[] invalidContexts = {
            "key1 = value1", // spaces around =
            "key1=value1, key2=value2", // space after comma
            "key1=value1,key2 =value2", // space before =
            "=value1", // missing key
            "key1=", // missing value
            "key1==value1", // double equals
            "key1=value1,,key2=value2" // double comma
        };

        for (String invalidContext : invalidContexts) {
            Settings settings = Settings
                .builder()
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "cryptofs")
                .put(CryptoDirectoryFactory.INDEX_KEY_PROVIDER_SETTING.getKey(), "aws-kms")
                .put(
                    CryptoDirectoryFactory.INDEX_KMS_ARN_SETTING.getKey(),
                    "arn:aws:kms:us-west-2:248189931838:key/eb9f247c-304a-4078-becb-2219e596c40d"
                )
                .put(CryptoDirectoryFactory.INDEX_KMS_ENC_CTX_SETTING.getKey(), invalidContext)
                .build();

            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> CryptoIndexSettingsValidator.validate(settings)
            );

            assertTrue(
                "Expected error for invalid encryption context: " + invalidContext,
                exception.getMessage().contains("Invalid encryption context format")
            );
        }
    }

    public void testValidateValidEncryptionContext() {
        String[] validContexts = {
            "domainARN=arn:aws:es:us-west-2:248189931838:domain/ile-testing-3-3-new-vfi-2",
            "key1=value1",
            "key1=value1,key2=value2",
            "key-with-dash=value",
            "key_with_underscore=value",
            "key123=value456" };

        for (String validContext : validContexts) {
            Settings settings = Settings
                .builder()
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "cryptofs")
                .put(CryptoDirectoryFactory.INDEX_KEY_PROVIDER_SETTING.getKey(), "aws-kms")
                .put(
                    CryptoDirectoryFactory.INDEX_KMS_ARN_SETTING.getKey(),
                    "arn:aws:kms:us-west-2:248189931838:key/eb9f247c-304a-4078-becb-2219e596c40d"
                )
                .put(CryptoDirectoryFactory.INDEX_KMS_ENC_CTX_SETTING.getKey(), validContext)
                .build();

            // Should not throw any exception
            CryptoIndexSettingsValidator.validate(settings);
        }
    }

    public void testValidateNullEncryptionContext() {
        Settings settings = Settings
            .builder()
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "cryptofs")
            .put(CryptoDirectoryFactory.INDEX_KEY_PROVIDER_SETTING.getKey(), "aws-kms")
            .put(
                CryptoDirectoryFactory.INDEX_KMS_ARN_SETTING.getKey(),
                "arn:aws:kms:us-west-2:248189931838:key/eb9f247c-304a-4078-becb-2219e596c40d"
            )
            .build();

        // Should not throw any exception - encryption context is optional
        CryptoIndexSettingsValidator.validate(settings);
    }

    public void testValidateValidCryptoProvider() {
        Settings settings = Settings
            .builder()
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "cryptofs")
            .put(CryptoDirectoryFactory.INDEX_KEY_PROVIDER_SETTING.getKey(), "aws-kms")
            .put(
                CryptoDirectoryFactory.INDEX_KMS_ARN_SETTING.getKey(),
                "arn:aws:kms:us-west-2:248189931838:key/eb9f247c-304a-4078-becb-2219e596c40d"
            )
            .build();

        // Should not throw any exception
        CryptoIndexSettingsValidator.validate(settings);
    }

    public void testValidateNonAwsKmsProviderDoesNotRequireKmsArn() {
        Settings settings = Settings
            .builder()
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "cryptofs")
            .put(CryptoDirectoryFactory.INDEX_KEY_PROVIDER_SETTING.getKey(), "dummy")
            .build();

        // Should not throw any exception - KMS ARN not required for non-aws-kms providers
        CryptoIndexSettingsValidator.validate(settings);
    }
}

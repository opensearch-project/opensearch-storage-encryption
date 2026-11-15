/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import java.security.Provider;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link CryptoDirectoryFactory#INDEX_CRYPTO_PROVIDER_SETTING} and
 * {@link CryptoDirectoryFactory#resolveCryptoProvider(Settings)}.
 */
public class CryptoDirectoryFactoryProviderTests extends OpenSearchTestCase {

    public void testResolveCryptoProviderDefaultsToSunJceWhenOmitted() {
        Provider provider = CryptoDirectoryFactory.resolveCryptoProvider(Settings.EMPTY);
        assertNotNull(provider);
        assertEquals(CryptoDirectoryFactory.DEFAULT_CRYPTO_PROVIDER, provider.getName());
        assertEquals("SunJCE", CryptoDirectoryFactory.INDEX_CRYPTO_PROVIDER_SETTING.get(Settings.EMPTY));
        assertEquals(
            CryptoDirectoryFactory.DEFAULT_CRYPTO_PROVIDER,
            CryptoDirectoryFactory.INDEX_CRYPTO_PROVIDER_SETTING.getDefault(Settings.EMPTY)
        );
    }

    public void testResolveCryptoProviderExplicitSunJce() {
        Settings settings = Settings.builder().put(CryptoDirectoryFactory.INDEX_CRYPTO_PROVIDER_SETTING.getKey(), "SunJCE").build();
        Provider provider = CryptoDirectoryFactory.resolveCryptoProvider(settings);
        assertNotNull(provider);
        assertEquals("SunJCE", provider.getName());
        assertEquals("SunJCE", CryptoDirectoryFactory.INDEX_CRYPTO_PROVIDER_SETTING.get(settings));
    }

    public void testResolveCryptoProviderRejectsEmpty() {
        Settings settings = Settings.builder().put(CryptoDirectoryFactory.INDEX_CRYPTO_PROVIDER_SETTING.getKey(), "").build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> CryptoDirectoryFactory.resolveCryptoProvider(settings)
        );
        assertTrue(exception.getMessage().contains("index.store.crypto.provider"));
        assertTrue(exception.getCause() instanceof SettingsException);
        assertTrue(exception.getCause().getMessage().contains("index.store.crypto.provider"));
    }

    public void testResolveCryptoProviderRejectsBlank() {
        Settings settings = Settings.builder().put(CryptoDirectoryFactory.INDEX_CRYPTO_PROVIDER_SETTING.getKey(), "   ").build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> CryptoDirectoryFactory.resolveCryptoProvider(settings)
        );
        assertTrue(exception.getMessage().contains("index.store.crypto.provider"));
        assertTrue(exception.getCause() instanceof SettingsException);
    }

    public void testResolveCryptoProviderRejectsUnknown() {
        Settings settings = Settings.builder().put(CryptoDirectoryFactory.INDEX_CRYPTO_PROVIDER_SETTING.getKey(), "NotAProvider").build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> CryptoDirectoryFactory.resolveCryptoProvider(settings)
        );
        assertTrue(exception.getMessage().contains("NotAProvider"));
        assertTrue(exception.getMessage().contains("index.store.crypto.provider"));
        assertTrue(exception.getCause() instanceof SettingsException);
        assertTrue(exception.getCause().getMessage().contains("NotAProvider"));
    }

    public void testCryptoProviderSettingScope() {
        assertTrue(CryptoDirectoryFactory.INDEX_CRYPTO_PROVIDER_SETTING.getProperties().contains(Setting.Property.IndexScope));
        assertTrue(CryptoDirectoryFactory.INDEX_CRYPTO_PROVIDER_SETTING.getProperties().contains(Setting.Property.InternalIndex));
        assertFalse(CryptoDirectoryFactory.INDEX_CRYPTO_PROVIDER_SETTING.getProperties().contains(Setting.Property.NodeScope));
        assertFalse(CryptoDirectoryFactory.INDEX_CRYPTO_PROVIDER_SETTING.getProperties().contains(Setting.Property.Dynamic));
    }
}

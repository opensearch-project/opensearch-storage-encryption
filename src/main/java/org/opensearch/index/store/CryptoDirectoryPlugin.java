/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.opensearch.common.settings.Setting;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.Plugin;

/**
 * A plugin that enables index level encryption and decryption.
 */
public class CryptoDirectoryPlugin extends Plugin implements IndexStorePlugin {

    /**
     * The default constructor.
     */
    public CryptoDirectoryPlugin() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(CryptoDirectoryFactory.INDEX_KMS_TYPE_SETTING, CryptoDirectoryFactory.INDEX_CRYPTO_PROVIDER_SETTING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        return java.util.Collections.singletonMap("cryptofs", new CryptoDirectoryFactory());
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.metrics.CryptoMetricsLogger;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.Plugin;

/**
 * A plugin that enables index level encryption and decryption.
 */
public class CryptoDirectoryPlugin extends Plugin implements IndexStorePlugin {

    public CryptoDirectoryPlugin(Settings settings) {
        super();
        initializeEMF(settings);
    }

    @SuppressForbidden(reason = "AWS EMF requires system properties")
    private void initializeEMF(Settings settings) {
        if (CryptoDirectoryFactory.EMF_ENABLED_SETTING.get(settings)) {
            System.setProperty("AWS_EMF_ENVIRONMENT", CryptoDirectoryFactory.EMF_ENVIRONMENT_SETTING.get(settings));
            System.setProperty("AWS_REGION", CryptoDirectoryFactory.EMF_REGION_SETTING.get(settings));
            System.setProperty("AWS_EMF_SERVICE_NAME", CryptoDirectoryFactory.EMF_SERVICE_NAME_SETTING.get(settings));
            System.setProperty("AWS_EMF_SERVICE_TYPE", CryptoDirectoryFactory.EMF_SERVICE_TYPE_SETTING.get(settings));
        }
        CryptoMetricsLogger.setSamplingRate(CryptoDirectoryFactory.EMF_SAMPLING_RATE_SETTING.get(settings));
        CryptoMetricsLogger.setNamespace(CryptoDirectoryFactory.EMF_NAMESPACE_SETTING.get(settings));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
                CryptoDirectoryFactory.INDEX_KMS_TYPE_SETTING,
                CryptoDirectoryFactory.INDEX_CRYPTO_PROVIDER_SETTING,
                CryptoDirectoryFactory.EMF_ENABLED_SETTING,
                CryptoDirectoryFactory.EMF_ENVIRONMENT_SETTING,
                CryptoDirectoryFactory.EMF_REGION_SETTING,
                CryptoDirectoryFactory.EMF_SERVICE_NAME_SETTING,
                CryptoDirectoryFactory.EMF_SERVICE_TYPE_SETTING,
                CryptoDirectoryFactory.EMF_SAMPLING_RATE_SETTING
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        return java.util.Collections.singletonMap("cryptofs", new CryptoDirectoryFactory());
    }
}

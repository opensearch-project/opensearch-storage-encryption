/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexModule;
import org.opensearch.index.store.CryptoDirectoryFactory;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportGetIndexCountForKeyAction extends HandledTransportAction<GetIndexCountForKeyRequest, GetIndexCountForKeyResponse> {

    private final ClusterService clusterService;

    @Inject
    public TransportGetIndexCountForKeyAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService
    ) {
        super(GetIndexCountForKeyAction.NAME, transportService, actionFilters, GetIndexCountForKeyRequest::new);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, GetIndexCountForKeyRequest request, ActionListener<GetIndexCountForKeyResponse> listener) {
        ActionListener.completeWith(listener, () -> {
            Metadata metadata = clusterService.state().metadata();
            String filterKeyId = request.getKeyId();
            String filterKeyProvider = request.getKeyProvider();

            long count = metadata
                .indices()
                .values()
                .stream()
                .filter(indexMetadata -> isCryptoIndex(indexMetadata))
                .filter(indexMetadata -> matchesFilters(indexMetadata, filterKeyId, filterKeyProvider))
                .count();

            return new GetIndexCountForKeyResponse((int) count);
        });
    }

    private boolean isCryptoIndex(IndexMetadata indexMetadata) {
        Settings settings = indexMetadata.getSettings();
        String storeType = settings.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey());
        return CryptoDirectoryFactory.STORE_TYPE.equals(storeType);
    }

    private boolean matchesFilters(IndexMetadata indexMetadata, String filterKeyId, String filterKeyProvider) {
        Settings settings = indexMetadata.getSettings();
        String keyId = CryptoDirectoryFactory.INDEX_KMS_ARN_SETTING.get(settings);
        String keyProvider = CryptoDirectoryFactory.INDEX_KEY_PROVIDER_SETTING.get(settings);

        boolean keyIdMatches = filterKeyId.equals(keyId);
        boolean keyProviderMatches = filterKeyProvider.equals(keyProvider);

        return keyIdMatches && keyProviderMatches;
    }

}

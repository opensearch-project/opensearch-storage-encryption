/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
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

            long count = metadata.indices().values().stream().filter(indexMetadata -> {
                Settings settings = indexMetadata.getSettings();
                String storeType = settings.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey());
                return CryptoDirectoryFactory.STORE_TYPE.equals(storeType);
            }).filter(indexMetadata -> {
                // filter only for KMS key for now.
                String keyId = CryptoDirectoryFactory.INDEX_KMS_ARN_SETTING.get(indexMetadata.getSettings());
                return filterKeyId == null || filterKeyId.equals(keyId);
            }).count();

            return new GetIndexCountForKeyResponse((int) count);
        });
    }

}

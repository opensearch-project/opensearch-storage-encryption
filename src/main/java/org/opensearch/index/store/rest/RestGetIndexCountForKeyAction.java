/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.rest;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

import java.io.IOException;
import java.util.List;

import org.opensearch.index.store.Constants;
import org.opensearch.index.store.action.GetIndexCountForKeyAction;
import org.opensearch.index.store.action.GetIndexCountForKeyRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

public class RestGetIndexCountForKeyAction extends BaseRestHandler {

    private static final String ACTION_NAME = "get_index_count_for_key_action";
    private static final String ROUTE_PATH = "/_plugins/_opensearch_storage_encryption/_index_count_for_key";

    @Override
    public String getName() {
        return ACTION_NAME;
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, ROUTE_PATH));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String keyId = request.param("key_id");
        String keyProvider = request.param("key_provider");
        String encryptionContext = request.param("encryption_context", Constants.DEFAULT_KMS_ENC_CTX); // default to empty string
        GetIndexCountForKeyRequest countRequest = new GetIndexCountForKeyRequest(keyId, keyProvider, encryptionContext);
        return channel -> client.execute(GetIndexCountForKeyAction.INSTANCE, countRequest, new RestToXContentListener<>(channel));
    }
}

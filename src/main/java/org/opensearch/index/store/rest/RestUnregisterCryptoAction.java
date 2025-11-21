/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.rest;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;

import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

public class RestUnregisterCryptoAction extends BaseRestHandler {

    private static final String ACTION_NAME = "unregister_key_action";
    private static final String ROUTE_PATH = "/_plugin/opensearch-storage-encryption/_unregister_key";

    @Override
    public String getName() {
        return ACTION_NAME;
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, ROUTE_PATH));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        throw new UnsupportedOperationException("Unregister key operation is not implemented");
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.rest;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.opensearch.rest.RestRequest.Method.POST;

import org.junit.Before;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;

public class RestUnregisterCryptoActionTests extends OpenSearchTestCase {

    private RestUnregisterCryptoAction action;
    private RestRequest request;
    private NodeClient client;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        action = new RestUnregisterCryptoAction();
        request = mock(RestRequest.class);
        client = mock(NodeClient.class);
    }

    public void testGetName() {
        assertEquals("unregister_key_action", action.getName());
    }

    public void testRoutes() {
        assertEquals(1, action.routes().size());
        assertEquals(POST, action.routes().get(0).getMethod());
        assertEquals("/_plugin/opensearch-storage-encryption/_unregister_key", action.routes().get(0).getPath());
    }

    public void testPrepareRequestThrowsUnsupportedOperationException() {
        expectThrows(UnsupportedOperationException.class, () -> action.prepareRequest(request, client));
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.action;

import org.opensearch.action.ActionType;

public class GetIndexCountForKeyAction extends ActionType<GetIndexCountForKeyResponse> {

    public static final GetIndexCountForKeyAction INSTANCE = new GetIndexCountForKeyAction();
    public static final String NAME = "cluster:admin/opensearch/storage_encryption/get_index_count_for_key";

    private GetIndexCountForKeyAction() {
        super(NAME, GetIndexCountForKeyResponse::new);
    }
}

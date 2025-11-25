/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.action;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class GetIndexCountForKeyRequest extends ActionRequest {
    private final String keyId;

    public GetIndexCountForKeyRequest(String keyId) {
        if (keyId == null || keyId.isBlank()) {
            throw new IllegalArgumentException("keyId must not be null or empty");
        }
        this.keyId = keyId;
    }

    public GetIndexCountForKeyRequest(StreamInput in) throws IOException {
        super(in);
        this.keyId = in.readString();
    }

    public String getKeyId() {
        return keyId;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (keyId == null || keyId.isBlank()) {
            ActionRequestValidationException e = new ActionRequestValidationException();
            e.addValidationError("keyId must not be null or empty");
            return e;
        }
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(keyId);
    }
}

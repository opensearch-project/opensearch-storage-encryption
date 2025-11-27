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

    /**
     * Key identifier representing an index-level encryption key.
     */
    private final String keyId;

    /**
     * Key provider type (e.g., "aws-kms").
     */
    private final String keyProvider;

    public GetIndexCountForKeyRequest(String keyId, String keyProvider) {
        if (keyId == null || keyId.isBlank()) {
            throw new IllegalArgumentException("keyId must not be null or empty");
        }
        if (keyProvider == null || keyProvider.isBlank()) {
            throw new IllegalArgumentException("keyProvider must not be null or empty");
        }
        this.keyId = keyId;
        this.keyProvider = keyProvider;
    }

    public GetIndexCountForKeyRequest(StreamInput in) throws IOException {
        super(in);
        this.keyId = in.readString();
        this.keyProvider = in.readString();
    }

    public String getKeyId() {
        return keyId;
    }

    public String getKeyProvider() {
        return keyProvider;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        if (keyId == null || keyId.isBlank()) {
            validationException = new ActionRequestValidationException();
            validationException.addValidationError("keyId must not be null or empty");
        }

        if (keyProvider == null || keyProvider.isBlank()) {
            if (validationException == null) {
                validationException = new ActionRequestValidationException();
            }
            validationException.addValidationError("keyProvider must not be null or empty");
        }

        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(keyId);
        out.writeString(keyProvider);
    }
}

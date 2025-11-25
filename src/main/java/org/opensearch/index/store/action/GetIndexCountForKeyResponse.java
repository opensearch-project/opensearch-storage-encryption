/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.action;

import java.io.IOException;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

public class GetIndexCountForKeyResponse extends ActionResponse implements ToXContentObject {

    private final int count;

    public GetIndexCountForKeyResponse(int count) {
        this.count = count;
    }

    public GetIndexCountForKeyResponse(StreamInput in) throws IOException {
        super(in);
        this.count = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(count);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("count", count);
        builder.endObject();
        return builder;
    }

    public int getCount() {
        return count;
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.mmap;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IOContext.Context;
import org.apache.lucene.store.ReadAdvice;

public class LuceneIOContextMAdvise {

    // madvise flags
    private static final int MADV_NORMAL = 0;
    private static final int MADV_SEQUENTIAL = 2;
    private static final int MADV_WILLNEED = 3;
    private static final int MADV_DONTNEED = 4;

    /**
     * Get madvise flags based on Lucene's IOContext
     * 
     * - MERGE context always uses SEQUENTIAL
     * - FLUSH context always uses SEQUENTIAL
     * - DEFAULT context can have RANDOM, SEQUENTIAL, or NORMAL
     * - READONCE is just DEFAULT with SEQUENTIAL
     */

    public static int getMAdviseFlags(IOContext context, String fileName) {
        if (context == null) {
            return MADV_NORMAL;
        }

        Context ctxType = context.context();
        ReadAdvice readAdvice = context.readAdvice();

        // Handle based on context type first
        switch (ctxType) {
            case MERGE -> {
                // Merges always sequential, free pages after reading
                return MADV_SEQUENTIAL | MADV_DONTNEED;
            }

            case FLUSH -> {
                // Flushes are sequential writes/reads
                return MADV_SEQUENTIAL;
            }

            case DEFAULT -> {
                // Check the actual readAdvice for DEFAULT context
                return switch (readAdvice) {
                    case SEQUENTIAL -> MADV_SEQUENTIAL;
                    case RANDOM -> MADV_NORMAL; // random in lucene seems to behaving problem.
                    case NORMAL -> MADV_NORMAL;
                    default -> MADV_NORMAL;
                };
            }

            // most optimal.
            default -> {
                return MADV_WILLNEED;
            }
        }
    }
}

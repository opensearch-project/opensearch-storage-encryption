/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead;

import java.io.Closeable;

/**
 * Per-IndexInput readahead context that manages sequential access detection
 * and triggers async prefetch as needed.
 */

/*
 * --------- Notes for later tuning --------------
 * Hot Lucene File Types for Block Cache and Readahead
 * ---------------------------------------------------
 * Not all Lucene files benefit equally from caching and readahead.
 * Below is a reference mapping for block cache / DirectIO optimization.
 *
 *  Extension   Purpose                      Access Pattern                 Cache Benefit
 *  ---------   ---------------------------  -----------------------------  -------------
 *  .doc        Postings doc IDs              Sequential during merge/search ✅ High
 *  .pos        Postings positions            Sequential for phrase queries  ✅ High
 *  .pay        Postings payloads             Sequential                     ✅ Medium
 *  .fdt        Stored fields data            Sequential during merge        ✅ High
 *  .fdx        Stored fields index           Mostly sequential              ✅ High
 *  .dvd        DocValues data                Sequential during merge        ✅ High
 *  .dvm        DocValues metadata            Small, random reads            ❌ Low
 *  .tim/.tip   Term dictionary & index       Mostly sequential              ✅ Medium
 *  .nvd/.nvm   Norms                         Sequential during merge        ✅ Medium
 *  .liv        Live docs                     Small, random reads            ❌ Low
 *  segments_N  Segment metadata              Small, random reads            ❌ Low
 *
 */

public interface ReadaheadContext extends Closeable {

    /**
     * Called on each segment load to update access pattern and possibly trigger readahead.
     *
     * @param fileOffset absolute file offset of the accessed segment
     * @param cacheMiss  true if the segment was not in the block cache
     */
    void onSegmentAccess(long fileOffset, boolean cacheMiss);

    /**
     * @return the current adaptive readahead window size (in segments)
     */
    int currentWindowSegments();

    /**
     * Trigger a readahead for the next window of segments.
     *
     * @param startSegmentIndex first segment index to prefetch
     */
    void triggerReadahead(int startSegmentIndex);

    /**
     * Reset the readahead state (e.g., after a large random seek or stream reset).
     */
    void reset();

    /**
     * Cancel any pending async prefetch for this stream.
     */
    void cancel();

    /**
     * @return true if readahead is currently enabled (based on cache misses and policy)
     */
    boolean isReadaheadEnabled();

    @Override
    void close();
}

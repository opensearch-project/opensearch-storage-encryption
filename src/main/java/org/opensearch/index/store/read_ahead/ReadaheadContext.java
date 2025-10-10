/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead;

import java.io.Closeable;

/**
 * Per-IndexInput readahead context that manages sequential access detection and triggers async prefetch operations.
 * 
 * <p>This interface represents the state and behavior for a single file stream's readahead operations.
 * Each IndexInput gets its own context to track access patterns, detect sequential reads, and coordinate
 * with the readahead system for optimal prefetching.
 * 
 * <p>The context maintains internal state for:
 * <ul>
 * <li><strong>Access pattern tracking:</strong> Records recent file offsets to detect sequential access</li>
 * <li><strong>Hit/miss statistics:</strong> Tracks cache performance to adapt readahead behavior</li>
 * <li><strong>Readahead policy:</strong> Associates with a policy that determines when and how much to prefetch</li>
 * <li><strong>State management:</strong> Provides reset and cancellation capabilities for lifecycle management</li>
 * </ul>
 * 
 * <p>Implementations should be thread-safe for concurrent access from IndexInput operations.
 * 
 * @opensearch.internal
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
     * Called on cache miss to update access pattern tracking and possibly trigger readahead operations.
     * 
     * <p>This method analyzes the access pattern based on the current and previous file offsets
     * to determine if a sequential access pattern is detected. If sequential access is identified
     * and the readahead policy determines it's appropriate, this will trigger prefetch operations.
     *
     * @param fileOffset the absolute file offset where the cache miss occurred
     */
    void onCacheMiss(long fileOffset);

    /**
     * Called on cache hits to track hit streaks for adaptive readahead behavior.
     * 
     * <p>This method updates internal statistics about cache performance, which can be used
     * by readahead policies to adapt their behavior. High cache hit rates may indicate
     * successful readahead operations and could influence future prefetch decisions.
     */
    void onCacheHit();

    /**
     * Manually triggers readahead operations starting from the specified file offset.
     * 
     * <p>This method bypasses the normal access pattern detection and forces readahead
     * to occur. It's typically used when the caller has knowledge that sequential
     * access will occur and wants to proactively start prefetching.
     * 
     * @param fileOffset the file offset to start readahead operations from
     */
    void triggerReadahead(long fileOffset);

    /**
     * Resets the readahead state to initial conditions.
     * 
     * <p>This method clears any accumulated access pattern state and resets internal
     * counters. It should be called after large random seeks, stream repositioning,
     * or other operations that break sequential access patterns.
     */
    void reset();

    /**
     * Cancels any pending asynchronous prefetch operations for this stream.
     * 
     * <p>This method immediately cancels any ongoing or queued readahead operations
     * associated with this context. It's typically called when the stream is closed
     * or when readahead is no longer needed.
     */
    void cancel();

    /**
     * Checks if readahead operations are currently enabled for this context.
     * 
     * <p>Readahead may be disabled due to random access patterns, policy decisions,
     * or explicit configuration. This method reflects the current operational state.
     * 
     * @return true if readahead is currently enabled and may trigger prefetch operations,
     *         false if readahead is disabled for this context
     */
    boolean isReadAheadEnabled();

    /**
     * Gets the readahead policy associated with this context.
     * 
     * <p>The policy determines readahead behavior such as when to trigger prefetch operations,
     * how much data to prefetch, and how to adapt to observed access patterns.
     * 
     * @return the readahead policy governing this context's prefetch behavior
     */
    ReadaheadPolicy policy();

    @Override
    void close();
}

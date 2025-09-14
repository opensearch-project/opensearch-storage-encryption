/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block;

/**
 * A functional interface that defines a callback to be invoked when a resource
 * is no longer in use and can be released.
 *
 * This is typically used in conjunction with reference-counted objects to implement
 * custom cleanup logic such as returning resources to a pool or closing
 * native resources.
 *
 * Implementations must be idempotent and thread-safe if used concurrently.
 * 
 * @param <T> the type of resource to be released
 */
@FunctionalInterface
public interface BlockReleaser<T> {

    /**
     * Releases the given resource. This method is called when
     * the reference count reaches zero.
     *
     * @param resource the resource to release
     */
    void release(T resource);
}

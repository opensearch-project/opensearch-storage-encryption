/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

/**
 * Controls whether the block cache is populated at write time or only at read time.
 *
 * @opensearch.internal
 */
public enum WriteCacheMode {
    /** Cache is populated during writes (current default behavior). */
    WRITE_THROUGH,
    /** Cache is only populated on reads; writes go to disk only. */
    READ_THROUGH;

    private static final String SYSTEM_PROPERTY = "tests.cacheMode";

    /**
     * Resolves the write cache mode from the {@code -Dtests.cacheMode} system property.
     * Returns {@link #WRITE_THROUGH} if the property is not set or unrecognized.
     */
    public static WriteCacheMode fromSystemProperty() {
        String value = System.getProperty(SYSTEM_PROPERTY);
        if (value == null) {
            return WRITE_THROUGH;
        }
        try {
            return WriteCacheMode.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            return WRITE_THROUGH;
        }
    }
}

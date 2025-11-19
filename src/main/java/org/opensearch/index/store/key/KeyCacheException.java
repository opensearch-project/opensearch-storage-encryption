/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.key;

/**
 * Exception thrown when key cache operations fail.
 * This exception can optionally suppress stack traces to reduce log spam
 * for expected failure scenarios (e.g., when keys are disabled or Master Key Provider is unavailable).
 * 
 * @opensearch.internal
 */
public class KeyCacheException extends RuntimeException {

    /**
     * Constructs a new KeyCacheException with the specified detail message and cause.
     * 
     * @param message the detail message
     * @param cause the cause of this exception
     * @param suppressStackTrace if true, stack trace generation is suppressed to reduce log spam
     */
    public KeyCacheException(String message, Throwable cause, boolean suppressStackTrace) {
        // Use the protected constructor that allows controlling stack trace generation
        // Parameters: message, cause, enableSuppression, writableStackTrace
        super(message, cause, true, !suppressStackTrace);
    }

    /**
     * Constructs a new KeyCacheException with the specified detail message and cause.
     * Stack trace generation is enabled by default.
     * 
     * @param message the detail message
     * @param cause the cause of this exception
     */
    public KeyCacheException(String message, Throwable cause) {
        this(message, cause, false);
    }

    /**
     * Extracts the root cause message from a nested exception chain.
     * Traverses the exception chain to find the deepest cause and returns its message.
     * Useful for presenting clean, actionable error messages to operators without
     * the noise of intermediate exception wrappers.
     * 
     * @param t the throwable to extract the root cause from
     * @return the message of the root cause, or the exception class name if no message exists
     */
    public static String extractRootCauseMessage(Throwable t) {
        if (t == null) {
            return "Unknown error";
        }

        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }

        String message = root.getMessage();
        return message != null ? message : root.getClass().getSimpleName();
    }
}

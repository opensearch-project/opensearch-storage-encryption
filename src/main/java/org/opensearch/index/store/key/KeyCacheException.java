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
 * <p>Also provides utilities for classifying exceptions into transient vs critical failures
 * to determine whether index blocks should be applied.
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

    /**
     * Classifies an exception from Master Key Provider operations into transient vs critical failures.
     * 
     * <p>Transient failures (throttling, rate limits, temporary network issues) should not trigger
     * index blocks as the system can continue using cached keys. Critical failures (disabled keys,
     * revoked keys, access denied) require blocks to protect data integrity.
     * 
     * @param e the exception to classify
     * @return TRANSIENT if the error is temporary, CRITICAL if it requires blocking
     */
    public static FailureType classify(Exception e) {
        if (e == null) {
            return FailureType.CRITICAL;
        }

        String message = e.getMessage();
        String rootMessage = extractRootCauseMessage(e);

        // Check for transient AWS KMS errors
        if (isTransientError(message, rootMessage)) {
            return FailureType.TRANSIENT;
        }

        // All other errors are considered critical (disabled key, access denied, etc.)
        return FailureType.CRITICAL;
    }

    /**
     * Checks if the error indicates a transient/throttling issue.
     */
    private static boolean isTransientError(String message, String rootMessage) {
        // Common AWS throttling and transient error patterns
        return containsAny(
            message,
            rootMessage,
            "ThrottlingException",
            "Rate exceeded",
            "RequestLimitExceeded",
            "TooManyRequestsException",
            "SlowDown",
            "ServiceUnavailableException",
            "InternalErrorException",
            "503 Service Unavailable"
        );
    }

    /**
     * Checks if any of the patterns appear in the message or root message (case-insensitive).
     */
    private static boolean containsAny(String message, String rootMessage, String... patterns) {
        if (message == null && rootMessage == null) {
            return false;
        }

        String combined = (message != null ? message : "") + " " + (rootMessage != null ? rootMessage : "");
        String combinedLower = combined.toLowerCase();

        for (String pattern : patterns) {
            if (combinedLower.contains(pattern.toLowerCase())) {
                return true;
            }
        }
        return false;
    }
}

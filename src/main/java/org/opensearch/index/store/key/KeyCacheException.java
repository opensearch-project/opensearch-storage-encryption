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
     * <p>Classification strategy (safer default):
     * <ol>
     *   <li>Check exception types in cause chain (AWS SDK exceptions like DisabledException)</li>
     *   <li>Check for CRITICAL error patterns in messages (disabled/revoked keys, access denied)</li>
     *   <li>Check for TRANSIENT error patterns in messages (throttling, rate limits)</li>
     *   <li>Default to TRANSIENT → safer for unknown errors (allows cached key usage)</li>
     * </ol>
     * 
     * <p>This approach minimizes false positive blocks. Exception types are checked first for
     * reliability, then message patterns for provider-agnostic support. Unknown errors are
     * assumed transient, which is safer since operations can continue using cached keys.
     * 
     * @param e the exception to classify
     * @return TRANSIENT if the error is temporary or unknown, CRITICAL if it requires blocking
     */
    public static FailureType classify(Exception e) {
        if (e == null) {
            return FailureType.CRITICAL;
        }

        // Check exception types in cause chain (most reliable)
        if (hasCriticalExceptionType(e)) {
            return FailureType.CRITICAL;
        }

        if (hasTransientExceptionType(e)) {
            return FailureType.TRANSIENT;
        }

        // Fallback or provider-agnostic support: Check message patterns
        String message = e.getMessage();
        String rootMessage = extractRootCauseMessage(e);

        if (isCriticalError(message, rootMessage)) {
            return FailureType.CRITICAL;
        }

        if (isTransientError(message, rootMessage)) {
            return FailureType.TRANSIENT;
        }

        // Default to TRANSIENT
        return FailureType.TRANSIENT;
    }

    /**
     * Checks if any exception in the cause chain is a critical AWS KMS exception type.
     * This is more reliable than string matching as exception types are stable.
     */
    private static boolean hasCriticalExceptionType(Throwable t) {
        return hasExceptionType(
            t,
            "DisabledException",
            "NotFoundException",
            "AccessDeniedException",
            "InvalidKeyUsageException",
            "KeyUnavailableException",
            "KMSInvalidStateException"
        );
    }

    /**
     * Checks if any exception in the cause chain is a transient AWS KMS exception type.
     */
    private static boolean hasTransientExceptionType(Throwable t) {
        return hasExceptionType(
            t,
            "ThrottlingException",
            "RequestLimitExceededException",
            "TooManyRequestsException",
            "ServiceUnavailableException",
            "InternalErrorException"
        );
    }

    /**
     * Traverses the exception cause chain looking for specific exception class names.
     * Uses simple name matching to work across different AWS SDK versions.
     */
    private static boolean hasExceptionType(Throwable t, String... classNames) {
        Throwable current = t;
        while (current != null) {
            String className = current.getClass().getSimpleName();
            for (String name : classNames) {
                if (className.equals(name)) {
                    return true;
                }
            }
            current = current.getCause();
            // Prevent infinite loops
            if (current == t) {
                break;
            }
        }
        return false;
    }

    /**
     * Checks if the error indicates a critical failure requiring immediate blocking.
     * Critical errors include disabled keys, revoked keys, access denied, and key not found.
     */
    private static boolean isCriticalError(String message, String rootMessage) {
        return containsAny(
            message,
            rootMessage,
            "DisabledException",
            "NotFoundException",
            "AccessDeniedException",
            "InvalidKeyUsageException",
            "KeyUnavailableException",
            "KMSInvalidStateException",
            "access denied",
            "unauthorized",
            "forbidden",
            "key not found",
            "invalid key",
            "key disabled",
            "key revoked"
        );
    }

    /**
     * Checks if the error indicates a transient/throttling issue.
     * Transient errors include rate limiting, throttling, and temporary service unavailability.
     */
    private static boolean isTransientError(String message, String rootMessage) {
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
            "503 Service Unavailable",
            "Connection timeout",
            "Network error"
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

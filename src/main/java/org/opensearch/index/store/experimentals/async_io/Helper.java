/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.experimentals.async_io;

import java.util.concurrent.CompletableFuture;

import io.netty.channel.unix.Errors;

/**
 * Utility class providing helper methods for async I/O operations and CompletableFuture handling.
 * 
 * <p>This class contains common patterns used in async I/O scenarios:
 * <ul>
 * <li>Syscall result transformation and error handling</li>
 * <li>Creating failed CompletableFutures for exception cases</li>
 * <li>Integration with Netty's error handling mechanisms</li>
 * </ul>
 * 
 * <p>All methods are static utility methods and this class should not be instantiated.
 * 
 * @opensearch.internal
 */
public class Helper {

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private Helper() {}

    /**
     * Transforms a syscall result CompletableFuture to handle negative return values as errors.
     * 
     * <p>Unix/Linux syscalls typically return negative values to indicate errors. This method
     * converts such negative return values into IOException exceptions using Netty's error
     * handling utilities.
     * 
     * @param method the name of the syscall method for error reporting
     * @param future the CompletableFuture containing the raw syscall result
     * @return a CompletableFuture that completes normally with the result if positive,
     *         or exceptionally with an IOException if the result is negative
     */
    public static CompletableFuture<Integer> syscallTransform(String method, CompletableFuture<Integer> future) {
        return future.thenCompose(syscall -> {
            if (syscall < 0) {
                return failureFuture(Errors.newIOException(method, syscall));
            } else {
                return CompletableFuture.completedFuture(syscall);
            }
        });
    }

    /**
     * Creates a CompletableFuture that is already completed exceptionally with the given throwable.
     * 
     * <p>This is a utility method that provides the equivalent of CompletableFuture.failedFuture()
     * which was not available in Java 8. The returned future will immediately fail with the
     * provided exception when accessed.
     * 
     * @param <T> the type parameter for the CompletableFuture
     * @param throwable the exception that the future should complete with
     * @return a CompletableFuture that is already completed exceptionally
     */
    public static <T> CompletableFuture<T> failureFuture(Throwable throwable) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(throwable);
        return future;
    }
}

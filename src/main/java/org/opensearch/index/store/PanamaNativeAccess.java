/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;

/**
 * Utility class for accessing native POSIX functions via Panama Foreign Function &amp; Memory API.
 */
@SuppressForbidden(reason = "Uses Panama FFI for native function access")
@SuppressWarnings("preview")
public final class PanamaNativeAccess {
    private static final Logger LOGGER = LogManager.getLogger(PanamaNativeAccess.class);
    private static final Linker LINKER = Linker.nativeLinker();

    private static final MethodHandle GET_PAGE_SIZE;
    private static final MethodHandle OPEN;
    private static final MethodHandle CLOSE;
    private static final MethodHandle POSIX_FADVISE;

    private static final SymbolLookup LIBC = LINKER.defaultLookup();

    private static final int POSIX_FADV_DONTNEED = 4;
    private static final int O_RDONLY = 0;

    // Prevent instantiation
    private PanamaNativeAccess() {
        throw new AssertionError("Utility class - do not instantiate");
    }

    static {
        try {
            GET_PAGE_SIZE = LINKER.downcallHandle(LIBC.find("getpagesize").orElseThrow(), FunctionDescriptor.of(ValueLayout.JAVA_INT));

            OPEN = LINKER
                .downcallHandle(
                    LIBC.find("open").orElseThrow(),
                    FunctionDescriptor
                        .of(
                            ValueLayout.JAVA_INT,
                            ValueLayout.ADDRESS, // const char *pathname
                            ValueLayout.JAVA_INT // int flags
                        )
                );

            CLOSE = LINKER
                .downcallHandle(
                    LIBC.find("close").orElseThrow(),
                    FunctionDescriptor
                        .of(
                            ValueLayout.JAVA_INT,
                            ValueLayout.JAVA_INT // int fd
                        )
                );

            POSIX_FADVISE = LINKER
                .downcallHandle(
                    LIBC.find("posix_fadvise").orElseThrow(),
                    FunctionDescriptor
                        .of(
                            ValueLayout.JAVA_INT, // return int
                            ValueLayout.JAVA_INT, // int fd
                            ValueLayout.JAVA_LONG, // offset
                            ValueLayout.JAVA_LONG, // len
                            ValueLayout.JAVA_INT // advice
                        )
                );

        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to load native functions", e);
        }
    }

    /**
     * Returns the system page size in bytes.
     * Thread-safe. Falls back to 4096 if native call fails.
     *
     * @return page size in bytes (typically 4096)
     */
    public static int getPageSize() {
        try {
            return (int) GET_PAGE_SIZE.invokeExact();
        } catch (Throwable e) {
            return 4096; // fallback to common page size
        }
    }

    /**
     * Advises the kernel to drop page cache for the specified file.
     * This is a best-effort operation and failures are logged but not propagated.
     * Thread-safe.
     *
     * @param filePath absolute path to the file
     * @return true if cache was successfully dropped, false otherwise
     */
    public static boolean dropFileCache(String filePath) {
        if (filePath == null || filePath.isEmpty()) {
            return false;
        }

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment cPath = arena.allocateUtf8String(filePath);

            int fd = (int) OPEN.invoke(cPath, O_RDONLY);
            if (fd < 0) {
                return false;
            }

            try {
                // 0, 0 means "entire file" - let kernel drop all cached pages
                int rc = (int) POSIX_FADVISE.invoke(fd, 0L, 0L, POSIX_FADV_DONTNEED);
                if (rc != 0) {
                    LOGGER.warn("posix_fadvise failed with rc={} for file: {}", rc, filePath);
                }
                return rc == 0;
            } finally {
                CLOSE.invoke(fd);
            }
        } catch (Throwable t) {
            // Best-effort operation: file may be deleted, permissions changed, etc.
            // Failures are expected and should not affect application functionality
            return false;
        }
    }
}

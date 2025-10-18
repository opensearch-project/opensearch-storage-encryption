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
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;

/**
 * Utility class for accessing native POSIX and libc functions via Panama
 * Foreign Function &amp; Memory API. Includes wrappers for getpagesize,
 * open/close, malloc/free, and optional posix_fadvise.
 *
 * <p>
 * Falls back to safe defaults if Panama FFI is not available.
 */
@SuppressForbidden(reason = "Uses Panama FFI for native function access")
@SuppressWarnings("preview")
public final class PanamaNativeAccess {

    private static final Logger LOGGER = LogManager.getLogger(PanamaNativeAccess.class);

    private static final boolean NATIVE_ACCESS_AVAILABLE;
    private static final MethodHandle GET_PAGE_SIZE;
    private static final MethodHandle OPEN;
    private static final MethodHandle CLOSE;
    private static final Optional<MethodHandle> POSIX_FADVISE;
    private static final MethodHandle MH_MALLOC;
    private static final MethodHandle MH_FREE;

    private static final int POSIX_FADV_DONTNEED = 4;
    private static final int O_RDONLY = 0;
    private static final int FALLBACK_PAGE_SIZE = 4096;

    private PanamaNativeAccess() {}

    static {
        boolean available = false;
        MethodHandle getPageSize = null;
        MethodHandle open = null;
        MethodHandle close = null;
        MethodHandle malloc = null;
        MethodHandle free = null;
        Optional<MethodHandle> posixFadviseOpt;

        try {
            Linker linker = Linker.nativeLinker();
            SymbolLookup libc = linker.defaultLookup();

            getPageSize = linker.downcallHandle(libc.find("getpagesize").orElseThrow(), FunctionDescriptor.of(ValueLayout.JAVA_INT));

            open = linker
                .downcallHandle(
                    libc.find("open").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT)
                );

            close = linker
                .downcallHandle(libc.find("close").orElseThrow(), FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));

            malloc = linker
                .downcallHandle(libc.find("malloc").orElseThrow(), FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

            free = linker.downcallHandle(libc.find("free").orElseThrow(), FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

            available = true;

            Optional<MethodHandle> tmpPosix = libc.find("posix_fadvise").flatMap(sym -> {
                try {
                    MethodHandle mh = linker
                        .downcallHandle(
                            sym,
                            FunctionDescriptor
                                .of(
                                    ValueLayout.JAVA_INT,
                                    ValueLayout.JAVA_INT,
                                    ValueLayout.JAVA_LONG,
                                    ValueLayout.JAVA_LONG,
                                    ValueLayout.JAVA_INT
                                )
                        );
                    return Optional.of(mh);
                } catch (Throwable t) {
                    return Optional.empty();
                }
            });
            posixFadviseOpt = tmpPosix;
        } catch (Throwable e) {
            LOGGER
                .warn(
                    "Panama FFM API not available; native calls will use fallback implementations. "
                        + "Start JVM with --enable-native-access=ALL-UNNAMED",
                    e
                );
            posixFadviseOpt = Optional.empty();

        }

        NATIVE_ACCESS_AVAILABLE = available;
        GET_PAGE_SIZE = getPageSize;
        OPEN = open;
        CLOSE = close;
        POSIX_FADVISE = posixFadviseOpt;

        MH_MALLOC = malloc;
        MH_FREE = free;
    }

    /**
     * Returns true if Panama FFI native access was successfully initialized.
     */
    public static boolean isAvailable() {
        return NATIVE_ACCESS_AVAILABLE;
    }

    /**
     * Returns the system page size in bytes, or 4096 on fallback.
     */
    public static int getPageSize() {
        if (!NATIVE_ACCESS_AVAILABLE) {
            return FALLBACK_PAGE_SIZE;
        }
        try {
            return (int) GET_PAGE_SIZE.invokeExact();
        } catch (Throwable e) {
            LOGGER.debug("Failed to get page size via native call, using fallback", e);
            return FALLBACK_PAGE_SIZE;
        }
    }

    /**
     * Advises the kernel to drop page cache for the specified file (no-op if
     * unsupported).
     */
    public static boolean dropFileCache(String filePath) {
        if (!NATIVE_ACCESS_AVAILABLE || filePath == null || filePath.isEmpty()) {
            return false;
        }

        // No-op if posix_fadvise not present
        if (POSIX_FADVISE.isEmpty()) {
            return false;
        }

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment cPath = arena.allocateUtf8String(filePath);
            int fd = (int) OPEN.invoke(cPath, O_RDONLY);
            if (fd < 0) {
                return false;
            }

            try {
                int rc = (int) POSIX_FADVISE.get().invoke(fd, 0L, 0L, POSIX_FADV_DONTNEED);
                if (rc != 0) {
                    LOGGER.warn("posix_fadvise() failed with rc={} for file: {}", rc, filePath);
                }
                return rc == 0;
            } finally {
                CLOSE.invoke(fd);
            }
        } catch (Throwable t) {
            LOGGER.debug("dropFileCache() failed for {}", filePath, t);
            return false;
        }
    }

    /**
     * Allocates native memory via libc malloc(size).
     */
    public static MemorySegment malloc(long size) {
        if (!NATIVE_ACCESS_AVAILABLE) {
            throw new UnsupportedOperationException("Panama FFI not available");
        }
        try {
            MemorySegment addr = (MemorySegment) MH_MALLOC.invoke(size);
            if (addr.address() == 0L) {
                throw new OutOfMemoryError("malloc(" + size + ") returned NULL");
            }
            return MemorySegment.ofAddress(addr.address()).reinterpret(size);
        } catch (Throwable t) {
            throw new RuntimeException("malloc failed", t);
        }
    }

    /**
     * Frees native memory allocated via malloc().
     */
    public static void free(MemorySegment segment) {
        if (segment == null || !NATIVE_ACCESS_AVAILABLE) {
            return;
        }
        try {
            MH_FREE.invoke(segment);
        } catch (Throwable t) {
            LOGGER.warn("free() failed", t);
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import java.io.IOException;
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
import org.opensearch.index.store.directio.BufferIOWithCaching;

@SuppressForbidden(reason = "temporary bypass")
@SuppressWarnings("preview")
public class PanamaNativeAccess {
    private static final Logger LOGGER = LogManager.getLogger(BufferIOWithCaching.class);
    private static final Linker LINKER = Linker.nativeLinker();

    public static final MethodHandle MMAP;
    private static final MethodHandle MADVISE;
    public static final MethodHandle MPROTECT;
    private static final MethodHandle GET_PAGE_SIZE;
    private static final MethodHandle OPEN;
    private static final MethodHandle CLOSE;
    public static final MethodHandle POSIX_MEMALIGN;
    public static final MethodHandle READ;
    public static final MethodHandle PREAD;
    public static final MethodHandle PWRITE;
    public static final MethodHandle POSIX_FADVISE;

    private static final SymbolLookup LIBC = LINKER.defaultLookup();

    public static final int MADV_WILLNEED = 3;
    public static final int PROT_READ = 0x1;
    public static final int PROT_WRITE = 0x2;
    public static final int MAP_PRIVATE = 0x02;
    public static final int POSIX_FADV_DONTNEED = 4;

    public static final int O_RDONLY = 0;
    public static final int O_DIRECT = 040000;
    public static final int O_SYNC = 04010000;

    static {
        try {
            // First try to find mmap
            Optional<MemorySegment> mmapSymbol = LIBC.find("mmap");
            if (mmapSymbol.isEmpty()) {
                // If mmap is not found, try mmap64 on some systems
                mmapSymbol = LIBC.find("mmap64");
            }

            if (mmapSymbol.isEmpty()) {
                throw new RuntimeException("Could not find mmap or mmap64 symbol");
            }

            MMAP = LINKER
                .downcallHandle(
                    mmapSymbol.get(),
                    FunctionDescriptor
                        .of(
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS, // addr
                            ValueLayout.JAVA_LONG, // length
                            ValueLayout.JAVA_INT, // prot
                            ValueLayout.JAVA_INT, // flags
                            ValueLayout.JAVA_INT, // fd
                            ValueLayout.JAVA_LONG // offset
                        )
                );

            MADVISE = LINKER
                .downcallHandle(
                    LIBC.find("madvise").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT)
                );

            MPROTECT = LINKER
                .downcallHandle(
                    LIBC.find("mprotect").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT)
                );

            GET_PAGE_SIZE = LINKER.downcallHandle(LIBC.find("getpagesize").orElseThrow(), FunctionDescriptor.of(ValueLayout.JAVA_INT));

            POSIX_MEMALIGN = LINKER
                .downcallHandle(
                    LIBC.find("posix_memalign").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
                );

            READ = LINKER
                .downcallHandle(
                    LIBC.find("read").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
                );

            PREAD = LINKER
                .downcallHandle(
                    LIBC.find("pread").orElseThrow(),
                    FunctionDescriptor
                        .of(
                            ValueLayout.JAVA_LONG,  // ssize_t
                            ValueLayout.JAVA_INT,   // int fd
                            ValueLayout.ADDRESS,    // void *buf
                            ValueLayout.JAVA_LONG,  // size_t count
                            ValueLayout.JAVA_LONG
                        )  // off_t offset
                );

            PWRITE = LINKER
                .downcallHandle(
                    LIBC.find("pwrite").orElseThrow(),
                    FunctionDescriptor
                        .of(
                            ValueLayout.JAVA_LONG,   // return ssize_t
                            ValueLayout.JAVA_INT,    // int fd
                            ValueLayout.ADDRESS,     // const void *buf
                            ValueLayout.JAVA_LONG,   // size_t count
                            ValueLayout.JAVA_LONG
                        )   // off_t offset
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
            throw new RuntimeException("Failed to load mmap", e);
        }
    }

    public static int getPageSize() {
        try {
            return (int) GET_PAGE_SIZE.invokeExact();
        } catch (Throwable e) {
            return 4096; // fallback to common page size
        }
    }

    public static void madvise(long address, long length, int advice) throws Throwable {
        int rc = (int) MADVISE.invokeExact(MemorySegment.ofAddress(address), length, advice);
        if (rc != 0) {
            throw new RuntimeException("madvise failed with rc=" + rc);
        }
    }

    static {
        try {
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
        } catch (Throwable e) {
            throw new RuntimeException("Failed to bind open/close", e);
        }
    }

    public static int openFile(String path) throws Throwable {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment pathSegment = arena.allocateFrom(path);
            return (int) OPEN.invoke(pathSegment, 0); // O_RDONLY = 0
        }
    }

    public static void pwrite(int fd, MemorySegment segment, long length, long offset) throws IOException {
        try {
            long written = (long) PWRITE.invokeExact(fd, segment.address(), length, offset);
            if (written != length) {
                throw new IOException("pwrite wrote only " + written + " of " + length + " bytes");
            }
        } catch (Throwable t) {
            throw new IOException("pwrite failed", t);
        }
    }

    public static void closeFile(int fd) throws Throwable {
        CLOSE.invoke(fd);
    }

    public static boolean dropFileCache(String filePath) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment cPath = arena.allocateFrom(filePath);

            int fd = (int) OPEN.invoke(cPath, O_RDONLY);
            if (fd < 0) {
                return false; // Cannot open file - may already be deleted
            }

            try {
                // 0, 0 means "entire file" - let kernel drop all cached pages
                int rc = (int) POSIX_FADVISE.invoke(fd, 0L, 0L, POSIX_FADV_DONTNEED);
                if (rc != 0) {
                    LOGGER.warn("posix_fadvise failed with rc={} for file: {}", rc, filePath);
                }
                return rc == 0; // Success if posix_fadvise returns 0
            } finally {
                CLOSE.invoke(fd);
            }
        } catch (Throwable t) {
            // Best-effort operation: file may be deleted, permissions changed, etc.
            // This is expected and should not affect application functionality
            return false;
        }
    }
}

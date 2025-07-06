/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.mmap;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.Optional;

import org.opensearch.common.SuppressForbidden;

@SuppressForbidden(reason = "temporary bypass")
@SuppressWarnings("preview")
public class PanamaNativeAccess {
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

    public static final int O_RDONLY = 0;
    public static final int O_DIRECT = 040000;
    public static final int O_SYNC = 04010000;

    private static final SymbolLookup LIBC = loadLibc();

    public static final int MADV_WILLNEED = 3;
    public static final int PROT_READ = 0x1;
    public static final int PROT_WRITE = 0x2;
    public static final int MAP_PRIVATE = 0x02;

    private static SymbolLookup loadLibc() {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("mac")) {
            return SymbolLookup.libraryLookup("/usr/lib/libSystem.B.dylib", Arena.global());
        } else if (os.contains("linux")) {
            try {
                // Try the 64-bit version first
                return SymbolLookup.libraryLookup("/lib64/libc.so.6", Arena.global());
            } catch (Exception e) {
                try {
                    // Fall back to the 32-bit version
                    return SymbolLookup.libraryLookup("/lib/libc.so.6", Arena.global());
                } catch (Exception e2) {
                    throw new RuntimeException("Could not load libc from either /lib64/libc.so.6 or /lib/libc.so.6", e2);
                }
            }
        } else {
            throw new UnsupportedOperationException("Unsupported OS: " + os);
        }
    }

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
            MemorySegment pathSegment = arena.allocateUtf8String(path);
            return (int) OPEN.invoke(pathSegment, 0); // O_RDONLY = 0
        }
    }

    public static int openFileWithODirect(String path, boolean direct, Arena arena) throws Throwable {
        MemorySegment cPath = arena.allocateUtf8String(path);
        int flags = O_RDONLY | (direct ? O_DIRECT : 0);

        return (int) OPEN.invoke(cPath, flags);
    }

    public static void closeFile(int fd) throws Throwable {
        CLOSE.invoke(fd);
    }

}

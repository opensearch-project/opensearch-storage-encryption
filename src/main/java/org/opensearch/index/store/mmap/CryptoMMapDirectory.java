/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.mmap;

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.cipher.CipherFactory;
import org.opensearch.index.store.iv.KeyIvResolver;

@SuppressWarnings("preview")
@SuppressForbidden(reason = "temporary bypass")
public final class CryptoMMapDirectory extends MMapDirectory {

    private final KeyIvResolver keyIvResolver;

    private static final Linker LINKER = Linker.nativeLinker();
    private static final int PROT_READ = 0x1;
    private static final int PROT_WRITE = 0x2;
    private static final int MAP_PRIVATE = 0x02;
    private static final MethodHandle MMAP;
    private static final SymbolLookup LIBC = loadLibc();

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
        } catch (Throwable e) {
            throw new RuntimeException("Failed to load mmap", e);
        }
    }

    public CryptoMMapDirectory(Path path, Provider provider, KeyIvResolver keyIvResolver) throws IOException {
        super(path);
        this.keyIvResolver = keyIvResolver;
    }

    /**
     * Sets the preload predicate based on file extension list.
     *
     * @param preLoadExtensions extensions to preload (e.g., ["dvd", "tim",
     * "*"])
     * @throws IOException if preload configuration fails
     */
    public void setPreloadExtensions(Set<String> preLoadExtensions) throws IOException {
        if (!preLoadExtensions.isEmpty()) {
            this.setPreload(createPreloadPredicate(preLoadExtensions));
        }
    }

    private static BiPredicate<String, IOContext> createPreloadPredicate(Set<String> preLoadExtensions) {
        if (preLoadExtensions.contains("*")) {
            return MMapDirectory.ALL_FILES;
        }
        return (fileName, context) -> {
            int dotIndex = fileName.lastIndexOf('.');
            if (dotIndex > 0) {
                String ext = fileName.substring(dotIndex + 1);
                return preLoadExtensions.contains(ext);
            }
            return false;
        };
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        ensureCanRead(name);

        Path file = getDirectory().resolve(name);
        long size = Files.size(file);
        boolean confined = context == IOContext.READONCE;
        Arena arena = confined ? Arena.ofConfined() : Arena.ofShared();
        int chunkSizePower = 34;

        try {
            // Open the file using native open() call
            int fd = openFile(file.toString());
            if (fd == -1) {
                throw new IOException("Failed to open file: " + file);
            }

            try {
                MemorySegment[] segments = mmapAndDecrypt(file, fd, size, arena, chunkSizePower);
                return MemorySegmentIndexInput
                    .newInstance("CryptoMemorySegmentIndexInput(path=\"" + file + "\")", arena, segments, size, chunkSizePower);
            } finally {
                // Close the file descriptor
                closeFile(fd);
            }

        } catch (Throwable t) {
            arena.close();
            throw new IOException("Failed to mmap/decrypt " + file, t);
        }
    }

    public MemorySegment[] mmapAndDecrypt(Path path, int fd, long size, Arena arena, int chunkSizePower) throws Throwable {
        final long chunkSize = 1L << chunkSizePower;
        final int numSegments = (int) (size >>> chunkSizePower) + 1;
        MemorySegment[] segments = new MemorySegment[numSegments];

        long offset = 0;
        for (int i = 0; i < numSegments; i++) {
            long remaining = size - offset;
            long segmentSize = Math.min(chunkSize, remaining);

            // Direct mmap call
            MemorySegment addr = (MemorySegment) MMAP
                .invoke(MemorySegment.NULL, segmentSize, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, offset);

            if (addr.address() == 0 || addr.address() == -1) {
                throw new IOException("mmap failed at offset: " + offset);
            }

            // Create segment directly in the arena's scope
            MemorySegment segment = MemorySegment.ofAddress(addr.address()).reinterpret(segmentSize, arena, null);

            // Decrypt in place
            decryptSegment(segment, offset);

            segments[i] = segment;
            offset += segmentSize;
        }

        return segments;
    }

    private void decryptSegment(MemorySegment segment, long offset) throws Exception {
        final byte[] key = this.keyIvResolver.getDataKey().getEncoded();
        final byte[] baseIv = this.keyIvResolver.getIvBytes();

        Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
        SecretKeySpec keySpec = new SecretKeySpec(key, "AES");
        byte[] ivCopy = Arrays.copyOf(baseIv, baseIv.length);

        int blockOffset = (int) (offset / CipherFactory.AES_BLOCK_SIZE_BYTES);
        for (int i = CipherFactory.IV_ARRAY_LENGTH - 1; i >= CipherFactory.IV_ARRAY_LENGTH - CipherFactory.COUNTER_SIZE_BYTES; i--) {
            ivCopy[i] = (byte) blockOffset;
            blockOffset >>>= Byte.SIZE;
        }

        cipher.init(Cipher.DECRYPT_MODE, keySpec, new IvParameterSpec(ivCopy));

        // Process the data in smaller chunks to avoid OOM
        ByteBuffer buffer = segment.asByteBuffer();
        final int CHUNK_SIZE = 8192; // 8KB chunks
        byte[] chunk = new byte[CHUNK_SIZE];

        int position = 0;
        while (position < buffer.capacity()) {
            int size = Math.min(CHUNK_SIZE, buffer.capacity() - position);
            buffer.position(position);
            buffer.get(chunk, 0, size);

            byte[] decrypted;
            if (position + size >= buffer.capacity()) {
                // Last chunk
                decrypted = cipher.doFinal(chunk, 0, size);
            } else {
                decrypted = cipher.update(chunk, 0, size);
            }

            if (decrypted != null) {
                buffer.position(position);
                buffer.put(decrypted);
            }

            position += size;
        }
    }

    private static final MethodHandle OPEN;
    private static final MethodHandle CLOSE;

    static {
        try {
            OPEN = LINKER
                .downcallHandle(
                    LIBC.find("open").orElseThrow(),
                    FunctionDescriptor
                        .of(
                            ValueLayout.JAVA_INT,
                            ValueLayout.ADDRESS,    // const char *pathname
                            ValueLayout.JAVA_INT    // int flags
                        )
                );

            CLOSE = LINKER
                .downcallHandle(
                    LIBC.find("close").orElseThrow(),
                    FunctionDescriptor
                        .of(
                            ValueLayout.JAVA_INT,
                            ValueLayout.JAVA_INT    // int fd
                        )
                );
        } catch (Throwable e) {
            throw new RuntimeException("Failed to bind open/close", e);
        }
    }

    private static int openFile(String path) throws Throwable {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment pathSegment = arena.allocateUtf8String(path);
            return (int) OPEN.invoke(pathSegment, 0); // O_RDONLY = 0
        }
    }

    private static void closeFile(int fd) throws Throwable {
        CLOSE.invoke(fd);
    }
}

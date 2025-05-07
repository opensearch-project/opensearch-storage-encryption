package org.opensearch.index.store;

import java.io.IOException;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.channels.FileChannel;
import java.security.SecureRandom;
import java.util.Optional;



import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.store.*;

public final class CryptoMMapDirectory extends MMapDirectory {

    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup LIBC = SymbolLookup.libraryLookup("c", Arena.global());

    private static final int PROT_READ  = 0x1;
    private static final int PROT_WRITE = 0x2;
    private static final int MAP_PRIVATE = 0x02;

    private static final MethodHandle MMAP;
    private static final MethodHandle MUNMAP;

    static {
        try {
            MMAP = LINKER.downcallHandle(
                LIBC.find("mmap").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.ADDRESS, // return: void*
                    ValueLayout.ADDRESS, // addr
                    ValueLayout.JAVA_LONG, // length
                    ValueLayout.JAVA_INT, // prot
                    ValueLayout.JAVA_INT, // flags
                    ValueLayout.JAVA_INT, // fd
                    ValueLayout.JAVA_LONG) // offset
            );

            MUNMAP = LINKER.downcallHandle(
                LIBC.find("munmap").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
            );
        } catch (Throwable e) {
            throw new RuntimeException("Failed to load mmap/munmap", e);
        }
    }

    public CryptoMMapDirectory(Path path) throws IOException {
        super(path);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        ensureCanRead(name);

        Path path = directory.resolve(name);
        long fileSize = java.nio.file.Files.size(path);
        String resourceDescription = "CryptoMemorySegmentIndexInput(path=\"" + path + "\")";

        // Load key and IV (replace with your real logic)
        byte[] key = generateDummyKey();
        byte[] iv = generateDummyIV();

        try (var fc = FileChannel.open(path, StandardOpenOption.READ)) {
            int fd = getFD(fc);

            try (Arena arena = Arena.ofConfined()) {
                MemorySegment addr = (MemorySegment) MMAP.invoke(
                    MemorySegment.NULL,
                    fileSize,
                    PROT_READ | PROT_WRITE,
                    MAP_PRIVATE,
                    fd,
                    0L
                );

                if (addr.address() == 0 || addr.address() == -1) {
                    throw new IOException("mmap failed");
                }

                MemorySegment segment = MemorySegment.ofAddress(addr.address(), fileSize, arena.scope());

                // 🔐 Perform in-place decryption
                decryptInPlace(segment, key, iv);

                return MemorySegmentIndexInput.newInstance(
                    resourceDescription,
                    arena,
                    new MemorySegment[] { segment },
                    fileSize,
                    chunkSizePower,
                    true // confined
                );
            }
        } catch (Throwable t) {
            throw new IOException("Failed to mmap and decrypt", t);
        }
    }

   private void decryptInPlace(MemorySegment segment, byte[] key, byte[] iv) {
    try {
        Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
        SecretKeySpec keySpec = new SecretKeySpec(key, "AES");
        IvParameterSpec ivSpec = new IvParameterSpec(iv);

        cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec);

        // Read content into a temp buffer
        byte[] input = new byte[(int) segment.byteSize()];
        segment.copyTo(MemorySegment.ofArray(input));

        // Decrypt
        byte[] output = cipher.doFinal(input);

        // Copy decrypted bytes back into mapped segment
        segment.asByteBuffer().put(output);
    } catch (Exception e) {
        throw new RuntimeException("AES decryption failed", e);
    }
}

    private byte[] generateDummyKey() {
        return new byte[32]; // replace with your real key
    }

    private byte[] generateDummyIV() {
        return new byte[16]; // replace with your real IV
    }

    private int getFD(FileChannel channel) {
        try {
            var fdField = FileChannel.class.getDeclaredField("fd");
            fdField.setAccessible(true);
            Object fdObj = fdField.get(channel);

            var fdValField = fdObj.getClass().getDeclaredField("fd");
            fdValField.setAccessible(true);
            return fdValField.getInt(fdObj);
        } catch (Exception e) {
            throw new RuntimeException("Unable to get file descriptor from FileChannel", e);
        }
    }
}

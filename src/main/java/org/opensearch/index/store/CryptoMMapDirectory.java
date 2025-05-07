package org.opensearch.index.store;

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
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.SuppressForbidden;

@SuppressForbidden(reason = "temporary bypass")
public final class CryptoMMapDirectory extends MMapDirectory {

  private static final Linker LINKER = Linker.nativeLinker();
  private static final SymbolLookup LIBC = SymbolLookup.libraryLookup("c", Arena.global());

  private static final int PROT_READ = 0x1;
  private static final int PROT_WRITE = 0x2;
  private static final int MAP_PRIVATE = 0x02;

  private static final MethodHandle MMAP;

  static {
    try {
      MMAP =
          LINKER.downcallHandle(
              LIBC.find("mmap").orElseThrow(),
              FunctionDescriptor.of(
                  ValueLayout.ADDRESS,
                  ValueLayout.ADDRESS, // addr
                  ValueLayout.JAVA_LONG, // length
                  ValueLayout.JAVA_INT, // prot
                  ValueLayout.JAVA_INT, // flags
                  ValueLayout.JAVA_INT, // fd
                  ValueLayout.JAVA_LONG // offset
                  ));
    } catch (Throwable e) {
      throw new RuntimeException("Failed to load mmap", e);
    }
  }

  public CryptoMMapDirectory(Path path) throws IOException {
    super(path);
  }

  public static MemorySegment mmapAndDecrypt(
      Path path, int fd, long size, Arena arena, byte[] key, byte[] iv) throws Throwable {
    MemorySegment addr =
        (MemorySegment)
            MMAP.invoke(MemorySegment.NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0L);

    if (addr.address() == 0 || addr.address() == -1) {
      throw new IOException("mmap failed");
    }

    MemorySegment segment = MemorySegment.ofAddress(addr.address());
    decryptInPlace(segment, key, iv);
    return segment;
  }

  // TODO: This can be invoked via FFI for zero copy.
  private static void decryptInPlace(MemorySegment segment, byte[] key, byte[] iv)
      throws Exception {
    try {
      Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
      SecretKeySpec keySpec = new SecretKeySpec(key, "AES");
      IvParameterSpec ivSpec = new IvParameterSpec(iv);
      cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec);

      // Read from segment into byte[] (source)
      ByteBuffer buffer = segment.asByteBuffer();
      byte[] input = new byte[buffer.remaining()];
      buffer.get(input);

      // Decrypt
      byte[] output = cipher.doFinal(input);

      // Write back decrypted data into the segment
      buffer.rewind();
      buffer.put(output);
    } catch (Exception e) {
      throw new RuntimeException("AES decryption failed", e);
    }
  }

  public static int getFD(FileChannel channel) {
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

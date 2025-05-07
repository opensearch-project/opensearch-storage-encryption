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

package org.apache.lucene.store;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.opensearch.index.store.CryptoMMapDirectory;

public final class CryptoIndexInputProvider
    implements MMapDirectory.MMapIndexInputProvider<
        ConcurrentHashMap<String, RefCountedSharedArena>> {

  @Override
  public IndexInput openInput(
      Path path,
      IOContext context,
      int chunkSizePower,
      boolean preload,
      Optional<String> group,
      ConcurrentHashMap<String, RefCountedSharedArena> arenas)
      throws IOException {
    long size = java.nio.file.Files.size(path);
    String resourceDescription = "CryptoMemorySegmentIndexInput(path=\"" + path + "\")";

    byte[] key = new byte[32]; // TODO: Load actual key
    byte[] iv = new byte[16]; // TODO: Load actual IV

    try (FileChannel fc = FileChannel.open(path, StandardOpenOption.READ);
        Arena arena = Arena.ofConfined()) {

      int fd = CryptoMMapDirectory.getFD(fc);
      MemorySegment segment = CryptoMMapDirectory.mmapAndDecrypt(path, fd, size, arena, key, iv);

      return MemorySegmentIndexInput.newInstance(
          resourceDescription,
          arena,
          new MemorySegment[] {segment},
          size,
          chunkSizePower,
          context == IOContext.READONCE);
    } catch (Throwable t) {
      throw new IOException("Failed to mmap and decrypt", t);
    }
  }

  @Override
  public long getDefaultMaxChunkSize() {
    return 1L << 30; // 1 GiB
  }

  @Override
  public boolean supportsMadvise() {
    return false;
  }

  @Override
  public ConcurrentHashMap<String, RefCountedSharedArena> attachment() {
    return new ConcurrentHashMap<>();
  }
}

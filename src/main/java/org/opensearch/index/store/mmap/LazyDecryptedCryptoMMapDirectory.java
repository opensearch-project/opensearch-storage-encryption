/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.mmap;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.Provider;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.cipher.EncryptionMetadataTrailer;
import org.opensearch.index.store.concurrency.RefCountedSharedArena;
import org.opensearch.index.store.key.KeyResolver;

@SuppressWarnings("preview")
@SuppressForbidden(reason = "temporary bypass")
public final class LazyDecryptedCryptoMMapDirectory extends MMapDirectory {

    private static final Logger LOGGER = LogManager.getLogger(LazyDecryptedCryptoMMapDirectory.class);

    private final KeyResolver keyResolver;

    private Function<String, Optional<String>> groupingFunction = GROUP_BY_SEGMENT;
    private final ConcurrentHashMap<String, RefCountedSharedArena> arenas = new ConcurrentHashMap<>();

    private static final int SHARED_ARENA_PERMITS = checkMaxPermits(getSharedArenaMaxPermitsSysprop());

    public LazyDecryptedCryptoMMapDirectory(Path path, Provider provider, KeyResolver keyResolver) throws IOException {
        super(path);
        this.keyResolver = keyResolver;
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

    /**
    * Configures a grouping function for files that are part of the same logical group. 
    * The gathering of files into a logical group is a hint that allows for better 
    * handling of resources.
    *
    * <p>By default, grouping is {@link #GROUP_BY_SEGMENT}. To disable, invoke this 
    * method with {@link #NO_GROUPING}.
    *
    * @param groupingFunction a function that accepts a file name and returns an 
    *     optional group key. If the optional is present, then its value is the 
    *     logical group to which the file belongs. Otherwise, the file name is not 
    *     associated with any logical group.
    */
    public void setGroupingFunction(Function<String, Optional<String>> groupingFunction) {
        this.groupingFunction = groupingFunction;
    }

    /**
     * Gets the current grouping function.
     */
    public Function<String, Optional<String>> getGroupingFunction() {
        return this.groupingFunction;
    }

    /**
    * Gets an arena for the given filename, potentially aggregating files from the same segment into
    * a single ref counted shared arena. A ref counted shared arena, if created will be added to the
    * given arenas map.
    */
    private Arena getSharedArena(String name, ConcurrentHashMap<String, RefCountedSharedArena> arenas) {
        final var group = groupingFunction.apply(name);

        if (group.isEmpty()) {
            return Arena.ofShared();
        }

        String key = group.get();
        var refCountedArena = arenas.computeIfAbsent(key, s -> new RefCountedSharedArena(s, () -> arenas.remove(s), SHARED_ARENA_PERMITS));
        if (refCountedArena.acquire()) {
            return refCountedArena;
        } else {
            return arenas.compute(key, (s, v) -> {
                if (v != null && v.acquire()) {
                    return v;
                } else {
                    v = new RefCountedSharedArena(s, () -> arenas.remove(s), SHARED_ARENA_PERMITS);
                    v.acquire(); // guaranteed to succeed
                    return v;
                }
            });
        }
    }

    private static int getSharedArenaMaxPermitsSysprop() {
        int ret = 1024; // default value
        try {
            String str = System.getProperty(SHARED_ARENA_MAX_PERMITS_SYSPROP);
            if (str != null) {
                ret = Integer.parseInt(str);
            }
        } catch (@SuppressWarnings("unused") NumberFormatException | SecurityException ignored) {
            LOGGER.warn("Cannot read sysprop " + SHARED_ARENA_MAX_PERMITS_SYSPROP + ", so the default value will be used.");
        }
        return ret;
    }

    private static int checkMaxPermits(int maxPermits) {
        if (RefCountedSharedArena.validMaxPermits(maxPermits)) {
            return maxPermits;
        }
        LOGGER
            .warn(
                "Invalid value for sysprop "
                    + MMapDirectory.SHARED_ARENA_MAX_PERMITS_SYSPROP
                    + ", must be positive and <= 0x07FF. The default value will be used."
            );
        return RefCountedSharedArena.DEFAULT_MAX_PERMITS;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        ensureCanRead(name);

        Path file = getDirectory().resolve(name);

        boolean confined = context == IOContext.READONCE;
        Arena arena = confined ? Arena.ofConfined() : Arena.ofShared();
        // TODO: evaluate the effect if this change on number of segments to be opened.
        // final Arena arena = confined ? Arena.ofConfined() : getSharedArena(name, arenas);

        boolean success = false;

        int chunkSizePower = 34;

        try (var fc = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            final long fileSize = fc.size();
            long lenghtWithoutEncMetadataTrailer = fileSize - EncryptionMetadataTrailer.ENCRYPTION_METADATA_TRAILER_SIZE;

            MemorySegment[] segments = mmap(fc, lenghtWithoutEncMetadataTrailer, arena, chunkSizePower, name, context);

            final IndexInput in = LazyDecryptedMemorySegmentIndexInput
                .newInstance(
                    "CryptoMemorySegmentIndexInput(path=\"" + file + "\")",
                    arena,
                    segments,
                    lenghtWithoutEncMetadataTrailer,
                    chunkSizePower,
                    keyResolver.getFileEncryptionKey(file, name).getEncoded()
                );
            success = true;
            return in;
        } catch (Throwable ex) {
            throw new IOException("Failed to mmap " + file, ex);
        } finally {
            if (success == false) {
                arena.close();
            }
        }
    }

    private MemorySegment[] mmap(FileChannel fc, long size, Arena arena, int chunkSizePower, String name, IOContext context)
        throws Throwable {
        final long chunkSize = 1L << chunkSizePower;
        final int numSegments = (int) ((size + chunkSize - 1) >>> chunkSizePower);

        int madviseFlags = LuceneIOContextMAdvise.getMAdviseFlags(context, name);

        final MemorySegment[] segments = new MemorySegment[numSegments];

        long offset = 0;
        for (int i = 0; i < numSegments; i++) {
            long remaining = size - offset;
            long segmentSize = Math.min(chunkSize, remaining);

            MemorySegment mmapSegment = fc.map(MapMode.PRIVATE, offset, segmentSize, arena);

            if (mmapSegment.address() == 0 || mmapSegment.address() == -1) {
                throw new IOException("mmap failed at offset: " + offset);
            }

            try {
                if (mmapSegment.address() % PanamaNativeAccess.getPageSize() == 0) {
                    PanamaNativeAccess.madvise(mmapSegment.address(), segmentSize, madviseFlags);
                }
            } catch (Throwable t) {
                LOGGER.warn("madvise {} failed for file {} with IOcontext {}", madviseFlags, name, context, t);
            }

            segments[i] = mmapSegment;
            offset += segmentSize;
        }

        return segments;
    }
}

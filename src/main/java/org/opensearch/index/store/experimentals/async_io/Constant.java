/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.experimentals.async_io;

/**
 * Constants for POSIX file operations and Linux io_uring operations.
 * 
 * <p>This class defines system-level constants used for asynchronous I/O operations,
 * including standard POSIX file open flags and io_uring operation codes. These constants
 * are typically used when interfacing with native file systems and async I/O subsystems.
 *
 * @opensearch.internal
 */
public class Constant {

    // Prevent instantiation
    private Constant() {
        throw new AssertionError("Utility class - do not instantiate");
    }

    /** Open file for reading only. */
    public final static int O_RDONLY = 0;

    /** Open file for writing only. */
    public final static int O_WRONLY = 1;

    /** Open file for reading and writing. */
    public final static int O_RDWR = 2;

    /** Create file if it does not exist. */
    public final static int O_CREAT = 64;

    /** Fail if file already exists (used with O_CREAT). */
    public final static int O_EXCL = 128;

    /** Truncate file to zero length if it already exists. */
    public final static int O_TRUNC = 512;

    /** Open file in append mode - writes go to end of file. */
    public final static int O_APPEND = 1024;

    /** Open file for direct I/O, bypassing page cache. */
    public final static int O_DIRECT = 16384;

    /** Synchronize data (but not metadata) on each write. */
    public final static int O_DSYNC = 4096;

    /** Synchronize data and metadata on each write. */
    public final static int O_SYNC = 1052672;

    /** Flag for io_uring fsync operation to sync only data, not metadata. */
    public final static int IORING_FSYNC_DATASYNC = 1;

    /** io_uring operation code for vectored read operation. */
    public final static byte IORING_OP_READV = 1;

    /** io_uring operation code for vectored write operation. */
    public final static byte IORING_OP_WRITEV = 2;

    /** io_uring operation code for file synchronization. */
    public final static byte IORING_OP_FSYNC = 3;

    /** io_uring operation code for asynchronous operation cancellation. */
    public final static byte IORING_OP_ASYNC_CANCEL = 14;

    /** io_uring operation code for opening a file relative to a directory. */
    public final static byte IORING_OP_OPENAT = 18;

    /** io_uring operation code for closing a file descriptor. */
    public final static byte IORING_OP_CLOSE = 19;

    /** io_uring operation code for reading from a file. */
    public final static byte IORING_OP_READ = 22;

    /** io_uring operation code for writing to a file. */
    public final static byte IORING_OP_WRITE = 23;

    /** io_uring operation code for truncating a file to specified length. */
    public static final byte IORING_OP_FTRUNCATE = 46;
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.async_io;

public class Constant {

    public final static int O_RDONLY = 0;
    public final static int O_WRONLY = 1;
    public final static int O_RDWR = 2;
    public final static int O_CREAT = 64;
    public final static int O_EXCL = 128;
    public final static int O_TRUNC = 512;
    public final static int O_APPEND = 1024;
    public final static int O_DIRECT = 16384;
    public final static int O_DSYNC = 4096;
    public final static int O_SYNC = 1052672;

    public final static int IORING_FSYNC_DATASYNC = 1;

    public final static byte IORING_OP_READV = 1;
    public final static byte IORING_OP_WRITEV = 2;
    public final static byte IORING_OP_FSYNC = 3;
    public final static byte IORING_OP_ASYNC_CANCEL = 14;
    public final static byte IORING_OP_OPENAT = 18;
    public final static byte IORING_OP_CLOSE = 19;
    public final static byte IORING_OP_READ = 22;
    public final static byte IORING_OP_WRITE = 23;
    public static final byte IORING_OP_FTRUNCATE = 46;
}

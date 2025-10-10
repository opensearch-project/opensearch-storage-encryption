/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.async_io;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.opensearch.common.SuppressForbidden;

import io.netty.channel.IoEvent;
import io.netty.channel.IoEventLoop;
import io.netty.channel.IoRegistration;
import io.netty.channel.unix.Errors;
import io.netty.channel.uring.IoUringIoEvent;
import io.netty.channel.uring.IoUringIoHandle;
import io.netty.channel.uring.IoUringIoOps;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * Asynchronous file I/O implementation using Linux io_uring interface for high-performance operations.
 * 
 * <p>This class provides non-blocking file operations including read, write, fsync, fdatasync, and truncate
 * operations through the io_uring system interface. It integrates with Netty's IoEventLoop for event-driven
 * async I/O processing and supports Direct I/O through ExtendedOpenOption.DIRECT when available.
 * 
 * <p>Key features:
 * <ul>
 * <li>Asynchronous file operations returning CompletableFuture results</li>
 * <li>Integration with Netty's io_uring implementation for efficient event handling</li>
 * <li>Support for Direct I/O bypass of kernel page cache when supported</li>
 * <li>Proper resource cleanup and cancellation of pending operations</li>
 * </ul>
 * 
 * <p>All async operations are submitted to the io_uring interface and completed asynchronously
 * through the provided IoEventLoop. The class manages operation lifecycle including cancellation
 * and proper cleanup when the file is closed.
 * 
 * @opensearch.internal
 */
@SuppressForbidden(reason = "doesn't uses nio")
@SuppressWarnings("preview")
public class IoUringFile implements AutoCloseable {

    private static final OpenOption ExtendedOpenOption_DIRECT;

    static {
        OpenOption option;
        try {
            final Class<? extends OpenOption> clazz = Class.forName("com.sun.nio.file.ExtendedOpenOption").asSubclass(OpenOption.class);
            option = Arrays.stream(clazz.getEnumConstants()).filter(e -> e.toString().equalsIgnoreCase("DIRECT")).findFirst().orElse(null);
        } catch (ClassNotFoundException e) {
            option = null;
        }
        ExtendedOpenOption_DIRECT = option;
    }

    /**
     * Returns the ExtendedOpenOption.DIRECT option for bypassing kernel page cache.
     * 
     * @return the DIRECT open option for enabling Direct I/O when supported
     * @throws UnsupportedOperationException if ExtendedOpenOption.DIRECT is not available in the current JDK
     */
    public static OpenOption getDirectOpenOption() {
        if (ExtendedOpenOption_DIRECT == null) {
            throw new UnsupportedOperationException(
                "com.sun.nio.file.ExtendedOpenOption.DIRECT is not available in the current JDK version."
            );
        }
        return ExtendedOpenOption_DIRECT;
    }

    private final int fd;

    private final IoUringFileIoHandle ioUringIoHandle;

    private final IoEventLoop ioEventLoop;

    private final IoRegistration ioRegistration;

    private IoUringFile(int fd, IoUringFileIoHandle ioUringIoHandle, IoEventLoop ioEventLoop, IoRegistration ioRegistration) {
        this.fd = fd;
        this.ioUringIoHandle = ioUringIoHandle;
        this.ioEventLoop = ioEventLoop;
        this.ioRegistration = ioRegistration;
        ioUringIoHandle.ioUringFile = this;
    }

    /**
     * Opens a file asynchronously using io_uring with the specified options.
     * 
     * @param file the file to open (must exist and not be a directory)
     * @param ioEventLoop the IoEventLoop for handling async operations
     * @param options the open options (StandardOpenOption and ExtendedOpenOption.DIRECT supported)
     * @return CompletableFuture that completes with IoUringFile when file is opened successfully
     * @throws IllegalArgumentException if file is directory, doesn't exist, or ioEventLoop incompatible
     */
    public static CompletableFuture<IoUringFile> open(File file, IoEventLoop ioEventLoop, OpenOption... options) {
        return open(file, ioEventLoop, calFlag(options));
    }

    /**
     * Opens a file asynchronously using io_uring with the specified open flags.
     * 
     * @param file the file to open (must exist and not be a directory)
     * @param ioEventLoop the IoEventLoop for handling async operations  
     * @param openFlag the open flags (combined O_RDONLY, O_WRONLY, O_RDWR, etc.)
     * @return CompletableFuture that completes with IoUringFile when file is opened successfully
     * @throws IllegalArgumentException if file is directory, doesn't exist, or ioEventLoop incompatible
     */
    public static CompletableFuture<IoUringFile> open(File file, IoEventLoop ioEventLoop, int openFlag) {

        if (file.isDirectory()) {
            throw new IllegalArgumentException("file is directory");
        }

        if (!file.exists()) {
            throw new IllegalArgumentException("file is not exists");
        }

        if (!ioEventLoop.isCompatible(IoUringIoHandle.class)) {
            throw new IllegalArgumentException("ioEventLoop is not compatible with IoUringIoHandle");
        }

        String absolutePath = file.getAbsolutePath();
        CompletableFuture<IoUringFile> initFuture = new CompletableFuture<>();
        IoUringFileIoHandle ioUringFileIoHandle = new IoUringFileIoHandle(ioEventLoop);

        ioEventLoop.register(ioUringFileIoHandle).addListener(new GenericFutureListener<Future<? super IoRegistration>>() {
            @Override
            public void operationComplete(Future<? super IoRegistration> future) throws Exception {
                if (!future.isSuccess()) {
                    initFuture.completeExceptionally(future.cause());
                    return;
                }

                IoRegistration ioUringIoRegistration = (IoRegistration) future.getNow();
                ioUringFileIoHandle.registration = ioUringIoRegistration;
                ioUringFileIoHandle.openAsync(absolutePath, openFlag, 0).whenComplete((syscallResult, t) -> {
                    if (t != null) {
                        initFuture.completeExceptionally(t);
                        return;
                    }
                    if (syscallResult < 0) {
                        initFuture.completeExceptionally(Errors.newIOException("IoUringFile::open", syscallResult));
                        return;
                    }
                    initFuture.complete(new IoUringFile(syscallResult, ioUringFileIoHandle, ioEventLoop, ioUringIoRegistration));
                });
            }
        });
        return initFuture;
    }

    private static int calFlag(OpenOption... options) {
        int oflags = 0;
        boolean read = false;
        boolean write = false;
        boolean append = false;
        boolean truncateExisting = false;
        boolean create = false;
        boolean createNew = false;
        boolean sync = false;
        boolean dsync = false;
        boolean direct = false;
        for (OpenOption option : options) {
            if (option instanceof StandardOpenOption) {
                switch ((StandardOpenOption) option) {
                    case READ:
                        read = true;
                        break;
                    case WRITE:
                        write = true;
                        break;
                    case APPEND:
                        append = true;
                        break;
                    case TRUNCATE_EXISTING:
                        truncateExisting = true;
                        break;
                    case CREATE:
                        create = true;
                        break;
                    case CREATE_NEW:
                        createNew = true;
                        break;
                    case SYNC:
                        sync = true;
                        break;
                    case DSYNC:
                        dsync = true;
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
                continue;
            }
            if (option == ExtendedOpenOption_DIRECT) {
                direct = true;
                continue;
            }
            if (option == null)
                throw new NullPointerException();
            throw new UnsupportedOperationException(option + " not supported");
        }

        if (read && write) {
            oflags = Constant.O_RDWR;
        } else {
            oflags = (write) ? Constant.O_WRONLY : Constant.O_RDONLY;
        }

        if (write) {
            if (truncateExisting)
                oflags |= Constant.O_TRUNC;
            if (append)
                oflags |= Constant.O_APPEND;

            // create flags
            if (createNew) {
                oflags |= (Constant.O_CREAT | Constant.O_EXCL);
            } else {
                if (create)
                    oflags |= Constant.O_CREAT;
            }
        }

        if (dsync)
            oflags |= Constant.O_DSYNC;
        if (sync)
            oflags |= Constant.O_SYNC;
        if (direct)
            oflags |= Constant.O_DIRECT;

        return oflags;
    }

    /**
     * Writes data asynchronously to the file at the specified offset using io_uring.
     * 
     * @param memoryAddress the native memory address containing data to write
     * @param length the number of bytes to write (returns completed future with 0 if length is 0)
     * @param offset the file offset to write data to
     * @return CompletableFuture that completes with number of bytes written
     * @throws IllegalStateException if file is closed or ioRegistration is invalid
     */
    public CompletableFuture<Integer> writeAsync(long memoryAddress, int length, long offset) {
        allowSubmit();

        if (length == 0) {
            return CompletableFuture.completedFuture(0);
        }

        return ioUringIoHandle.writeAsync(memoryAddress, length, offset, fd);
    }

    /**
     * Synchronizes all file data and metadata to storage asynchronously (full fsync).
     * 
     * @return CompletableFuture that completes with syscall result (0 on success)
     * @throws IllegalStateException if file is closed or ioRegistration is invalid
     */
    public CompletableFuture<Integer> fsync() {
        return fsync(0, 0);
    }

    /**
     * Synchronizes file data and metadata to storage asynchronously with range specification.
     * 
     * @param len the length of data to sync (0 for full file)
     * @param offset the file offset to start syncing from (0 for beginning)
     * @return CompletableFuture that completes with syscall result (0 on success)
     * @throws IllegalStateException if file is closed or ioRegistration is invalid
     */
    public CompletableFuture<Integer> fsync(int len, long offset) {
        allowSubmit();
        return Helper.syscallTransform("fsync", ioUringIoHandle.fsyncAsync(fd, false, len, offset));
    }

    /**
     * Synchronizes only file data to storage asynchronously (excludes metadata, faster than fsync).
     * 
     * @return CompletableFuture that completes with syscall result (0 on success)
     * @throws IllegalStateException if file is closed or ioRegistration is invalid
     */
    public CompletableFuture<Integer> fdatasync() {
        return fdatasync(0, 0);
    }

    /**
     * Synchronizes file data to storage asynchronously with range specification (excludes metadata).
     * 
     * @param len the length of data to sync (0 for full file)
     * @param offset the file offset to start syncing from (0 for beginning)
     * @return CompletableFuture that completes with syscall result (0 on success) 
     * @throws IllegalStateException if file is closed or ioRegistration is invalid
     */
    public CompletableFuture<Integer> fdatasync(int len, long offset) {
        allowSubmit();
        return Helper.syscallTransform("fsync", ioUringIoHandle.fsyncAsync(fd, true, len, offset));
    }

    /**
     * Truncates the file to the specified length asynchronously using io_uring.
     * 
     * @param length the new file length (file will be truncated or extended to this size)
     * @return CompletableFuture that completes with syscall result (0 on success)
     * @throws IllegalStateException if file is closed or ioRegistration is invalid
     */
    public CompletableFuture<Integer> truncate(long length) {
        allowSubmit();
        return Helper.syscallTransform("truncate", ioUringIoHandle.truncateAsync(fd, length));
    }

    private void allowSubmit() {
        boolean needThrow = !ioRegistration.isValid() || isClosed();
        if (needThrow) {
            throw new IllegalStateException("ioRegistration is not valid or file is closed");
        }
    }

    @Override
    public void close() throws Exception {
        if (ioUringIoHandle.isClosed.compareAndSet(false, true)) {
            ioRegistration.cancel();
        }
        ioUringIoHandle.cancelAllAsync();
    }

    /**
     * Returns whether this IoUringFile has been closed and is no longer accepting operations.
     * 
     * @return true if the file is closed, false if still open and accepting async operations
     */
    public boolean isClosed() {
        return ioUringIoHandle.isClosed.get();
    }

    private static class AsyncOpContext {
        private final CompletableFuture<Integer> future;
        private final byte opsCode;
        private long uringId;

        private AsyncOpContext(CompletableFuture<Integer> future, byte opsCode) {
            this.future = future;
            this.opsCode = opsCode;
        }
    }

    private static class IoUringFileIoHandle implements IoUringIoHandle {

        private final AtomicBoolean isClosed;
        private final IoEventLoop ioEventLoop;
        private IntObjectHashMap<AsyncOpContext> readFutures;
        private short readId = Short.MIN_VALUE;
        private IntObjectHashMap<AsyncOpContext> writeFutures;
        private short writeId = Short.MIN_VALUE;
        private IntObjectHashMap<AsyncOpContext> otherFutures;
        private short otherId = Short.MIN_VALUE;
        private IoUringFile ioUringFile;
        private IoRegistration registration;
        private AsyncOpContext openContext;

        private IoUringFileIoHandle(IoEventLoop ioEventLoop) {
            this.ioEventLoop = ioEventLoop;
            this.readFutures = new IntObjectHashMap<>();
            this.writeFutures = new IntObjectHashMap<>();
            this.isClosed = new AtomicBoolean(false);
        }

        static IoUringIoOps newAsyncCancel(byte flags, long userData, short data) {
            return new IoUringIoOps(
                Constant.IORING_OP_ASYNC_CANCEL,
                flags,
                (short) 0,
                -1,
                0,
                userData,
                0,
                0,
                data,
                (short) 0,
                (short) 0,
                0,
                0
            );
        }

        private CompletableFuture<Integer> openAsync(String absolutePath, int flags, int mode) {
            // Build a direct ByteBuffer for the path + NUL terminator
            byte[] bytes = absolutePath.getBytes(StandardCharsets.UTF_8);
            ByteBuffer pathBuf = ByteBuffer.allocateDirect(bytes.length + 1);
            pathBuf.put(bytes).put((byte) 0).flip();

            // Get the raw memory address using MemorySegment
            @SuppressWarnings("preview")
            long addr = MemorySegment.ofBuffer(pathBuf).address();

            IoUringIoOps ioOps = new IoUringIoOps(
                Constant.IORING_OP_OPENAT,
                (byte) 0,
                (short) 0,
                -1,
                0L,
                addr,
                mode,
                flags,
                readId,
                (short) 0,
                (short) 0,
                0,
                0L
            );

            CompletableFuture<Integer> openFuture = new CompletableFuture<>();
            openContext = new AsyncOpContext(openFuture, Constant.IORING_OP_OPENAT);

            long uringId = registration.submit(ioOps);
            if (uringId == -1) {
                openFuture.completeExceptionally(new IOException("submit open failed"));
            }

            // Keep a strong ref to pathBuf until completion (GC safety)
            openFuture.whenComplete((res, t) -> {
                // Let GC reclaim pathBuf after open is done
            });

            return openFuture;
        }

        private CompletableFuture<Integer> writeAsync(long memoryAddress, int length, long offset, int fd) {
            CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
            if (ioEventLoop.inEventLoop()) {
                submitWrite(memoryAddress, length, offset, fd, writeFuture);
            } else {
                ioEventLoop.execute(() -> { submitWrite(memoryAddress, length, offset, fd, writeFuture); });
            }
            return writeFuture;
        }

        private void submitWrite(long memoryAddress, int length, long offset, int fd, CompletableFuture<Integer> promise) {
            assert ioEventLoop.inEventLoop();
            IoUringIoOps ioOps = null;
            AsyncOpContext context = new AsyncOpContext(promise, Constant.IORING_OP_WRITE);
            while (true) {
                short writeId = this.writeId;
                this.writeId = (short) (writeId + 1);
                if (writeFutures.containsKey(writeId)) {
                    continue;
                }
                ioOps = new IoUringIoOps(
                    Constant.IORING_OP_WRITE,
                    (byte) 0,
                    (short) 0,
                    fd,
                    offset,
                    memoryAddress,
                    length,
                    0,
                    writeId,
                    (short) 0,
                    (short) 0,
                    0,
                    0L
                );
                writeFutures.put(writeId, context);
                break;
            }
            long uringId = registration.submit(ioOps);
            if (uringId == -1) {
                promise.completeExceptionally(new IOException("submit write failed"));
            } else {
                context.uringId = uringId;
            }
        }

        public CompletableFuture<Integer> fsyncAsync(int fd, boolean isSyncData, int len, long offset) {
            CompletableFuture<Integer> fsyncFuture = new CompletableFuture<>();

            if (ioEventLoop.inEventLoop()) {
                submitFsync(fd, fsyncFuture, isSyncData, len, offset);
            } else {
                ioEventLoop.execute(() -> { submitFsync(fd, fsyncFuture, isSyncData, len, offset); });
            }
            return fsyncFuture;
        }

        public CompletableFuture<Integer> truncateAsync(int fd, long length) {
            CompletableFuture<Integer> truncateFuture = new CompletableFuture<>();

            if (ioEventLoop.inEventLoop()) {
                submitTruncate(fd, length, truncateFuture);
            } else {
                ioEventLoop.execute(() -> { submitTruncate(fd, length, truncateFuture); });
            }

            return truncateFuture;
        }

        public void submitTruncate(int fd, long length, CompletableFuture<Integer> promise) {
            assert ioEventLoop.inEventLoop();

            if (this.otherFutures == null) {
                this.otherFutures = new IntObjectHashMap<>();
            }

            short id = this.otherId++;
            AsyncOpContext context = new AsyncOpContext(promise, Constant.IORING_OP_FTRUNCATE);
            otherFutures.put(id, context);

            IoUringIoOps ioOps = new IoUringIoOps(
                Constant.IORING_OP_FTRUNCATE,
                (byte) 0,
                (short) 0,
                fd,
                length,  // offset used as size
                0L,
                0,
                0,
                id,
                (short) 0,
                (short) 0,
                0,
                0L
            );

            long uringId = registration.submit(ioOps);
            if (uringId == -1) {
                promise.completeExceptionally(new IOException("submitTruncate: submission failed"));
            } else {
                context.uringId = uringId;
            }
        }

        public void submitFsync(int fd, CompletableFuture<Integer> promise, boolean isSyncData, int len, long offset) {
            assert ioEventLoop.inEventLoop();
            IntObjectHashMap<AsyncOpContext> otherFutures = this.otherFutures;
            if (otherFutures == null) {
                otherFutures = this.otherFutures = new IntObjectHashMap<>();
            }
            IoUringIoOps ioOps = null;
            AsyncOpContext context = new AsyncOpContext(promise, Constant.IORING_OP_FSYNC);
            while (true) {
                short otherId = this.otherId;
                this.otherId = (short) (otherId + 1);
                if (otherFutures.containsKey(otherId)) {
                    continue;
                }
                if (isSyncData) {
                    ioOps = new IoUringIoOps(
                        Constant.IORING_OP_FSYNC,
                        (byte) 0,
                        (short) 0,
                        fd,
                        offset,
                        0L,
                        len,
                        Constant.IORING_FSYNC_DATASYNC,
                        otherId,
                        (short) 0,
                        (short) 0,
                        0,
                        0L
                    );
                } else {
                    ioOps = new IoUringIoOps(
                        Constant.IORING_OP_FSYNC,
                        (byte) 0,
                        (short) 0,
                        fd,
                        offset,
                        0L,
                        len,
                        0,
                        otherId,
                        (short) 0,
                        (short) 0,
                        0,
                        0L
                    );
                }
                otherFutures.put(otherId, context);
                break;
            }
            long uringId = registration.submit(ioOps);
            if (uringId == -1) {
                promise.completeExceptionally(new IOException("submit fsync failed"));
            } else {
                context.uringId = uringId;
            }
        }

        private void submitCloseAsync(int fd) {
            assert ioEventLoop.inEventLoop();
            IoUringIoOps closeOps = new IoUringIoOps(
                Constant.IORING_OP_CLOSE,
                (byte) 0,
                (short) 0,
                fd,
                0L,
                0L,
                0,
                0,
                (short) 0,
                (short) 0,
                (short) 0,
                0,
                0L
            );
            registration.submit(closeOps);
        }

        private void cancelAllAsync() {

            if (!ioEventLoop.inEventLoop()) {
                ioEventLoop.execute(this::cancelAllAsync);
                return;
            }

            assert ioEventLoop.inEventLoop();
            short cancelId = 0;
            IoRegistration ioRegistration = this.registration;
            for (AsyncOpContext context : readFutures.values()) {
                IoUringIoOps ops = newAsyncCancel((byte) 0, context.uringId, cancelId);
                ioRegistration.submit(ops);
                cancelId++;
            }
            for (AsyncOpContext context : writeFutures.values()) {
                IoUringIoOps ops = newAsyncCancel((byte) 0, context.uringId, cancelId);
                ioRegistration.submit(ops);
                cancelId++;
            }
            if (otherFutures != null) {
                for (AsyncOpContext context : otherFutures.values()) {
                    IoUringIoOps ops = newAsyncCancel((byte) 0, context.uringId, cancelId);
                    ioRegistration.submit(ops);
                    cancelId++;
                }
            }
        }

        @Override
        public void handle(IoRegistration ioRegistration, IoEvent ioEvent) {
            IoUringIoEvent event = (IoUringIoEvent) ioEvent;
            byte opCode = event.opcode();

            if (opCode == Constant.IORING_OP_OPENAT) {
                openContext.future.complete(event.res());
                openContext = null;
                return;
            }

            if (opCode == Constant.IORING_OP_READ || opCode == Constant.IORING_OP_READV) {
                AsyncOpContext asyncOpContext = readFutures.remove(event.data());
                if (asyncOpContext != null) {
                    asyncOpContext.future.complete(event.res());
                }
                return;
            }

            if (opCode == Constant.IORING_OP_WRITE || opCode == Constant.IORING_OP_WRITEV) {
                AsyncOpContext asyncOpContext = writeFutures.remove(event.data());
                if (asyncOpContext != null) {
                    asyncOpContext.future.complete(event.res());
                }
                return;
            }

            if (opCode == Constant.IORING_OP_CLOSE) {
                return;
            }

            if (otherFutures != null) {
                AsyncOpContext asyncOpContext = otherFutures.remove(event.data());
                if (asyncOpContext != null) {
                    asyncOpContext.future.complete(event.res());
                }
            }

            if (isClosed.get() && readFutures.isEmpty() && writeFutures.isEmpty()) {
                submitCloseAsync(ioUringFile.fd);
                return;
            }
        }

        @Override
        public void close() throws Exception {
            ioUringFile.close();
        }
    }
}

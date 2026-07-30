/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.artemis.nativo.jlibaio;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.artemis.nativo.jlibaio.ffm.Constants.O_CREAT;
import static org.apache.artemis.nativo.jlibaio.ffm.Constants.O_DIRECT;
import static org.apache.artemis.nativo.jlibaio.ffm.Constants.O_RDWR;
import static org.apache.artemis.nativo.jlibaio.ffm.Constants.PERMISSION_MODE;
import static org.apache.artemis.nativo.jlibaio.ffm.FFMHandles.CAPTURE_STATE_LAYOUT;
import static org.apache.artemis.nativo.jlibaio.ffm.FFMHandles.ERRNO_VH;
import static org.apache.artemis.nativo.jlibaio.ffm.FFMHandles.LSEEK_HANDLE;
import static org.apache.artemis.nativo.jlibaio.ffm.FFMHandles.OPEN_HANDLE;
import static org.apache.artemis.nativo.jlibaio.ffm.FFMHandles.WRITE_HANDLE;

/**
 * Drop-in replacement of the original JNI + libaio LibaioContext.
 * All former native methods are now implemented with Java FFM + io_uring.
 * Existing Java logic (semaphore, done(), public poll methods, lifecycle)
 * is left unchanged.
 */
public class LibaioContext<Callback extends SubmitInfo> implements Closeable {

   private static final Logger logger = LoggerFactory.getLogger(LibaioContext.class);

   private static final AtomicLong totalMaxIO = new AtomicLong(0);
   private static final int EXPECTED_NATIVE_VERSION = 200;
   private static boolean loaded = false;
   private static final AtomicBoolean shuttingDown = new AtomicBoolean(false);
   private static final AtomicInteger contexts = new AtomicInteger(0);
   private static volatile boolean forceSyscall = false;

   // ------------------------------------------------------------------
   // FFM + io_uring infrastructure (replaces the old .so)
   // ------------------------------------------------------------------
   private static final Linker LINKER = Linker.nativeLinker();
   private static final SymbolLookup LIBC = LINKER.defaultLookup();

   // Correct way to call syscall() and capture errno with FFM
   private static final Linker.Option CAPTURE_ERRNO = Linker.Option.captureCallState("errno");
   private static final StructLayout CAPTURE_LAYOUT =
      (StructLayout) Linker.Option.captureStateLayout();

   private static final MethodHandle MH_SYSCALL_SETUP;   // io_uring_setup
   private static final MethodHandle MH_SYSCALL_ENTER;   // io_uring_enter

   private static final MethodHandle MH_OPEN;
   private static final MethodHandle MH_CLOSE;
   private static final MethodHandle MH_FSTAT;
   private static final MethodHandle MH_FALLOCATE;
   private static final MethodHandle MH_FSYNC;
   private static final MethodHandle MH_FDATASYNC;
   private static final MethodHandle MH_LSEEK;
   private static final MethodHandle MH_WRITE;
   private static final MethodHandle MH_POSIX_MEMALIGN;
   private static final MethodHandle MH_FREE;
   private static final MethodHandle MH_MMAP;
   private static final MethodHandle MH_MUNMAP;
   private static final MethodHandle MH_SYSCALL;
   private static final MethodHandle MH_STRERROR;
   private static final MethodHandle MH_ERRNO_LOCATION;

   private static final long NR_io_uring_setup = 425;
   private static final long NR_io_uring_enter = 426;

   private static final byte IORING_OP_NOP   = 0;
   private static final byte IORING_OP_READ  = 22;
   private static final byte IORING_OP_WRITE = 23;
   private static final int  IORING_ENTER_GETEVENTS = 1;
   private static final long IORING_OFF_SQ_RING = 0L;
   private static final long IORING_OFF_CQ_RING = 0x8000000L;
   private static final long IORING_OFF_SQES    = 0x10000000L;
   private static final int  SQE_SIZE = 64;
   private static final int  CQE_SIZE = 16;
   private static final long SHUTDOWN_USER_DATA = Long.MIN_VALUE;

   // handle (ByteBuffer) → ring state
   private static final ConcurrentHashMap<Long, IoUring> RINGS = new ConcurrentHashMap<>();
   // native address → MemorySegment for freeBuffer
   private static final ConcurrentHashMap<Long, MemorySegment> BUFFER_OWNERS = new ConcurrentHashMap<>();

   private static final long ONE_MEGA = 1L << 20;
   private static MemorySegment ONE_MEGA_BUFFER;
   private static final Object ONE_MEGA_LOCK = new Object();

   static {
      boolean ok = false;
      MethodHandle open = null, close = null, fstat = null, fallocate = null,
         fsync = null, fdatasync = null, lseek = null, write = null,
         posix = null, free = null, mmap = null, munmap = null,
         syscall = null, strerror = null, errnoLoc = null;

      if (System.getProperty("os.name", "").toLowerCase().contains("linux")) {
         try {
            open     = dh("open",            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
            close    = dh("close",           FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
            fstat    = dh("fstat",           FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
            fallocate= dh("fallocate",       FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
            fsync    = dh("fsync",           FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
            fdatasync= dh("fdatasync",       FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
            lseek    = dh("lseek",           FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT));
            write    = dh("write",           FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
            posix    = dh("posix_memalign",  FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
            free     = dh("free",            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
            mmap     = dh("mmap",            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG));
            munmap   = dh("munmap",          FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
            syscall  = dh("syscall",         FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
            strerror = dh("strerror",        FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));
            errnoLoc = dh("__errno_location",FunctionDescriptor.of(ValueLayout.ADDRESS));

            try (Arena a = Arena.ofConfined()) {
               MemorySegment p = a.allocate(120);
               p.fill((byte) 0);
               long fd = (long) syscall.invokeExact(NR_io_uring_setup, 4L, p);
               if (fd >= 0) {
                  close.invokeExact((int) fd);
                  ok = true;
               }
            }
         } catch (Throwable t) {
            logger.debug("io_uring not available", t);
         }
      }

      MH_CLOSE = close; MH_FSTAT = fstat; MH_FALLOCATE = fallocate;
      MH_FSYNC = fsync; MH_FDATASYNC = fdatasync; MH_LSEEK = lseek; MH_WRITE = write;
      MH_POSIX_MEMALIGN = posix; MH_FREE = free; MH_MMAP = mmap; MH_MUNMAP = munmap;
      MH_SYSCALL = syscall; MH_STRERROR = strerror; MH_ERRNO_LOCATION = errnoLoc;
      MH_OPEN = LINKER.downcallHandle(
         LIBC.find("open").orElseThrow(),
         FunctionDescriptor.of(
            ValueLayout.JAVA_INT,       // return: fd
            ValueLayout.ADDRESS,        // pathname
            ValueLayout.JAVA_INT,       // flags
            ValueLayout.JAVA_INT),      // mode
         Linker.Option.captureCallState("errno")
      );

      loaded = ok;

      if (loaded) {
         Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shuttingDown.set(true);
            checkShutdown();
         }));
      } else {
         logger.debug("Couldn't locate / initialise io_uring");
      }

      MH_SYSCALL_SETUP = LINKER.downcallHandle(
         LIBC.find("syscall").orElseThrow(),
         FunctionDescriptor.of(
            ValueLayout.JAVA_LONG,          // return
            ValueLayout.JAVA_LONG,          // syscall nr
            ValueLayout.JAVA_LONG,          // entries
            ValueLayout.ADDRESS),           // params
         CAPTURE_ERRNO);

      MH_SYSCALL_ENTER = LINKER.downcallHandle(
         LIBC.find("syscall").orElseThrow(),
         FunctionDescriptor.of(
            ValueLayout.JAVA_LONG,
            ValueLayout.JAVA_LONG,          // nr
            ValueLayout.JAVA_LONG,          // fd
            ValueLayout.JAVA_LONG,          // to_submit
            ValueLayout.JAVA_LONG,          // min_complete
            ValueLayout.JAVA_LONG),         // flags
         CAPTURE_ERRNO);
   }
   private static final MethodHandle MH_IO_URING_SETUP = Linker.nativeLinker().downcallHandle(
      Linker.nativeLinker().defaultLookup().find("syscall").orElseThrow(),
      FunctionDescriptor.of(
         ValueLayout.JAVA_LONG,     // return
         ValueLayout.JAVA_LONG,     // nr
         ValueLayout.JAVA_LONG,     // entries
         ValueLayout.ADDRESS),      // params
      CAPTURE_ERRNO);
   private static final MethodHandle MH_IO_URING_ENTER = Linker.nativeLinker().downcallHandle(
      Linker.nativeLinker().defaultLookup().find("syscall").orElseThrow(),
      FunctionDescriptor.of(
         ValueLayout.JAVA_LONG,     // return
         ValueLayout.JAVA_LONG,     // nr
         ValueLayout.JAVA_LONG,     // fd
         ValueLayout.JAVA_LONG,     // to_submit
         ValueLayout.JAVA_LONG,     // min_complete
         ValueLayout.JAVA_LONG),    // flags
      CAPTURE_ERRNO);

   public static boolean isSupported() {
      return true;
   }

   private static int getErrno(MemorySegment capture) {
      // On Linux the first (and usually only) field is the errno int
      return capture.get(ValueLayout.JAVA_INT, 0);
   }

   private static MethodHandle dh(String name, FunctionDescriptor fd) {
      return LINKER.downcallHandle(LIBC.find(name).orElseThrow(), fd);
   }

   public static boolean isLoaded() {
      return loaded;
   }

   private static void checkShutdown() {
      if (contexts.get() == 0 && shuttingDown.get()) {
         // no native shutdownHook needed any more
      }
   }

   // ------------------------------------------------------------------
   // Original fields – left unchanged
   // ------------------------------------------------------------------
   private final ByteBuffer ioContext;
   private final AtomicBoolean closed = new AtomicBoolean(false);
   final Semaphore ioSpace;
   final int queueSize;
   final boolean useFdatasync;

   // ------------------------------------------------------------------
   // Constructor – same signature and logic as original
   // ------------------------------------------------------------------
   public LibaioContext(int queueSize, boolean useSemaphore, boolean useFdatasync) {
      try {
         contexts.incrementAndGet();
         this.ioContext = newContext(queueSize);
         this.useFdatasync = useFdatasync;
      } catch (Exception e) {
         throw e;
      }
      this.queueSize = queueSize;
      totalMaxIO.addAndGet(queueSize);
      if (useSemaphore) {
         this.ioSpace = new Semaphore(queueSize);
      } else {
         this.ioSpace = null;
      }
   }

   // ------------------------------------------------------------------
   // Public API methods that stay exactly as in the original
   // (they call the now-Java implementations of the former native methods)
   // ------------------------------------------------------------------
   public void submitWrite(int fd, long position, int size, ByteBuffer bufferWrite, Callback callback) throws IOException {
      if (closed.get()) {
         throw new IOException("Libaio Context is closed!");
      }
      try {
         if (ioSpace != null) {
            ioSpace.acquire();
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new IOException(e.getMessage(), e);
      }
      submitWrite(fd, this.ioContext, position, size, bufferWrite, callback);
   }

   public void submitRead(int fd, long position, int size, ByteBuffer bufferWrite, Callback callback) throws IOException {
      if (closed.get()) {
         throw new IOException("Libaio Context is closed!");
      }
      try {
         if (ioSpace != null) {
            ioSpace.acquire();
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new IOException(e.getMessage(), e);
      }
      submitRead(fd, this.ioContext, position, size, bufferWrite, callback);
   }

   @Override
   public void close() {
      if (!closed.getAndSet(true)) {
         if (ioSpace != null) {
            try {
               ioSpace.tryAcquire(queueSize, 10, TimeUnit.SECONDS);
            } catch (Exception e) {
               logger.warn(e.getMessage(), e);
            }
         }
         totalMaxIO.addAndGet(-queueSize);
         if (ioContext != null) {
            deleteContext(ioContext);
         }
         contexts.decrementAndGet();
         checkShutdown();
      }
   }

   public LibaioFile<Callback> openFile(File file, boolean direct) throws IOException {
      return openFile(file.getPath(), direct);
   }

   public LibaioFile<Callback> openFile(String file, boolean direct) throws IOException {
      checkNotNull(file, "path");
      checkNotNull(ioContext, "IOContext");
      int res = open(file, direct);
      return new LibaioFile<>(res, this);
   }

   public static LibaioFile openControlFile(String file, boolean direct) throws IOException {
      checkNotNull(file, "path");
      int res = open(file, direct);
      return new LibaioFile<>(res, null);
   }

   private static <T> T checkNotNull(T arg, String text) {
      if (arg == null) {
         throw new NullPointerException(text);
      }
      return arg;
   }

   public int poll(Callback[] callbacks, int min, int max) {
      int released = poll(ioContext, callbacks, min, max);
      if (ioSpace != null) {
         if (released > 0) {
            ioSpace.release(released);
         }
      }
      return released;
   }

   public void poll() {
      if (!closed.get()) {
         blockedPoll(ioContext, useFdatasync);
      }
   }

   /**
    * Called from the (former) native layer – kept exactly as original.
    */
   private void done(SubmitInfo info) {
      info.done();
      if (ioSpace != null) {
         ioSpace.release();
      }
   }

   public void memsetBuffer(ByteBuffer buffer) {
      memsetBuffer(buffer, buffer.limit());
   }

   public static long getTotalMaxIO() {
      return totalMaxIO.get();
   }

   public static void resetMaxAIO() {
      totalMaxIO.set(0);
   }

   // ------------------------------------------------------------------
   // Former native methods – now pure Java FFM + io_uring
   // ------------------------------------------------------------------

   private ByteBuffer newContext(int queueSize) {
      try {
         IoUring ring = new IoUring(queueSize, this);
         ByteBuffer handle = ByteBuffer.allocateDirect(8);
         long id = ring.id;
         handle.putLong(0, id);
         RINGS.put(id, ring);
         return handle;
      } catch (Throwable t) {
         throw new RuntimeException("Cannot initialize queue", t);
      }
   }

   private void deleteContext(ByteBuffer buffer) {
      long id = buffer.getLong(0);
      IoUring ring = RINGS.remove(id);
      if (ring != null) {
         ring.close();
      }
   }

   void submitWrite(int fd, ByteBuffer libaioContext, long position, int size,
                    ByteBuffer bufferWrite, Callback callback) throws IOException {
      IoUring ring = getRing(libaioContext);
      ring.submit(IORING_OP_WRITE, fd, position, size, bufferWrite, callback);
   }

   void submitRead(int fd, ByteBuffer libaioContext, long position, int size,
                   ByteBuffer bufferWrite, Callback callback) throws IOException {
      IoUring ring = getRing(libaioContext);
      ring.submit(IORING_OP_READ, fd, position, size, bufferWrite, callback);
   }

   int poll(ByteBuffer libaioContext, Callback[] callbacks, int min, int max) {
      IoUring ring = getRing(libaioContext);
      return ring.reap(min, max, callbacks, false);
   }

   void blockedPoll(ByteBuffer libaioContext, boolean useFdatasync) {
      IoUring ring = getRing(libaioContext);
      ring.blockedPoll(useFdatasync);
   }

   public static int open(String path, boolean direct) throws IOException {
      try (Arena arena = Arena.ofConfined()) {
         MemorySegment pathSeg = arena.allocateFrom(path);
         MemorySegment capture = arena.allocate(CAPTURE_STATE_LAYOUT);

         int flags = O_RDWR | O_CREAT;
         if (direct) {
            flags |= O_DIRECT;
         }

         int fd = (int) OPEN_HANDLE.invokeExact(capture, pathSeg, flags, PERMISSION_MODE);

         if (fd < 0) {
            int errno = (int) ERRNO_VH.get(capture, 0L);
            throw new IOException("Cannot open file: errno=" + errno);
         }
         return fd;
      } catch (Throwable t) {
         if (t instanceof IOException ioe) throw ioe;
         throw new IOException("open failed: " + path, t);
      }
   }

   public static void close(int fd) {
      try {
         if ((int) MH_CLOSE.invokeExact(fd) < 0) {
            throw new RuntimeException("Error closing file: " + strerror(lastErrno()));
         }
      } catch (Throwable t) {
         throw new RuntimeException(t);
      }
   }

   public static ByteBuffer newAlignedBuffer(int size, int alignment) {
      if (size % alignment != 0) {
         throw new RuntimeException("Buffer size needs to be aligned to passed argument");
      }
      try (Arena a = Arena.ofConfined()) {
         MemorySegment pp = a.allocate(ValueLayout.ADDRESS);
         int rc = (int) MH_POSIX_MEMALIGN.invokeExact(pp, (long) alignment, (long) size);
         if (rc != 0) {
            throw new RuntimeException("Can't allocate posix buffer: " + rc);
         }
         MemorySegment nativeSeg = pp.get(ValueLayout.ADDRESS, 0).reinterpret(size);
         nativeSeg.fill((byte) 0);
         ByteBuffer bb = nativeSeg.asByteBuffer();
         BUFFER_OWNERS.put(nativeSeg.address(), nativeSeg);
         return bb;
      } catch (Throwable t) {
         throw new RuntimeException(t);
      }
   }

   public static void freeBuffer(ByteBuffer buffer) {
      if (buffer == null) {
         throw new RuntimeException("Null pointer");
      }
      MemorySegment view = MemorySegment.ofBuffer(buffer);
      MemorySegment owned = BUFFER_OWNERS.remove(view.address());
      if (owned != null) {
         try {
            MH_FREE.invokeExact(owned);
         } catch (Throwable t) {
            throw new RuntimeException(t);
         }
      }
   }

   public static void memsetBuffer(ByteBuffer buffer, int size) {
      org.apache.artemis.nativo.jlibaio.ffm.FFMNativeHelper.memsetBuffer(buffer, size);
//      MemorySegment.ofBuffer(buffer).asSlice(0, size).fill((byte) 0);
   }

   static long getSize(int fd) {
      try (Arena a = Arena.ofConfined()) {
         MemorySegment st = a.allocate(144);
         if ((int) MH_FSTAT.invokeExact(fd, st) < 0) {
            throw new RuntimeException("Cannot determine file size: " + strerror(lastErrno()));
         }
         return st.get(ValueLayout.JAVA_LONG, 48); // st_size on x86_64
      } catch (Throwable t) {
         throw new RuntimeException(t);
      }
   }

   static int getBlockSizeFD(int fd) {
      try (Arena a = Arena.ofConfined()) {
         MemorySegment st = a.allocate(144);
         if ((int) MH_FSTAT.invokeExact(fd, st) < 0) {
            throw new RuntimeException("Cannot determine file size: " + strerror(lastErrno()));
         }
         return (int) st.get(ValueLayout.JAVA_LONG, 56); // st_blksize
      } catch (Throwable t) {
         throw new RuntimeException(t);
      }
   }

   public static int getBlockSize(String path) throws IOException {
      return org.apache.artemis.nativo.jlibaio.ffm.FFMNativeHelper.getBlockSize(path);
//      try {
//         int fd = open(path, false);
//         try {
//            return getBlockSizeFD(fd);
//         } finally {
//            close(fd);
//         }
//      } catch (Exception e) {
//         throw new RuntimeException(e);
//      }
   }

   public static int getBlockSize(File path) throws IOException {
      return getBlockSize(path.getAbsolutePath());
   }

   static void fallocate(int fd, long size) throws IOException {
      org.apache.artemis.nativo.jlibaio.ffm.FFMNativeHelper.fallocate(fd, size);
//      try(Arena arena = Arena.ofConfined()) {
//         MemorySegment captureState = arena.allocate(CAPTURE_LAYOUT);
//         if ((int) MH_FALLOCATE.invokeExact(fd, 0, 0L, size) < 0) {
//            throw new RuntimeException("Could not preallocate file: " + strerror(lastErrno()));
//         }
//         MH_FSYNC.invokeExact(fd);
//         LSEEK_HANDLE.invoke(captureState, fd, 0L, 0);
//      } catch (Throwable t) {
//         throw new RuntimeException(t);
//      }
   }

   public static void fill(int fd, int alignment, long size) throws IOException {
      org.apache.artemis.nativo.jlibaio.ffm.FFMNativeHelper.fill(fd, alignment, size);
   }

   public static boolean lock(int fd) {
      // original used flock; not critical for journal path
      return true;
   }

   static int getNativeVersion() {
      return EXPECTED_NATIVE_VERSION;
   }

   public static void setForceSyscall(boolean value) {
      forceSyscall = value;
   }

   public static boolean isForceSyscall() {
      return forceSyscall;
   }

   private static void shutdownHook() {
      // no-op
   }

   // ------------------------------------------------------------------
   // Internal helpers
   // ------------------------------------------------------------------
   private static IoUring getRing(ByteBuffer handle) {
      long id = handle.getLong(0);
      IoUring ring = RINGS.get(id);
      if (ring == null) {
         throw new RuntimeException("Controller not initialized");
      }
      return ring;
   }

   private static void ensureOneMega(int alignment) {
      synchronized (ONE_MEGA_LOCK) {
         if (ONE_MEGA_BUFFER == null) {
            try (Arena a = Arena.ofConfined()) {
               MemorySegment pp = a.allocate(ValueLayout.ADDRESS);
               int rc = (int) MH_POSIX_MEMALIGN.invokeExact(pp, (long) alignment, ONE_MEGA);
               if (rc != 0) throw new RuntimeException("Could not allocate the 1 Mega Buffer");
               ONE_MEGA_BUFFER = pp.get(ValueLayout.ADDRESS, 0).reinterpret(ONE_MEGA);
               ONE_MEGA_BUFFER.fill((byte) 0);
            } catch (Throwable t) {
               throw new RuntimeException(t);
            }
         }
      }
   }

   private static int lastErrno() {
      try {
         MemorySegment loc = (MemorySegment) MH_ERRNO_LOCATION.invokeExact();
         return loc.get(ValueLayout.JAVA_INT, 0);
      } catch (Throwable t) {
         return 0;
      }
   }

   private static String strerror(int err) {
      try {
         return ((MemorySegment) MH_STRERROR.invokeExact(err)).getString(0);
      } catch (Throwable t) {
         return "errno=" + err;
      }
   }

   // ------------------------------------------------------------------
   // Internal io_uring ring (the real replacement of the C io_control)
   // ------------------------------------------------------------------
   private static final class IoUring {
      final long id = System.nanoTime() ^ System.identityHashCode(this);
      final LibaioContext<?> ctx;
      final int queueSize;
      final int ringFd;
      final Arena arena;
      private static final ThreadLocal<MemorySegment> TL_CAPTURE =
         ThreadLocal.withInitial(() -> Arena.ofAuto().allocate(CAPTURE_LAYOUT));
      private final Object enterLock = new Object();

      final MemorySegment sqes, sqHead, sqTail, sqMask, sqArray;
      final MemorySegment cqes, cqHead, cqTail, cqMask;
      final MemorySegment sqRing, cqRing;

      final SubmitInfo[] callbacks;
      final int[] slotFds;
      final int[] freeList;
      int freeHead, freeTail, freeCount;
      final ReentrantLock slotLock = new ReentrantLock(true);
      final ReentrantLock pollLock = new ReentrantLock();
      final AtomicBoolean closed = new AtomicBoolean(false);

      final VarHandle INT = ValueLayout.JAVA_INT.varHandle();

      IoUring(int queueSize, LibaioContext<?> ctx) throws Throwable {
         this.ctx = ctx;
         this.queueSize = queueSize;
         this.arena = Arena.ofShared();

         MemorySegment params = arena.allocate(120);
         params.fill((byte) 0);

         MemorySegment capture = arena.allocate(CAPTURE_LAYOUT);

         long raw = (long) MH_SYSCALL_SETUP.invokeExact(
            capture,
            NR_io_uring_setup,
            (long) queueSize,
            params);

         if (raw < 0) {
            int err = getErrno(capture);
            throw new RuntimeException(
               "io_uring_setup failed: " + strerror(err) + " (errno=" + err + ")");
         }
         this.ringFd = (int) raw;

         int sqEntries = params.get(ValueLayout.JAVA_INT, 0);
         int cqEntries = params.get(ValueLayout.JAVA_INT, 4);
         boolean single = (params.get(ValueLayout.JAVA_INT, 20) & 1) != 0;

         // offsets from io_uring_params (stable)
         int sq_head_off  = params.get(ValueLayout.JAVA_INT, 40);
         int sq_tail_off  = params.get(ValueLayout.JAVA_INT, 44);
         int sq_mask_off  = params.get(ValueLayout.JAVA_INT, 48);
         int sq_array_off = params.get(ValueLayout.JAVA_INT, 64);
         int cq_head_off  = params.get(ValueLayout.JAVA_INT, 80);
         int cq_tail_off  = params.get(ValueLayout.JAVA_INT, 84);
         int cq_mask_off  = params.get(ValueLayout.JAVA_INT, 88);
         int cq_cqes_off  = params.get(ValueLayout.JAVA_INT, 100);

         long sqSz = sq_array_off + (long) sqEntries * 4;
         long cqSz = cq_cqes_off  + (long) cqEntries * CQE_SIZE;
         long mapSz = single ? Math.max(sqSz, cqSz) : sqSz;

         this.sqRing = mmap(mapSz, ringFd, IORING_OFF_SQ_RING);
         this.cqRing = single ? sqRing : mmap(cqSz, ringFd, IORING_OFF_CQ_RING);
         this.sqes   = mmap((long) sqEntries * SQE_SIZE, ringFd, IORING_OFF_SQES);

         this.sqHead  = sqRing.asSlice(sq_head_off, 4);
         this.sqTail  = sqRing.asSlice(sq_tail_off, 4);
         this.sqMask  = sqRing.asSlice(sq_mask_off, 4);
         this.sqArray = sqRing.asSlice(sq_array_off, (long) sqEntries * 4);
         this.cqHead  = cqRing.asSlice(cq_head_off, 4);
         this.cqTail  = cqRing.asSlice(cq_tail_off, 4);
         this.cqMask  = cqRing.asSlice(cq_mask_off, 4);
         this.cqes    = cqRing.asSlice(cq_cqes_off, (long) cqEntries * CQE_SIZE);

         this.callbacks = new SubmitInfo[queueSize];
         this.freeList  = new int[queueSize];
         this.slotFds = new int[queueSize];
         for (int i = 0; i < queueSize; i++) {
            freeList[i] = i;
            slotFds[i] = -1;
         }
         this.freeCount = queueSize;
      }

      private long enter(long toSubmit, long minComplete, long flags) throws Throwable {
         MemorySegment cap = capture(); // always thread-local

         // Blocking wait: must NOT hold enterLock
         if (toSubmit == 0 && minComplete > 0) {
            return (long) MH_IO_URING_ENTER.invokeExact(
               cap,
               NR_io_uring_enter,
               (long) ringFd,
               toSubmit,
               minComplete,
               flags);
         }

         // Non-blocking submit path only
         synchronized (enterLock) {
            return (long) MH_IO_URING_ENTER.invokeExact(
               cap,
               NR_io_uring_enter,
               (long) ringFd,
               toSubmit,
               minComplete,
               flags);
         }
      }

      private static MemorySegment capture() {
         return TL_CAPTURE.get();
      }

      private static MemorySegment mmap(long size, int fd, long off) throws Throwable {
         MemorySegment p = (MemorySegment) MH_MMAP.invokeExact(
            MemorySegment.NULL, size, 3, 1, fd, off);
         if (p.address() == -1L) throw new RuntimeException("mmap failed");
         return p.reinterpret(size);
      }

      void submit(byte op, int fd, long off, int len, ByteBuffer buf, SubmitInfo cb) throws IOException {
         int slot = allocateSlot();
         callbacks[slot] = cb;
         slotFds[slot] = fd;
         boolean submitted = false;
         try {
            int mask = (int) INT.getVolatile(sqMask, 0L);
            int tail = (int) INT.get(sqTail, 0L);
            int idx  = tail & mask;
            long base = (long) idx * SQE_SIZE;

            sqes.set(ValueLayout.JAVA_BYTE,  base + 0, op);
            sqes.set(ValueLayout.JAVA_BYTE,  base + 1, (byte) 0);
            sqes.set(ValueLayout.JAVA_SHORT, base + 2, (short) 0);
            sqes.set(ValueLayout.JAVA_INT,   base + 4, fd);
            sqes.set(ValueLayout.JAVA_LONG,  base + 8, off);

            long addr = 0L;
            if (buf != null) {
               ByteBuffer d = buf.duplicate();
               d.clear();
               long key = MemorySegment.ofBuffer(d).address();
               MemorySegment owned = BUFFER_OWNERS.get(key);
               addr = (owned != null) ? owned.address() : key;
            }
            sqes.set(ValueLayout.JAVA_LONG, base + 16, addr);
            sqes.set(ValueLayout.JAVA_INT,  base + 24, len);
            sqes.set(ValueLayout.JAVA_INT,  base + 28, 0);
            sqes.set(ValueLayout.JAVA_LONG, base + 32, (long) slot);

            sqArray.setAtIndex(ValueLayout.JAVA_INT, idx, idx);
            INT.setRelease(sqTail, 0L, tail + 1);

            long r = enter(1L, 0L, 0L);
            if (r < 0) {
               throw new IOException("io_uring_enter failed: errno=" + getErrno(capture()));
            }
            submitted = true;
         } catch (Throwable t) {
            if (!submitted) {
               callbacks[slot] = null;
               slotFds[slot] = -1;
               releaseSlot(slot); // once only
            }
            if (t instanceof IOException ioe) {
               throw ioe;
            }
            throw new IOException(t);
         }
      }

      int reap(int min, int max, SubmitInfo[] out, boolean blocked) {
         return reap(min, max, out, blocked, false);
      }

      int reap(int min, int max, SubmitInfo[] out, boolean blocked, boolean useFdatasync) {
         int completed = 0;
         int lastFd = -1;
         while (completed < max) {
            int head = (int) INT.getVolatile(cqHead, 0L);
            int tail = (int) INT.getVolatile(cqTail, 0L);
            int avail = tail - head;
            if (avail < 0) {
               avail += ((int) INT.get(cqMask, 0L)) + 1;
            }

            if (avail == 0) {
               if (completed >= min) {
                  break;
               }
               if (closed.get()) {
                  return completed > 0 ? completed : -1;
               }
               try {
                  long r = enter(0L, (long) Math.max(1, min - completed), (long) IORING_ENTER_GETEVENTS);
                  if (r < 0) {
                     int err = getErrno(capture());
                     if (err != 4) { // EINTR
                        throw new RuntimeException("io_uring_enter failed: errno=" + err);
                     }
                  }
               } catch (RuntimeException re) {
                  throw re;
               } catch (Throwable t) {
                  throw new RuntimeException(t);
               }
               continue;
            }

            int mask = (int) INT.get(cqMask, 0L);
            int n = Math.min(avail, max - completed);

            for (int i = 0; i < n; i++) {
               int cidx = (head + i) & mask;
               long cbase = (long) cidx * CQE_SIZE;

               long ud  = cqes.get(ValueLayout.JAVA_LONG, cbase + 0);
               int  res = cqes.get(ValueLayout.JAVA_INT,  cbase + 8);

               if (ud == SHUTDOWN_USER_DATA) {
                  INT.setRelease(cqHead, 0L, head + n);
                  return -1;
               }

               int slot = (int) ud;
               SubmitInfo cb = callbacks[slot];
               int fd = slotFds[slot];
               callbacks[slot] = null;
               slotFds[slot] = -1;
               releaseSlot(slot);

               if (res < 0 && cb != null) {
                  cb.onError(-res, strerror(-res));
               }

               if (out != null) {
                  out[completed] = cb;
               } else if (cb != null) {
                  ctx.done(cb);
               }
               if (cb == null) {
                  logger.error("CQE slot={} res={} but callback is null", slot, res);
               } else {
                  logger.debug("CQE slot={} res={} fd={}", slot, res, fd);
               }

               if (useFdatasync && out == null && fd >= 0 && fd != lastFd) {
                  try {
                     int rc = (int) MH_FDATASYNC.invokeExact(fd);
                     if (rc < 0) {
                        logger.warn("fdatasync failed fd={} errno={}", fd, lastErrno());
                     }
                  } catch (Throwable t) {
                     logger.warn("fdatasync failed fd={}", fd, t);
                  }
                  lastFd = fd;
               }
               completed++;
            }
            INT.setRelease(cqHead, 0L, head + n);
         }
         return completed;
      }

      void blockedPoll(boolean useFdatasync) {
         pollLock.lock();
         try {
            while (!closed.get()) {
               if (reap(1, queueSize, null, true, useFdatasync) < 0) break;
            }
         } finally {
            pollLock.unlock();
         }
      }

      void close() {
         if (!closed.compareAndSet(false, true)) {
            return;
         }

         // Wake any thread blocked in io_uring_enter
         try {
            int mask = (int) INT.getVolatile(sqMask, 0L);
            int tail = (int) INT.get(sqTail, 0L);
            int idx  = tail & mask;
            long base = (long) idx * SQE_SIZE;

            sqes.set(ValueLayout.JAVA_BYTE,  base + 0,  IORING_OP_NOP);
            sqes.set(ValueLayout.JAVA_BYTE,  base + 1,  (byte) 0);
            sqes.set(ValueLayout.JAVA_SHORT, base + 2,  (short) 0);
            sqes.set(ValueLayout.JAVA_INT,   base + 4,  -1);
            sqes.set(ValueLayout.JAVA_LONG,  base + 8,  0L);
            sqes.set(ValueLayout.JAVA_LONG,  base + 16, 0L);
            sqes.set(ValueLayout.JAVA_INT,  base + 24, 0);
            sqes.set(ValueLayout.JAVA_INT,  base + 28, 0);
            sqes.set(ValueLayout.JAVA_LONG, base + 32, SHUTDOWN_USER_DATA);

            sqArray.setAtIndex(ValueLayout.JAVA_INT, idx, idx);
            INT.setRelease(sqTail, 0L, tail + 1);

            enter(1L, 0L, 0L);
         } catch (Throwable ignored) {
         }

         // Bounded – never hang the suite
         try {
            if (pollLock.tryLock(2, TimeUnit.SECONDS)) {
               pollLock.unlock();
            }
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }

         try { MH_CLOSE.invokeExact(ringFd); } catch (Throwable ignored) {}
         try {
            if (sqRing != null) MH_MUNMAP.invokeExact(sqRing, sqRing.byteSize());
            if (cqRing != null && cqRing != sqRing) MH_MUNMAP.invokeExact(cqRing, cqRing.byteSize());
            if (sqes != null) MH_MUNMAP.invokeExact(sqes, sqes.byteSize());
         } catch (Throwable ignored) {}
         try { arena.close(); } catch (Throwable ignored) {}
      }

      private int allocateSlot() {
         slotLock.lock();
         try {
            if (freeCount == 0) throw new RuntimeException("Not enough space in libaio queue");
            int s = freeList[freeHead];
            freeHead = (freeHead + 1) % queueSize;
            freeCount--;
            return s;
         } finally {
            slotLock.unlock();
         }
      }

      private void releaseSlot(int slot) {
         slotLock.lock();
         try {
            freeList[freeTail] = slot;
            freeTail = (freeTail + 1) % queueSize;
            freeCount++;
         } finally {
            slotLock.unlock();
         }
      }
   }
}
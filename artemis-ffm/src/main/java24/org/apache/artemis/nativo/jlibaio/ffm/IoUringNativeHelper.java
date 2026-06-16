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
package org.apache.artemis.nativo.jlibaio.ffm;

import com.davidvlijmincx.lio.api.JUring;
import com.davidvlijmincx.lio.api.Result;
import com.davidvlijmincx.lio.api.SqeOptions;
import com.davidvlijmincx.lio.api.WriteResult;
import com.davidvlijmincx.lio.api.ReadResult;
import org.apache.artemis.nativo.jlibaio.SubmitInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * A NativeHelper backed by io_uring via JUring.
 * <p>
 * This implementation intentionally avoids the libaio-specific ring structures and
 * instead uses JUring for submission and completion handling.
 * <p>
 * Prototype notes for fair comparison with the FFM libaio path:
 * <ul>
 *   <li>Bounded {@link IoUringSlot} pool (same idea as the IOCB circular pool)
 *       so in-flight ops cannot grow without limit / OOM.</li>
 *   <li>{@code useFdatasync} is honoured. Until JUring exposes
 *       {@code prepareFsync}/{@code IORING_OP_FSYNC}, we issue a libc
 *       {@code fdatasync} after write completions (same durability contract).
 *       Structure is ready to switch to ring-submitted fsync later.</li>
 *   <li>{@code blockedPoll} exits cleanly when the context is closed.</li>
 * </ul>
 */
public class IoUringNativeHelper<Callback extends SubmitInfo> implements NativeHelper<Callback> {

   private static final Logger logger = LoggerFactory.getLogger(IoUringNativeHelper.class);

   /** libc fdatasync — used until JUring gains prepareFsync. */
   private static final MethodHandle FDATASYNC_HANDLE;

   static {
      MethodHandle mh = null;
      try {
         Linker linker = Linker.nativeLinker();
         SymbolLookup stdlib = linker.defaultLookup();
         mh = linker.downcallHandle(
               stdlib.find("fdatasync").orElseThrow(() -> new UnsatisfiedLinkError("fdatasync not found")),
               FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT),
               Linker.Option.captureCallState("errno"));
      } catch (Throwable t) {
         LoggerFactory.getLogger(IoUringNativeHelper.class)
               .warn("Failed to bind fdatasync; useFdatasync will be a no-op: {}", t.getMessage());
      }
      FDATASYNC_HANDLE = mh;
   }

   @Override
   public IOControl<Callback> newContext(int queueSize) {
      String os = System.getProperty("os.name").toLowerCase();
      if (!os.contains("linux")) {
         throw new UnsupportedOperationException("io_uring is only supported on Linux. Current OS: " + os);
      }

      // Try to help JUring find liburing-ffi.so if it's in a non-standard location
      String iouringPath = System.getProperty("artemis.iouring.path");
      if (iouringPath != null && !iouringPath.isEmpty()) {
         try {
            java.io.File libFile = new java.io.File(iouringPath);
            if (libFile.isAbsolute() && libFile.isFile()) {
               System.load(libFile.getAbsolutePath());
            } else {
               System.loadLibrary(iouringPath);
            }
            logger.info("Successfully pre-loaded liburing-ffi from {}", iouringPath);
         } catch (Throwable t) {
            logger.warn("Failed to pre-load liburing-ffi from {}: {}", iouringPath, t.getMessage());
         }
      } else {
         // Try to pre-load liburing-ffi from common names if not provided
         String[] commonNames = {"uring-ffi", "liburing-ffi"};
         for (String name : commonNames) {
            try {
               System.loadLibrary(name);
               logger.info("Successfully pre-loaded {} via loadLibrary", name);
               break;
            } catch (Throwable ignore) {
            }
         }
      }

      IOControl<Callback> io = new IOControl<>();
      // Occupancy tokens so IOControl.getIOCB/putIOCB still bounds in-flight ops.
      // Real per-op state lives in IoUringSlot (circular pool inside IoUringContextData).
      MemorySegment[] iocbPool = new MemorySegment[queueSize];
      for (int i = 0; i < queueSize; i++) {
         MemorySegment iocb = Arena.global().allocate(IOCBInit.IOCB_LAYOUT);
         IOCBInit.setAioData(iocb, i);
         iocbPool[i] = iocb;
      }
      io.setIocbPool(iocbPool);
      io.setQueueSize(queueSize);
      try {
         JUring ring;
         try {
            ring = new JUring(queueSize);
         } catch (Throwable t1) {
            String originalMessage = t1.getMessage();
            logger.info("First attempt to initialize JUring with queueSize {} failed: {}. Retrying with smaller queue size 16.", queueSize, originalMessage);
            // Fallback: try smaller queue sizes which might be more likely to succeed in constrained environments
            try {
               ring = new JUring(16);
            } catch (Throwable t2) {
               logger.info("Second attempt to initialize JUring with queueSize 16 failed: {}. Retrying with queue size 1.", t2.getMessage());
               try {
                  ring = new JUring(1);
               } catch (Throwable t3) {
                  logger.debug("Third attempt to initialize JUring with queueSize 1 failed: {}", t3.getMessage());
                  if (originalMessage != null && originalMessage.contains("Unknown error -1")) {
                     throw new RuntimeException(originalMessage + ". This often indicates insufficient locked memory (RLIMIT_MEMLOCK) or Seccomp restrictions. Try increasing ulimit -l or check Docker security settings.", t1);
                  }
                  throw t1; // throw original
               }
            }
         }
         IoUringContextData data = new IoUringContextData(ring, queueSize);
         io.setCustomContext(data);
      } catch (Throwable t) {
         Throwable root = t;
         while (root.getCause() != null && (root.getMessage() == null || root instanceof ExceptionInInitializerError || root instanceof NoClassDefFoundError)) {
            root = root.getCause();
         }
         String message = root.getMessage();
         if (message == null) {
            message = root.getClass().getName();
         }
         logger.debug("io_uring initialization failed: {}", message, t);
         throw new RuntimeException("io_uring initialization failed: " + message, t);
      }
      return io;
   }

   @Override
   public void deleteContext(IOControl<Callback> ioControl) {
      if (ioControl == null) return;
      Object ctx = ioControl.getCustomContext();
      if (ctx instanceof IoUringContextData data) {
         data.closed = true; // unblock blockedPoll
         JUring ring = data.ring;
         if (ring != null) {
            try {
               ring.close();
            } catch (Throwable t) {
               // ignore
            }
         }
      }
      // free dummy IOCBs (occupancy tokens)
      MemorySegment[] pool = ioControl.iocbPool();
      if (pool != null) {
         for (MemorySegment seg : pool) {
            if (seg != null && seg.address() != 0) {
               FFMNativeHelper.freeBuffer(seg);
            }
         }
      }
   }

   @Override
   public void submitWrite(int fd,
                           IOControl<Callback> ioControl,
                           long position,
                           int size,
                           ByteBuffer bufferWrite,
                           Callback callback) throws IOException {
      Object context = ioControl.getCustomContext();
      if (!(context instanceof IoUringContextData contextData)) {
         throw new IOException("Invalid io_uring context");
      }
      JUring ring = contextData.ring;
      if (ring == null) {
         throw new IOException("JUring is not initialized");
      }
      if (contextData.closed) {
         throw new IOException("io_uring context is closed");
      }

      // Bound in-flight ops via both the slot pool and IOControl occupancy tokens
      IoUringSlot slot = contextData.getSlot();
      if (slot == null) {
         throw new IOException("JUring queue exhausted (slot pool)");
      }
      MemorySegment iocb = ioControl.getIOCB();
      if (iocb == null || iocb.address() == 0) {
         contextData.putSlot(slot);
         throw new IOException("JUring queue exhausted");
      }
      int callbackId = (int) IOCBInit.getAioData(iocb);
      try {
         ioControl.addCallback(callbackId, callback);
         ByteBuffer dup = bufferWrite.duplicate();
         dup.clear();
         MemorySegment seg = MemorySegment.ofBuffer(dup);
         long id = ring.prepareWrite(fd, seg, position, SqeOptions.IOSQE_ASYNC);

         slot.op = IoUringSlot.OP_WRITE;
         slot.fd = fd;
         slot.callback = callback;
         slot.userBuffer = bufferWrite;
         slot.iocbToken = iocb;
         slot.uringId = id;

         ioControl.addUserDataCallback(id, callback);
         contextData.idToIocb.put(id, iocb);
         contextData.idToSlot.put(id, slot);
         ring.submit();
      } catch (Throwable t) {
         ioControl.takeCallback(callbackId);
         ioControl.putIOCB(iocb);
         contextData.putSlot(slot);
         throw new IOException("submitWrite (io_uring) failed", t);
      }
   }

   @Override
   public void submitRead(int fd,
                          IOControl<Callback> ioControl,
                          long position,
                          int size,
                          ByteBuffer bufferWrite,
                          Callback callback) throws IOException {
      Object context = ioControl.getCustomContext();
      if (!(context instanceof IoUringContextData contextData)) {
         throw new IOException("Invalid io_uring context");
      }
      JUring ring = contextData.ring;
      if (ring == null) {
         throw new IOException("JUring is not initialized");
      }
      if (contextData.closed) {
         throw new IOException("io_uring context is closed");
      }

      IoUringSlot slot = contextData.getSlot();
      if (slot == null) {
         throw new IOException("JUring queue exhausted (slot pool)");
      }
      MemorySegment iocb = ioControl.getIOCB();
      if (iocb == null || iocb.address() == 0) {
         contextData.putSlot(slot);
         throw new IOException("JUring queue exhausted");
      }
      int callbackId = (int) IOCBInit.getAioData(iocb);
      try {
         ioControl.addCallback(callbackId, callback);
         // JUring allocate-and-copy path for reads (prototype). Prefer
         // setupReadBufferPool + prepareReadPooled / fixed buffers when available
         // to eliminate the extra copy for a production path.
         long id = ring.prepareRead(fd, size, position, SqeOptions.IOSQE_ASYNC);

         slot.op = IoUringSlot.OP_READ;
         slot.fd = fd;
         slot.callback = callback;
         slot.userBuffer = bufferWrite;
         slot.iocbToken = iocb;
         slot.uringId = id;

         ioControl.addUserDataCallback(id, callback);
         contextData.readTargets.put(id, bufferWrite);
         contextData.idToIocb.put(id, iocb);
         contextData.idToSlot.put(id, slot);
         ring.submit();
      } catch (Throwable t) {
         ioControl.takeCallback(callbackId);
         ioControl.putIOCB(iocb);
         contextData.putSlot(slot);
         throw new IOException("submitRead (io_uring) failed", t);
      }
   }

   @Override
   public int poll(IOControl<Callback> ioControl, Callback[] callbacks, int min, int max) {
      Object context = ioControl.getCustomContext();
      if (!(context instanceof IoUringContextData data)) {
         return -1;
      }
      JUring ring = data.ring;
      if (ring == null) {
         return -1;
      }
      try {
         List<Result> results;
         if (min <= 0) {
            results = ring.peekForBatchResult(max);
         } else {
            results = ring.waitForBatchResult(Math.min(min, max));
         }
         if (results == null || results.isEmpty()) {
            return 0;
         }
         int count = Math.min(results.size(), max);
         int released = 0;
         for (int i = 0; i < count; i++) {
            Result r = results.get(i);
            long id = r.id();
            Callback cb = ioControl.takeUserDataCallback(id);
            MemorySegment iocb = data.idToIocb.remove(id);
            IoUringSlot slot = data.idToSlot.remove(id);
            if (r instanceof ReadResult rr) {
               try {
                  ByteBuffer target = data.readTargets.remove(id);
                  if (target != null) {
                     ByteBuffer src = rr.buffer().asByteBuffer();
                     src.clear();
                     int len = (int) Math.max(0, rr.result());
                     if (len > src.remaining()) {
                        len = Math.max(0, src.remaining());
                     }
                     ByteBuffer dst = target.duplicate();
                     dst.clear();
                     int oldLimit = src.limit();
                     src.limit(src.position() + len);
                     dst.put(src);
                     src.limit(oldLimit);
                  }
               } catch (Throwable t) {
                  logger.warn("Failed to copy ReadResult into target buffer: {}", t.getMessage());
               } finally {
                  try {
                     rr.close();
                  } catch (Exception ignore) {
                  }
               }
            }
            if (cb != null) {
               callbacks[i] = cb;
               long resValue = 0;
               if (r instanceof WriteResult wr) {
                  resValue = wr.result();
               } else if (r instanceof ReadResult rr2) {
                  resValue = rr2.result();
               }
               if (resValue < 0) {
                  cb.onError((int) resValue, "io_uring operation failed");
               } else {
                  cb.done();
               }
               released++;
            } else {
               logger.warn("poll(io_uring): callback not found for id {}", id);
            }
            if (iocb != null) {
               ioControl.putIOCB(iocb);
            }
            if (slot != null) {
               data.putSlot(slot);
            }
         }
         return released;
      } catch (Throwable t) {
         logger.error("poll(io_uring) failed", t);
         return -1;
      }
   }

   @Override
   public void blockedPoll(IOControl<Callback> ioControl, boolean useFdatasync) {
      if (!(ioControl.getCustomContext() instanceof IoUringContextData data)) {
         return;
      }
      JUring ring = data.ring;
      int lastFile = -1;

      // Exit when deleteContext sets data.closed (and closes the ring).
      while (!data.closed) {
         try {
            List<Result> list = ring.waitForBatchResult(1);
            if (list == null || list.isEmpty()) {
               continue;
            }
            for (Result r : list) {
               long id = r.id();
               Callback cb = ioControl.takeUserDataCallback(id);
               MemorySegment iocb = data.idToIocb.remove(id);
               IoUringSlot slot = data.idToSlot.remove(id);
               int fd = (slot != null) ? slot.fd : -1;

               // Durability: match FFMNativeHelper — fdatasync when fd changes
               // (or first fd). Uses libc fdatasync until JUring exposes
               // prepareFsync / IORING_OP_FSYNC; structure is ready to swap.
               if (useFdatasync && fd >= 0 && fd != lastFile) {
                  try {
                     fdatasync(fd);
                  } catch (Throwable t) {
                     logger.warn("blockedPoll: fdatasync failed for fd {}: {}", fd, t.getMessage());
                     if (cb != null) {
                        cb.onError(-1, "fdatasync failed: " + t.getMessage());
                        if (iocb != null) {
                           ioControl.putIOCB(iocb);
                        }
                        if (slot != null) {
                           data.putSlot(slot);
                        }
                        continue;
                     }
                  }
                  lastFile = fd;
               }

               if (r instanceof ReadResult rr) {
                  try {
                     ByteBuffer target = data.readTargets.remove(id);
                     if (target != null) {
                        ByteBuffer src = rr.buffer().asByteBuffer();
                        src.clear();
                        int len = (int) Math.max(0, rr.result());
                        if (len > src.remaining()) {
                           len = Math.max(0, src.remaining());
                        }
                        ByteBuffer dst = target.duplicate();
                        dst.clear();
                        int oldLimit = src.limit();
                        src.limit(src.position() + len);
                        dst.put(src);
                        src.limit(oldLimit);
                     }
                  } catch (Throwable t) {
                     logger.warn("Failed to copy ReadResult into target buffer: {}", t.getMessage());
                  } finally {
                     try {
                        rr.close();
                     } catch (Exception ignore) {
                     }
                  }
               }
               if (cb != null) {
                  long resValue = 0;
                  if (r instanceof WriteResult wr) {
                     resValue = wr.result();
                  } else if (r instanceof ReadResult rr2) {
                     resValue = rr2.result();
                  }
                  if (resValue < 0) {
                     cb.onError((int) resValue, "io_uring operation failed");
                  } else {
                     cb.done();
                  }
               }
               if (iocb != null) {
                  ioControl.putIOCB(iocb);
               }
               if (slot != null) {
                  data.putSlot(slot);
               }
            }
            lastFile = -1; // reset after each harvested batch
         } catch (Throwable t) {
            if (data.closed) {
               break;
            }
            logger.error("blockedPoll(io_uring) error", t);
            // keep going unless the ring is dead / closed
         }
      }
   }

   /**
    * Issue fdatasync(2) for durability. Prototype stand-in for IORING_OP_FSYNC
    * + IORING_FSYNC_DATASYNC until JUring exposes prepareFsync.
    */
   private static void fdatasync(int fd) throws IOException {
      if (FDATASYNC_HANDLE == null) {
         throw new IOException("fdatasync not available (native binding failed)");
      }
      try (Arena arena = Arena.ofConfined()) {
         MemorySegment capture = arena.allocate(Linker.Option.captureStateLayout());
         int res = (int) FDATASYNC_HANDLE.invoke(capture, fd);
         if (res < 0) {
            // Best-effort errno: first int of the capture-state layout is typically errno
            int errno = capture.get(ValueLayout.JAVA_INT, 0);
            throw new IOException("fdatasync(fd=" + fd + ") failed, errno=" + errno);
         }
      } catch (IOException e) {
         throw e;
      } catch (Throwable t) {
         throw new IOException("fdatasync failed", t);
      }
   }
}

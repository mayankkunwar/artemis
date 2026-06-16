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

import org.apache.artemis.nativo.jlibaio.SubmitInfo;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

/**
 * One in-flight io_uring operation. Pre-allocated in a circular pool so we
 * never allocate per-op control state (mirrors the libaio IOCB pool).
 */
final class IoUringSlot {
   static final int OP_NONE = 0;
   static final int OP_READ = 1;
   static final int OP_WRITE = 2;
   static final int OP_FSYNC = 3;

   final int index;
   int op;
   int fd;
   SubmitInfo callback;
   ByteBuffer userBuffer;
   /** Occupancy token from IOControl pool (optional accounting). */
   MemorySegment iocbToken;
   /** JUring user_data id. */
   long uringId;
   /** When true, a follow-up fdatasync must complete before callback.done(). */
   boolean needsFdatasync;
   /** Slot that owns the user callback when this is a linked fsync slot. */
   IoUringSlot parentWriteSlot;

   IoUringSlot(int index) {
      this.index = index;
   }

   void reset() {
      op = OP_NONE;
      fd = -1;
      callback = null;
      userBuffer = null;
      iocbToken = null;
      uringId = -1L;
      needsFdatasync = false;
      parentWriteSlot = null;
   }
}

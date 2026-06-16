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

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-context state for the io_uring backend.
 * <p>
 * Holds the JUring instance plus a bounded slot pool (the io_uring equivalent
 * of the libaio IOCB circular pool) so we never allocate per-op control
 * structures and cannot OOM under load.
 */
class IoUringContextData {
   final JUring ring;
   final ConcurrentHashMap<Long, ByteBuffer> readTargets = new ConcurrentHashMap<>();
   final ConcurrentHashMap<Long, MemorySegment> idToIocb = new ConcurrentHashMap<>();
   /** Maps JUring user_data id -> in-flight slot. */
   final ConcurrentHashMap<Long, IoUringSlot> idToSlot = new ConcurrentHashMap<>();
   /** Set true by deleteContext so blockedPoll can exit cleanly. */
   volatile boolean closed = false;

   private final IoUringSlot[] slots;
   private final int queueSize;
   private final AtomicInteger freeCount;
   private int getIdx;
   private int putIdx;
   private final Object slotLock = new Object();

   IoUringContextData(JUring ring, int queueSize) {
      this.ring = ring;
      this.queueSize = Math.max(0, queueSize);
      this.slots = new IoUringSlot[this.queueSize];
      for (int i = 0; i < this.queueSize; i++) {
         slots[i] = new IoUringSlot(i);
      }
      this.freeCount = new AtomicInteger(this.queueSize);
      this.getIdx = 0;
      this.putIdx = 0;
   }

   /**
    * Take a free slot from the circular pool.
    *
    * @return null if the pool is exhausted (same semantics as getIOCB() == null)
    */
   IoUringSlot getSlot() {
      synchronized (slotLock) {
         if (freeCount.get() <= 0 || queueSize <= 0) {
            return null;
         }
         IoUringSlot slot = slots[getIdx];
         getIdx++;
         if (getIdx >= queueSize) {
            getIdx = 0;
         }
         freeCount.decrementAndGet();
         slot.reset();
         return slot;
      }
   }

   /** Return a slot to the circular pool after completion or failure. */
   void putSlot(IoUringSlot slot) {
      if (slot == null) {
         return;
      }
      synchronized (slotLock) {
         if (queueSize <= 0 || freeCount.get() >= queueSize) {
            return;
         }
         slot.reset();
         slots[putIdx] = slot;
         putIdx++;
         if (putIdx >= queueSize) {
            putIdx = 0;
         }
         freeCount.incrementAndGet();
      }
   }

   int freeSlots() {
      return freeCount.get();
   }
}

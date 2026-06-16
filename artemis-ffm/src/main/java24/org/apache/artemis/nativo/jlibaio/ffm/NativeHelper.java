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

import java.io.IOException;
import java.nio.ByteBuffer;

public interface NativeHelper<Callback extends SubmitInfo> {

   IOControl<Callback> newContext(int queueSize);

   void deleteContext(IOControl<Callback> ioControl);

   void submitWrite(int fd,
                    IOControl<Callback> ioControl,
                    long position,
                    int size,
                    ByteBuffer bufferWrite,
                    Callback callback) throws IOException;

   void submitRead(int fd,
                   IOControl<Callback> ioControl,
                   long position,
                   int size,
                   ByteBuffer bufferWrite,
                   Callback callback) throws IOException;

   int poll(IOControl<Callback> ioControl, Callback[] callbacks, int min, int max);

   void blockedPoll(IOControl<Callback> ioControl, boolean useFdatasync);
}

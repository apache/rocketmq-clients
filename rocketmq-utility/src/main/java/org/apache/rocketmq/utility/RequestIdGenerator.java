/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.utility;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

public class RequestIdGenerator {
    private static final RequestIdGenerator INSTANCE = new RequestIdGenerator();

    private final byte[] name;

    public static RequestIdGenerator getInstance() {
        return INSTANCE;
    }

    private RequestIdGenerator() {
        ByteBuffer nameBuffer = ByteBuffer.allocate(16);
        final byte[] macAddressBytes = UtilAll.macAddress();
        nameBuffer.put(macAddressBytes, 0, 6);

        ByteBuffer pidBuffer = ByteBuffer.allocate(4);
        pidBuffer.order(ByteOrder.BIG_ENDIAN);
        final int pid = UtilAll.processId();
        pidBuffer.putInt(pid);

        nameBuffer.put(pidBuffer.array(), 2, 2);
        nameBuffer.putLong(System.nanoTime());
        this.name = nameBuffer.array();
    }

    public String next() {
        return UUID.nameUUIDFromBytes(name).toString();
    }
}

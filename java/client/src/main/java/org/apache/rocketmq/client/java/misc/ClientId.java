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

package org.apache.rocketmq.client.java.misc;

import java.util.concurrent.atomic.AtomicLong;

public class ClientId {
    private static final AtomicLong INDEX = new AtomicLong(0);
    private static final String CLIENT_ID_SEPARATOR = "@";

    private final long index;
    private final String id;

    public ClientId() {
        this.index = INDEX.getAndIncrement();
        StringBuilder sb = new StringBuilder();
        final String hostName = Utilities.hostName();
        sb.append(hostName);
        sb.append(CLIENT_ID_SEPARATOR);
        sb.append(Utilities.processId());
        sb.append(CLIENT_ID_SEPARATOR);
        sb.append(index);
        sb.append(CLIENT_ID_SEPARATOR);
        sb.append(Long.toString(System.nanoTime(), 36));
        this.id = sb.toString();
    }

    /**
     * Client index within current process.
     *
     * @return index.
     */
    public long getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return id;
    }
}

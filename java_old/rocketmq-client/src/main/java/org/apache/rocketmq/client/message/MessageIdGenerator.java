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

package org.apache.rocketmq.client.message;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.utility.UtilAll;

public class MessageIdGenerator {
    private static final MessageIdGenerator INSTANCE = new MessageIdGenerator();
    private static final String VERSION = "01";

    private final String prefix;
    private final long secondsSinceCustomEpoch;
    private final long secondsStartTimestamp;
    private long seconds;
    private final AtomicInteger sequence;

    private MessageIdGenerator() {
        ByteBuffer prefixBuffer = ByteBuffer.allocate(8);
        prefixBuffer.order(ByteOrder.BIG_ENDIAN);

        byte[] prefix0 = UtilAll.macAddress();
        prefixBuffer.put(prefix0, 0, 6);

        ByteBuffer pidBuffer = ByteBuffer.allocate(4);
        pidBuffer.order(ByteOrder.BIG_ENDIAN);
        final int pid = UtilAll.processId();
        pidBuffer.putInt(pid);

        // Copy the lower 2 bytes
        prefixBuffer.put(pidBuffer.array(), 2, 2);

        prefixBuffer.flip();
        prefix = VERSION + UtilAll.encodeHexString(prefixBuffer, false);

        secondsSinceCustomEpoch = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - customEpochMillis());
        secondsStartTimestamp = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());
        seconds = deltaSeconds();

        sequence = new AtomicInteger(0);
    }

    public static MessageIdGenerator getInstance() {
        return INSTANCE;
    }

    private long customEpochMillis() {
        // 2021-01-01 00:00:00
        final Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, 2021);
        calendar.set(Calendar.MONTH, Calendar.JANUARY);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        return calendar.getTime().getTime();
    }

    public String next() {
        ByteBuffer suffixBuffer = ByteBuffer.allocate(8);
        suffixBuffer.order(ByteOrder.BIG_ENDIAN);

        final ByteBuffer deltaSecondsBuffer = ByteBuffer.allocate(8);
        deltaSecondsBuffer.order(ByteOrder.BIG_ENDIAN);
        final long deltaSeconds = deltaSeconds();
        if (seconds != deltaSeconds) {
            seconds = deltaSeconds;
        }
        deltaSecondsBuffer.putLong(seconds);
        suffixBuffer.put(deltaSecondsBuffer.array(), 4, 4);
        suffixBuffer.putInt(sequence.getAndIncrement());

        suffixBuffer.flip();
        return prefix + UtilAll.encodeHexString(suffixBuffer, false);
    }

    private long deltaSeconds() {
        return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime()) - secondsStartTimestamp + secondsSinceCustomEpoch;
    }
}

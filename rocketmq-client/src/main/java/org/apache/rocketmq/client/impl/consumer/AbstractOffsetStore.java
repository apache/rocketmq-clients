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

package org.apache.rocketmq.client.impl.consumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.message.MessageQueue;

/**
 * Persist offset frequently is expensive in most case. Provide a simple way to persist offset per
 */
public abstract class AbstractOffsetStore implements OffsetStore {
    private long nanoTime;
    private final long persistPeriodSeconds;

    private final ConcurrentMap<MessageQueue, Long> offsetTable;

    public AbstractOffsetStore(long persistPeriodSeconds) {
        this.nanoTime = System.nanoTime();
        this.persistPeriodSeconds = persistPeriodSeconds;
        this.offsetTable = new ConcurrentHashMap<MessageQueue, Long>();
    }

    @Override
    public void start() {
        final Map<MessageQueue, Long> queueOffsetTable = loadOffset();
        if (null != queueOffsetTable) {
            offsetTable.putAll(queueOffsetTable);
        }
    }

    /**
     * Load offset from disk or other external storage.
     *
     * @return persisted offset.
     */
    public abstract Map<MessageQueue, Long> loadOffset();

    /**
     * Persist offset to disk or other external storage.
     *
     * @param offsetTable offset to persist.
     */
    public abstract void persistOffset(Map<MessageQueue, Long> offsetTable);

    @Override
    public void updateOffset(MessageQueue mq, long offset) {
        offsetTable.put(mq, offset);
        if (System.nanoTime() - nanoTime > persistPeriodSeconds) {
            persistOffset(offsetTable);
            nanoTime = System.nanoTime();
        }
    }

    @Override
    public long readOffset(MessageQueue mq) {
        final Long offset = offsetTable.get(mq);
        if (null == offset) {
            return NULL_OFFSET;
        }
        return offset;
    }
}

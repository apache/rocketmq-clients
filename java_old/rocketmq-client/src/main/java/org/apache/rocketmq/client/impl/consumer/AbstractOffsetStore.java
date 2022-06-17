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

import com.google.common.base.Optional;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.utility.ExecutorServices;
import org.apache.rocketmq.utility.ThreadFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persist offset frequently is expensive in most case. Provide a simple way to persist offset for each
 * {@link MessageQueue}
 */
public abstract class AbstractOffsetStore implements OffsetStore {
    private static final Logger log = LoggerFactory.getLogger(AbstractOffsetStore.class);

    private final long persistPeriodSeconds;

    private final ConcurrentMap<MessageQueue, Long> offsetTable;
    private final ScheduledExecutorService offsetPersistScheduler;

    public AbstractOffsetStore(long persistPeriodSeconds) {
        this.persistPeriodSeconds = persistPeriodSeconds;
        this.offsetTable = new ConcurrentHashMap<MessageQueue, Long>();
        this.offsetPersistScheduler = new ScheduledThreadPoolExecutor(
                1,
                new ThreadFactoryImpl("OffsetPersistScheduler"));
    }

    @Override
    public void start() {
        final Map<MessageQueue, Long> queueOffsetTable = loadOffset();
        if (null != queueOffsetTable) {
            offsetTable.putAll(queueOffsetTable);
        }
        this.offsetPersistScheduler.scheduleWithFixedDelay(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            persistOffset(offsetTable);
                        } catch (Throwable t) {
                            log.error("Exception occurs while trying to persist offset", t);
                        }
                    }
                },
                persistPeriodSeconds,
                persistPeriodSeconds,
                TimeUnit.SECONDS);
    }

    @Override
    public void shutdown() {
        try {
            if (!ExecutorServices.awaitTerminated(offsetPersistScheduler)) {
                log.error("[Bug] Timeout to shutdown the offset persist scheduler.");
            }
        } catch (Throwable t) {
            log.error("Failed to shutdown the offset persist scheduler.", t);
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
    }

    @Override
    public Optional<Long> readOffset(MessageQueue mq) {
        final Long offset = offsetTable.get(mq);
        if (null == offset) {
            return Optional.absent();
        }
        return Optional.of(offset);
    }
}

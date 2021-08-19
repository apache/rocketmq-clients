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

import org.apache.rocketmq.client.message.MessageQueue;

/**
 * Persist offset frequently is expensive in most case. Provide a simple way to persist offset per
 */
public abstract class AbstractOffsetStore implements OffsetStore {
    private long nanoTime;
    private final long persistPeriodSeconds;

    public AbstractOffsetStore(long persistPeriodSeconds) {
        this.nanoTime = System.nanoTime();
        this.persistPeriodSeconds = persistPeriodSeconds;
    }

    /**
     * Persist offset to disk or other external storage.
     *
     * @param mq     offset owner.
     * @param offset the next offset of {@link MessageQueue}
     */
    public abstract void persistOffset(MessageQueue mq, long offset);

    @Override
    public void updateOffset(MessageQueue mq, long offset) {
        if (System.nanoTime() - nanoTime > persistPeriodSeconds) {
            persistOffset(mq, offset);
            nanoTime = System.nanoTime();
        }
    }
}

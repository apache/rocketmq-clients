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

package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.message.MessageQueue;

public class PullMessageQuery {
    private static final long PULL_MESSAGE_TIMEOUT_MILLIS = 3 * 1000;

    private final MessageQueue messageQueue;
    private final FilterExpression filterExpression;
    private final long queueOffset;
    private final int batchSize;
    private final long awaitTimeMillis;
    private final long timeoutMillis;

    // TODO: compare awaitTimeMillis and timeoutMillis here.
    public PullMessageQuery(MessageQueue messageQueue, FilterExpression filterExpression, long queueOffset,
                            int batchSize, long awaitTimeMillis, long timeoutMillis) {
        this.messageQueue = messageQueue;
        this.filterExpression = filterExpression;
        this.queueOffset = queueOffset;
        this.batchSize = batchSize;
        this.awaitTimeMillis = awaitTimeMillis;
        this.timeoutMillis = timeoutMillis;
    }

    // TODO: compare awaitTimeMillis and timeoutMillis here.
    public PullMessageQuery(MessageQueue messageQueue, long queueOffset, int batchSize, long awaitTimeMillis,
                            long timeoutMillis) {
        this.messageQueue = messageQueue;
        this.filterExpression = new FilterExpression();
        this.queueOffset = queueOffset;
        this.batchSize = batchSize;
        this.awaitTimeMillis = awaitTimeMillis;
        this.timeoutMillis = timeoutMillis;
    }

    // TODO: compare awaitTimeMillis and timeoutMillis here.
    public PullMessageQuery(MessageQueue messageQueue, long queueOffset, int batchSize, long awaitTimeMillis) {
        this.messageQueue = messageQueue;
        this.filterExpression = new FilterExpression();
        this.queueOffset = queueOffset;
        this.batchSize = batchSize;
        this.awaitTimeMillis = awaitTimeMillis;
        this.timeoutMillis = PULL_MESSAGE_TIMEOUT_MILLIS;
    }

    public MessageQueue getMessageQueue() {
        return this.messageQueue;
    }

    public FilterExpression getFilterExpression() {
        return this.filterExpression;
    }

    public long getQueueOffset() {
        return this.queueOffset;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    public long getAwaitTimeMillis() {
        return this.awaitTimeMillis;
    }

    public long getTimeoutMillis() {
        return this.timeoutMillis;
    }
}

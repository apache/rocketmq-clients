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

package org.apache.rocketmq.client.java.impl.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.java.metrics.MessageCacheObserver;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;

public class MessageCacheObserverImpl implements MessageCacheObserver {
    private final ConcurrentMap<MessageQueueImpl, ProcessQueue> processQueueTable;

    public MessageCacheObserverImpl(ConcurrentMap<MessageQueueImpl, ProcessQueue> processQueueTable) {
        this.processQueueTable = processQueueTable;
    }

    @Override
    public Map<String, Long> getCachedMessageCount() {
        Map<String, Long> cachedMessageCountMap = new HashMap<>();
        for (ProcessQueue pq : processQueueTable.values()) {
            final String topic = pq.getMessageQueue().getTopic();
            long count = cachedMessageCountMap.containsKey(topic) ? cachedMessageCountMap.get(topic) : 0;
            count += pq.getInflightMessageCount();
            count += pq.getPendingMessageCount();
            cachedMessageCountMap.put(topic, count);
        }
        return cachedMessageCountMap;
    }

    @Override
    public Map<String, Long> getCachedMessageBytes() {
        Map<String, Long> cachedMessageBytesMap = new HashMap<>();
        for (ProcessQueue pq : processQueueTable.values()) {
            final String topic = pq.getMessageQueue().getTopic();
            long bytes = cachedMessageBytesMap.containsKey(topic) ? cachedMessageBytesMap.get(topic) : 0;
            bytes += pq.getCachedMessageBytes();
            cachedMessageBytesMap.put(topic, bytes);
        }
        return cachedMessageBytesMap;
    }
}

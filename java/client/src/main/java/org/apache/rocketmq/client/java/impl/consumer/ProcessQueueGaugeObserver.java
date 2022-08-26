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

import io.opentelemetry.api.common.Attributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.java.metrics.GaugeEnum;
import org.apache.rocketmq.client.java.metrics.GaugeObserver;
import org.apache.rocketmq.client.java.metrics.MetricLabels;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;

public class ProcessQueueGaugeObserver implements GaugeObserver {
    private final ConcurrentMap<MessageQueueImpl, ProcessQueue> processQueueTable;
    private final ClientId clientId;
    private final String consumerGroup;
    private final List<GaugeEnum> gauges;

    public ProcessQueueGaugeObserver(ConcurrentMap<MessageQueueImpl, ProcessQueue> processQueueTable, ClientId clientId,
        String consumerGroup) {
        this.processQueueTable = processQueueTable;
        this.clientId = clientId;
        this.consumerGroup = consumerGroup;
        this.gauges = new ArrayList<>();
        gauges.add(GaugeEnum.CONSUMER_CACHED_MESSAGES);
        gauges.add(GaugeEnum.CONSUMER_CACHED_BYTES);
    }

    @Override
    public List<GaugeEnum> getGauges() {
        return gauges;
    }

    @Override
    public Map<Attributes, Double> getValues(GaugeEnum gauge) {
        switch (gauge) {
            case CONSUMER_CACHED_MESSAGES:
                Map<Attributes, Double> cachedMessageCountMap = new HashMap<>();
                for (ProcessQueue pq : processQueueTable.values()) {
                    final String topic = pq.getMessageQueue().getTopic();
                    Attributes attributes = Attributes.builder()
                        .put(MetricLabels.TOPIC, topic)
                        .put(MetricLabels.CONSUMER_GROUP, consumerGroup)
                        .put(MetricLabels.CLIENT_ID, clientId.toString())
                        .build();
                    double count = cachedMessageCountMap.containsKey(attributes) ?
                        cachedMessageCountMap.get(attributes) : 0;
                    count += pq.getCachedMessageCount();
                    cachedMessageCountMap.put(attributes, count);
                }
                return cachedMessageCountMap;
            case CONSUMER_CACHED_BYTES:
                Map<Attributes, Double> cachedMessageBytesMap = new HashMap<>();
                for (ProcessQueue pq : processQueueTable.values()) {
                    final String topic = pq.getMessageQueue().getTopic();
                    Attributes attributes = Attributes.builder()
                        .put(MetricLabels.TOPIC, topic)
                        .put(MetricLabels.CONSUMER_GROUP, consumerGroup)
                        .put(MetricLabels.CLIENT_ID, clientId.toString())
                        .build();
                    double bytes = cachedMessageBytesMap.containsKey(attributes) ?
                        cachedMessageBytesMap.get(attributes) : 0;
                    bytes += pq.getCachedMessageBytes();
                    cachedMessageBytesMap.put(attributes, bytes);
                }
                return cachedMessageBytesMap;
            default:
                return new HashMap<>();
        }
    }
}

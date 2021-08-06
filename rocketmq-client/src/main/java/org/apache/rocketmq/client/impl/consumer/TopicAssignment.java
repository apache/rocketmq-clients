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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.Immutable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.TopicRouteData;

@Slf4j
@ToString
@EqualsAndHashCode
@Immutable
public class TopicAssignment {
    private static final ThreadLocal<AtomicInteger> PARTITION_INDEX = new ThreadLocal<AtomicInteger>();

    @Getter
    private final List<Assignment> assignmentList;

    public TopicAssignment(TopicRouteData topicRouteData) {
        this.assignmentList = new ArrayList<Assignment>();
        final List<Partition> partitions = topicRouteData.getPartitions();
        for (Partition partition : partitions) {
            if (MixAll.MASTER_BROKER_ID != partition.getBroker().getId()) {
                continue;
            }
            final MessageQueue mq = new MessageQueue(partition);
            final Assignment assignment = new Assignment(mq, MessageRequestMode.PULL);
            assignmentList.add(assignment);
        }
    }

    public TopicAssignment(List<apache.rocketmq.v1.Assignment> assignmentList) {
        this.assignmentList = new ArrayList<Assignment>();

        for (apache.rocketmq.v1.Assignment item : assignmentList) {
            MessageQueue messageQueue =
                    new MessageQueue(new Partition(item.getPartition()));

            MessageRequestMode mode = MessageRequestMode.POP;
            switch (item.getMode()) {
                case PULL:
                    mode = MessageRequestMode.PULL;
                    break;
                case POP:
                    mode = MessageRequestMode.POP;
                    break;
                default:
                    log.warn("Unknown message request mode={}, default to pop.", item.getMode());
            }
            this.assignmentList.add(new Assignment(messageQueue, mode));
        }
    }

    public static int getNextPartitionIndex() {
        if (null == PARTITION_INDEX.get()) {
            PARTITION_INDEX.set(new AtomicInteger(RandomUtils.nextInt()));
        }
        return PARTITION_INDEX.get().getAndIncrement();
    }
}

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

package org.apache.rocketmq.client.impl.producer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import apache.rocketmq.v1.Broker;
import apache.rocketmq.v1.Permission;
import apache.rocketmq.v1.Resource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.route.Endpoints;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.client.tools.TestBase;
import org.testng.annotations.Test;

public class SendingTopicRouteDataTest extends TestBase {
    private final String brokerNamePrefix = "broker";
    private final String endpointsHostPrefix = "host";

    @Test
    public void testTakePartitionsWithSameBroker() throws ClientException {
        int partitionCount = 3;
        List<apache.rocketmq.v1.Partition> partitionList = new ArrayList<apache.rocketmq.v1.Partition>();
        for (int i = 0; i < partitionCount; i++) {
            // same broker.
            final apache.rocketmq.v1.Partition partition = dummyProtoPartition(dummyProtoTopic0(), dummyProtoBroker0(),
                                                                               Permission.READ_WRITE, i);
            partitionList.add(partition);

        }
        final TopicRouteData topicRouteData = new TopicRouteData(partitionList);
        final SendingTopicRouteData sendingPartitions = new SendingTopicRouteData(topicRouteData);
        assertFalse(sendingPartitions.isEmpty());
        Set<Endpoints> isolated = new HashSet<Endpoints>();
        int count;
        {
            count = partitionCount;
            final List<Partition> partitions = sendingPartitions.takePartitions(isolated, count);
            assertTrue(topicRouteData.getPartitions().containsAll(partitions));
            assertEquals(partitions.size(), 1);
        }
        {
            count = partitionCount - 1;
            final List<Partition> partitions = sendingPartitions.takePartitions(isolated, count);
            assertTrue(topicRouteData.getPartitions().containsAll(partitions));
            assertEquals(partitions.size(), 1);

        }
        {
            count = partitionCount + 1;
            final List<Partition> partitions = sendingPartitions.takePartitions(isolated, count);
            assertTrue(topicRouteData.getPartitions().containsAll(partitions));
            assertEquals(partitions.size(), 1);
        }
    }

    @Test
    public void testTakePartitionsWithDifferentBroker() throws ClientException {
        int partitionCount = 3;
        List<apache.rocketmq.v1.Partition> partitionList = new ArrayList<apache.rocketmq.v1.Partition>();
        for (int i = 0; i < partitionCount; i++) {
            // different broker.
            final apache.rocketmq.v1.Partition partition = dummyProtoPartition(dummyProtoTopic0(), dummyProtoBroker(
                    brokerNamePrefix + i), Permission.READ_WRITE, i);
            partitionList.add(partition);

        }
        final TopicRouteData topicRouteData = new TopicRouteData(partitionList);
        final SendingTopicRouteData sendingPartitions = new SendingTopicRouteData(topicRouteData);
        assertFalse(sendingPartitions.isEmpty());
        Set<Endpoints> isolated = new HashSet<Endpoints>();
        int count;
        {
            count = partitionCount;
            final List<Partition> partitions = sendingPartitions.takePartitions(isolated, count);
            assertTrue(topicRouteData.getPartitions().containsAll(partitions));
            assertTrue(partitions.containsAll(topicRouteData.getPartitions()));
        }
        {
            count = partitionCount - 1;
            final List<Partition> partitions = sendingPartitions.takePartitions(isolated, count);
            assertTrue(topicRouteData.getPartitions().containsAll(partitions));
            assertEquals(partitions.size(), count);

        }
        {
            count = partitionCount + 1;
            final List<Partition> partitions = sendingPartitions.takePartitions(isolated, count);
            assertTrue(topicRouteData.getPartitions().containsAll(partitions));
            assertEquals(partitions.size(), partitionCount);
        }
    }

    @Test
    public void testTakePartitionsWithIsolated() throws ClientException {
        int partitionNotIsolatedCount = 2;
        List<apache.rocketmq.v1.Partition> partitionList = new ArrayList<apache.rocketmq.v1.Partition>();
        final Resource dummyTopic = dummyProtoTopic0();
        for (int i = 0; i < partitionNotIsolatedCount; i++) {
            // different broker.
            final String name = brokerNamePrefix + i;
            final Broker broker = dummyProtoBroker(name, MixAll.MASTER_BROKER_ID,
                                                   dummyProtoEndpoints(dummyProtoAddress(endpointsHostPrefix + i, i)));
            final apache.rocketmq.v1.Partition partition = dummyProtoPartition(dummyTopic, broker,
                                                                               Permission.READ_WRITE, i);
            partitionList.add(partition);

        }
        // isolated.
        final String isolatedHostName = "isolatedHost";
        final String isolatedBrokerName = "isolatedBrokerName";
        final apache.rocketmq.v1.Endpoints isolatedProtoEndpoints =
                dummyProtoEndpoints(dummyProtoAddress(isolatedHostName, 8080));
        final Broker isolatedProtoBroker = dummyProtoBroker(isolatedBrokerName, MixAll.MASTER_BROKER_ID,
                                                            isolatedProtoEndpoints);
        final apache.rocketmq.v1.Partition isolatedProtoPartition = dummyProtoPartition(dummyTopic, isolatedProtoBroker,
                                                                                        Permission.READ_WRITE, 0);
        partitionList.add(isolatedProtoPartition);

        final TopicRouteData topicRouteData = new TopicRouteData(partitionList);
        final SendingTopicRouteData sendingPartitions = new SendingTopicRouteData(topicRouteData);
        assertFalse(sendingPartitions.isEmpty());

        Set<Endpoints> isolated = new HashSet<Endpoints>();
        final Endpoints isolatedEndpoints = new Endpoints(isolatedProtoEndpoints);
        isolated.add(isolatedEndpoints);
        final Partition isolatedPartition = new Partition(isolatedProtoPartition);
        int count;
        {
            count = 1 + partitionNotIsolatedCount;
            final List<Partition> partitions = sendingPartitions.takePartitions(isolated, count);
            assertEquals(partitions.size(), partitionNotIsolatedCount);
            assertFalse(partitions.contains(isolatedPartition));
        }
        {
            count = partitionNotIsolatedCount;
            final List<Partition> partitions = sendingPartitions.takePartitions(isolated, count);
            assertEquals(partitions.size(), partitionNotIsolatedCount);

        }
        {
            count = partitionNotIsolatedCount + 1;
            final List<Partition> partitions = sendingPartitions.takePartitions(isolated, count);
            assertEquals(partitions.size(), partitionNotIsolatedCount);
        }
    }

    @Test
    public void testTakePartitionsWithAllIsolated() throws ClientException {
        int partitionCount = 3;
        List<apache.rocketmq.v1.Partition> partitionList = new ArrayList<apache.rocketmq.v1.Partition>();
        Set<Endpoints> isolated = new HashSet<Endpoints>();
        final Resource dummyTopic = dummyProtoTopic0();
        for (int i = 0; i < partitionCount; i++) {
            // different broker.
            final String name = brokerNamePrefix + i;
            final apache.rocketmq.v1.Endpoints protoEndpoints =
                    dummyProtoEndpoints(dummyProtoAddress(endpointsHostPrefix + i, i));
            final Endpoints endpoints = new Endpoints(protoEndpoints);
            isolated.add(endpoints);
            final Broker broker = dummyProtoBroker(name, MixAll.MASTER_BROKER_ID,
                                                   protoEndpoints);
            final apache.rocketmq.v1.Partition partition = dummyProtoPartition(dummyTopic, broker,
                                                                               Permission.READ_WRITE, i);
            partitionList.add(partition);

        }

        final TopicRouteData topicRouteData = new TopicRouteData(partitionList);
        final SendingTopicRouteData sendingPartitions = new SendingTopicRouteData(topicRouteData);
        assertFalse(sendingPartitions.isEmpty());

        int count;
        {
            count = partitionCount;
            final List<Partition> partitions = sendingPartitions.takePartitions(isolated, count);
            assertTrue(topicRouteData.getPartitions().containsAll(partitions));
            assertTrue(partitions.containsAll(topicRouteData.getPartitions()));
        }
        {
            count = partitionCount - 1;
            final List<Partition> partitions = sendingPartitions.takePartitions(isolated, count);
            assertTrue(topicRouteData.getPartitions().containsAll(partitions));
            assertEquals(partitions.size(), count);

        }
        {
            count = partitionCount + 1;
            final List<Partition> partitions = sendingPartitions.takePartitions(isolated, count);
            assertTrue(topicRouteData.getPartitions().containsAll(partitions));
            assertTrue(partitions.containsAll(topicRouteData.getPartitions()));
        }
    }
}
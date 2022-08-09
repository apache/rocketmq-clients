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

package org.apache.rocketmq.client.java.impl.producer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.Permission;
import apache.rocketmq.v2.Resource;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import io.grpc.Metadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.route.TopicRouteData;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProducerImplTest extends TestBase {
    private final ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
        .setEndpoints(FAKE_ACCESS_POINT).build();

    @SuppressWarnings("SameParameterValue")
    private ProducerImpl createProducerWithTopic(String topic) {
        List<MessageQueue> messageQueueList = new ArrayList<>();
        MessageQueue mq = MessageQueue.newBuilder().setTopic(Resource.newBuilder().setName(topic))
            .setPermission(Permission.READ_WRITE)
            .addAcceptMessageTypes(MessageType.NORMAL)
            .setBroker(Broker.newBuilder().setName(FAKE_BROKER_NAME_0).setEndpoints(fakePbEndpoints0()))
            .setId(0).build();
        messageQueueList.add(mq);
        final TopicRouteData topicRouteData = new TopicRouteData(messageQueueList);
        final PublishingLoadBalancer publishingLoadBalancer = new PublishingLoadBalancer(topicRouteData);
        final Set<String> set = new HashSet<>();
        set.add(topic);
        final ProducerImpl producer = Mockito.spy(new ProducerImpl(clientConfiguration, set, 1, null));
        producer.publishingRouteDataCache.put(topic, publishingLoadBalancer);
        final Service mockedService = mock(Service.class);
        Mockito.doReturn(mockedService).when(producer).startAsync();
        Mockito.doReturn(mockedService).when(producer).stopAsync();
        Mockito.doReturn(true).when(producer).isRunning();
        producer.startAsync().awaitRunning();
        return producer;
    }

    @Test(expected = IllegalStateException.class)
    public void testSendBeforeStartup() throws ClientException {
        final Set<String> set = Collections.singleton(FAKE_TOPIC_0);
        final ProducerImpl producer = Mockito.spy(new ProducerImpl(clientConfiguration, set, 1, null));
        final Message message = fakeMessage(FAKE_TOPIC_0);
        producer.send(message);
    }

    @Test
    public void testSendWithTopic() throws Exception {
        final ProducerImpl producer = createProducerWithTopic(FAKE_TOPIC_0);
        final Message message = fakeMessage(FAKE_TOPIC_0);
        final MessageQueueImpl messageQueue = fakeMessageQueueImpl(FAKE_TOPIC_0);
        final SendReceiptImpl sendReceiptImpl = fakeSendReceiptImpl(messageQueue);
        Mockito.doReturn(Futures.immediateFuture(Collections.singletonList(sendReceiptImpl)))
            .when(producer).send0(any(Metadata.class), any(Endpoints.class), anyList(), any(MessageQueueImpl.class));
        producer.send(message);
        verify(producer, times(1)).send0(any(Metadata.class), any(Endpoints.class), anyList(),
            any(MessageQueueImpl.class));
        producer.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSendFailureWithTopic() throws ClientException {
        final ProducerImpl producer = createProducerWithTopic(FAKE_TOPIC_0);
        final Message message = fakeMessage(FAKE_TOPIC_0);
        final Exception exception = new IllegalArgumentException();
        Mockito.doReturn(Futures.immediateFailedFuture(exception))
            .when(producer).send0(any(Metadata.class), any(Endpoints.class), anyList(), any(MessageQueueImpl.class));
        producer.send(message);
        final int maxAttempts = producer.producerSettings.getRetryPolicy().getMaxAttempts();
        verify(producer, times(maxAttempts)).send0(any(Metadata.class), any(Endpoints.class), anyList(),
            any(MessageQueueImpl.class));
        producer.close();
    }
}
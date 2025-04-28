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

package org.apache.rocketmq.test.client;

import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SystemProperties;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.PushConsumerBuilder;
import org.apache.rocketmq.test.server.BaseMockServerImpl;
import org.apache.rocketmq.test.server.GrpcServerIntegrationTest;
import org.apache.rocketmq.test.server.MockServer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.rocketmq.client.apis.consumer.FilterExpression.SUB_ALL;
import static org.awaitility.Awaitility.await;

public class FifoConsumeAcceleratorTest extends GrpcServerIntegrationTest {
    private final String topic = "topic";
    private final int messageBatchSize = 30;
    MockServer serverImpl;

    Map<String, Message> messageMap = new ConcurrentHashMap<>();
    LinkedBlockingDeque<List<String>> messageList = new LinkedBlockingDeque<>();

    class MockServerImpl extends BaseMockServerImpl {
        public MockServerImpl(String topic) {
            super(topic, "broker", true);
        }

        @Override
        public void receiveMessage(ReceiveMessageRequest request,
            StreamObserver<ReceiveMessageResponse> responseObserver) {
            try {
                List<String> messages = messageList.poll(10, TimeUnit.SECONDS);
                if (messages == null || messages.isEmpty()) {
                    responseObserver.onNext(ReceiveMessageResponse.newBuilder().setStatus(messageNotFound).build());
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onNext(ReceiveMessageResponse.newBuilder().setStatus(mockStatus).build());
                    for (String message : messages) {
                        responseObserver.onNext(ReceiveMessageResponse.newBuilder().setMessage(messageMap.get(message)).build());
                    }
                    responseObserver.onNext(ReceiveMessageResponse.newBuilder().setDeliveryTimestamp(Timestamps.fromMillis(System.currentTimeMillis())).build());
                    responseObserver.onCompleted();
                }
            } catch (InterruptedException e) {
                responseObserver.onError(e);
            }
        }
    }

    @Before
    public void setUp() throws Exception {
        serverImpl = new MockServerImpl(topic);
        setUpServer(serverImpl, port);
        serverImpl.setPort(port);
    }

    @Test
    public void test() throws Exception {
        List<String> consumeList = new CopyOnWriteArrayList<>();
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        SessionCredentialsProvider sessionCredentialsProvider = new StaticSessionCredentialsProvider("", "");

        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(serverImpl.getLocalEndpoints())
            .setCredentialProvider(sessionCredentialsProvider)
            .build();
        PushConsumerBuilder builder = provider.newPushConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            .setConsumerGroup("group")
            .setEnableFifoConsumeAccelerator(true)
            .setSubscriptionExpressions(Collections.singletonMap(topic, SUB_ALL))
            .setMessageListener(messageView -> {
                consumeList.add(messageView.getMessageId().toString());
                return ConsumeResult.SUCCESS;
            });

        try (PushConsumer ignore = builder.build()) {
            Random random = new Random();
            random.setSeed(System.currentTimeMillis());
            for (int testTime = 0; testTime < 100; testTime++) {
                List<String> messageBatch = new ArrayList<>();
                for (int i = 0; i < messageBatchSize; i++) {
                    messageBatch.add(buildMessage(random.nextInt() % 2 == 0 ? "A" : "B", i));
                }
                messageList.add(messageBatch);
                await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                    Assert.assertEquals(messageBatchSize, consumeList.size());
                });
                checkConsumeOrder(consumeList);
            }
        }
    }

    public void checkConsumeOrder(List<String> consumeList) {
        Map<String, List<Integer>> messageGroupByMessageGroup = new HashMap<>();
        for (String messageId : consumeList) {
            String messageGroup = messageMap.get(messageId).getSystemProperties().getMessageGroup();
            if (StringUtils.isNotEmpty(messageGroup)) {
                messageGroupByMessageGroup.computeIfAbsent(messageGroup, k -> new ArrayList<>())
                    .add(getIndexFromMessage(messageMap.get(messageId)));
            } else {
                messageGroupByMessageGroup.computeIfAbsent("", k -> new ArrayList<>())
                    .add(getIndexFromMessage(messageMap.get(messageId)));
            }
        }
        for (List<Integer> list : messageGroupByMessageGroup.values()) {
            List<Integer> sortedList = new ArrayList<>(list);
            Collections.sort(sortedList);
            Assert.assertEquals(list, sortedList);
        }
        consumeList.clear();
    }

    public String buildMessage(String messageGroup, int index) {
        String messageId = messageGroup + index;
        Message message = Message.newBuilder()
            .setTopic(Resource.newBuilder().setName(topic).build())
            .setBody(ByteString.copyFrom(new byte[10]))
            .setSystemProperties(SystemProperties.newBuilder()
                .setMessageId(messageId)
                .setMessageGroup(messageGroup)
                .build())
            .build();
        messageMap.put(messageId, message);
        return messageId;
    }

    public int getIndexFromMessage(Message message) {
        String messageGroup = message.getSystemProperties().getMessageGroup();
        return Integer.parseInt(message.getSystemProperties().getMessageId().substring(messageGroup.length()));
    }
}

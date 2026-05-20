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

import static org.apache.rocketmq.client.apis.consumer.FilterExpression.SUB_ALL;
import static org.awaitility.Awaitility.await;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SystemProperties;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.BatchPolicy;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.PushConsumerBuilder;
import org.apache.rocketmq.test.server.BaseMockServerImpl;
import org.apache.rocketmq.test.server.GrpcServerIntegrationTest;
import org.apache.rocketmq.test.server.MockServer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BatchPushConsumerTest extends GrpcServerIntegrationTest {
    private final String topic = "topic";
    private final LinkedBlockingDeque<List<String>> messageQueue = new LinkedBlockingDeque<>();
    private MockServer serverImpl;

    static class BatchMockServerImpl extends BaseMockServerImpl {
        private final LinkedBlockingDeque<List<String>> messageQueue;
        public final AtomicInteger ackCount = new AtomicInteger(0);
        public final AtomicInteger nackCount = new AtomicInteger(0);

        public BatchMockServerImpl(String topic, boolean fifo, LinkedBlockingDeque<List<String>> messageQueue) {
            super(topic, "broker", fifo);
            this.messageQueue = messageQueue;
        }

        @Override
        public void receiveMessage(ReceiveMessageRequest request,
            StreamObserver<ReceiveMessageResponse> responseObserver) {
            try {
                List<String> messages = messageQueue.poll(10, TimeUnit.SECONDS);
                if (messages == null || messages.isEmpty()) {
                    responseObserver.onNext(ReceiveMessageResponse.newBuilder()
                        .setStatus(messageNotFound).build());
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onNext(ReceiveMessageResponse.newBuilder()
                        .setStatus(mockStatus).build());
                    for (String messageId : messages) {
                        Message msg = Message.newBuilder()
                            .setTopic(Resource.newBuilder().setName(topic).build())
                            .setBody(ByteString.copyFrom(new byte[10]))
                            .setSystemProperties(SystemProperties.newBuilder()
                                .setMessageId(messageId)
                                .build())
                            .build();
                        responseObserver.onNext(ReceiveMessageResponse.newBuilder()
                            .setMessage(msg).build());
                    }
                    responseObserver.onNext(ReceiveMessageResponse.newBuilder()
                        .setDeliveryTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                        .build());
                    responseObserver.onCompleted();
                }
            } catch (InterruptedException e) {
                responseObserver.onError(e);
            }
        }

        @Override
        public void ackMessage(AckMessageRequest request,
            StreamObserver<AckMessageResponse> responseObserver) {
            ackCount.incrementAndGet();
            responseObserver.onNext(AckMessageResponse.newBuilder().setStatus(mockStatus).build());
            responseObserver.onCompleted();
        }

        @Override
        public void changeInvisibleDuration(ChangeInvisibleDurationRequest request,
            StreamObserver<ChangeInvisibleDurationResponse> responseObserver) {
            nackCount.incrementAndGet();
            responseObserver.onNext(ChangeInvisibleDurationResponse.newBuilder()
                .setStatus(mockStatus).build());
            responseObserver.onCompleted();
        }
    }

    @Before
    public void setUp() throws Exception {
        serverImpl = new BatchMockServerImpl(topic, false, messageQueue);
        setUpServer(serverImpl, port);
        serverImpl.setPort(port);
    }

    private PushConsumerBuilder createBuilder(
        org.apache.rocketmq.client.apis.consumer.BatchMessageListener listener,
        BatchPolicy policy) throws Exception {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        SessionCredentialsProvider creds = new StaticSessionCredentialsProvider("", "");
        ClientConfiguration config = ClientConfiguration.newBuilder()
            .setEndpoints(serverImpl.getLocalEndpoints())
            .setCredentialProvider(creds)
            .build();
        return provider.newPushConsumerBuilder()
            .setClientConfiguration(config)
            .setConsumerGroup("group")
            .setSubscriptionExpressions(Collections.singletonMap(topic, SUB_ALL))
            .setBatchMessageListener(listener, policy);
    }

    @Test
    public void testConcurrentBatchConsumeSuccess() throws Exception {
        int batchSize = 4;
        BatchPolicy policy = new BatchPolicy(batchSize, Duration.ofSeconds(5));
        CopyOnWriteArrayList<Integer> batchSizes = new CopyOnWriteArrayList<>();

        try (PushConsumer ignore = createBuilder(messageViews -> {
            batchSizes.add(messageViews.size());
            return ConsumeResult.SUCCESS;
        }, policy).build()) {
            List<String> messages = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                messages.add("msg-" + i);
            }
            messageQueue.add(messages);

            await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                Assert.assertFalse("Should have received at least one batch", batchSizes.isEmpty());
                Assert.assertEquals(batchSize, batchSizes.get(0).intValue());
            });

            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(batchSize, ((BatchMockServerImpl) serverImpl).ackCount.get()));
        }
    }

    @Test
    public void testConcurrentBatchConsumeFailure() throws Exception {
        int batchSize = 3;
        BatchPolicy policy = new BatchPolicy(batchSize, Duration.ofSeconds(5));
        AtomicInteger listenerCallCount = new AtomicInteger(0);

        try (PushConsumer ignore = createBuilder(messageViews -> {
            listenerCallCount.incrementAndGet();
            return ConsumeResult.FAILURE;
        }, policy).build()) {
            List<String> messages = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                messages.add("fail-msg-" + i);
            }
            messageQueue.add(messages);

            await().atMost(30, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertTrue("Listener should have been called", listenerCallCount.get() >= 1));

            // Give some time for ack processing
            Thread.sleep(2000);
            Assert.assertEquals("No messages should be acked on FAILURE",
                0, ((BatchMockServerImpl) serverImpl).ackCount.get());
        }
    }

    @Test
    public void testBatchSizeNotExceedMaxBatchCount() throws Exception {
        int maxBatchCount = 3;
        BatchPolicy policy = new BatchPolicy(maxBatchCount, Duration.ofMillis(500));
        CopyOnWriteArrayList<Integer> batchSizes = new CopyOnWriteArrayList<>();

        try (PushConsumer ignore = createBuilder(messageViews -> {
            batchSizes.add(messageViews.size());
            return ConsumeResult.SUCCESS;
        }, policy).build()) {
            // Send more messages than maxBatchCount
            List<String> messages = new ArrayList<>();
            for (int i = 0; i < 7; i++) {
                messages.add("batch-msg-" + i);
            }
            messageQueue.add(messages);

            await().atMost(30, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(7, ((BatchMockServerImpl) serverImpl).ackCount.get()));

            for (int size : batchSizes) {
                Assert.assertTrue("Batch size should not exceed maxBatchCount: " + size,
                    size <= maxBatchCount);
            }
        }
    }

    @Test
    public void testTimeoutFlush() throws Exception {
        int maxBatchCount = 100;
        BatchPolicy policy = new BatchPolicy(maxBatchCount, Duration.ofMillis(500));
        CopyOnWriteArrayList<Integer> batchSizes = new CopyOnWriteArrayList<>();

        try (PushConsumer ignore = createBuilder(messageViews -> {
            batchSizes.add(messageViews.size());
            return ConsumeResult.SUCCESS;
        }, policy).build()) {
            // Send fewer messages than maxBatchCount — should be flushed by timeout
            List<String> messages = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                messages.add("timeout-msg-" + i);
            }
            messageQueue.add(messages);

            await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                Assert.assertFalse("Batch should have been flushed by timeout", batchSizes.isEmpty());
                Assert.assertEquals(3, batchSizes.get(0).intValue());
            });
        }
    }

    @Test
    public void testFifoBatchConsumeSuccess() throws Exception {
        // Create a FIFO mock server
        LinkedBlockingDeque<List<String>> fifoQueue = new LinkedBlockingDeque<>();
        BatchMockServerImpl fifoServer = new BatchMockServerImpl(topic, true, fifoQueue);
        setUpServer(fifoServer, 0);
        fifoServer.setPort(port);

        int batchSize = 4;
        BatchPolicy policy = new BatchPolicy(batchSize, Duration.ofSeconds(5));
        CopyOnWriteArrayList<List<String>> receivedBatches = new CopyOnWriteArrayList<>();

        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        SessionCredentialsProvider creds = new StaticSessionCredentialsProvider("", "");
        ClientConfiguration config = ClientConfiguration.newBuilder()
            .setEndpoints(fifoServer.getLocalEndpoints())
            .setCredentialProvider(creds)
            .build();

        try (PushConsumer ignore = provider.newPushConsumerBuilder()
            .setClientConfiguration(config)
            .setConsumerGroup("group")
            .setSubscriptionExpressions(Collections.singletonMap(topic, SUB_ALL))
            .setBatchMessageListener(messageViews -> {
                List<String> ids = new ArrayList<>();
                messageViews.forEach(mv -> ids.add(mv.getMessageId().toString()));
                receivedBatches.add(ids);
                return ConsumeResult.SUCCESS;
            }, policy)
            .build()) {

            List<String> messages = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                messages.add("fifo-msg-" + i);
            }
            fifoQueue.add(messages);

            await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                Assert.assertFalse("Should receive at least one batch", receivedBatches.isEmpty());
                Assert.assertEquals(batchSize, receivedBatches.get(0).size());
            });

            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(batchSize, fifoServer.ackCount.get()));
        }
    }

    @Test
    public void testFifoBatchConsumeFailureThenSuccess() throws Exception {
        LinkedBlockingDeque<List<String>> fifoQueue = new LinkedBlockingDeque<>();
        BatchMockServerImpl fifoServer = new BatchMockServerImpl(topic, true, fifoQueue);
        setUpServer(fifoServer, 0);
        fifoServer.setPort(port);

        int batchSize = 2;
        BatchPolicy policy = new BatchPolicy(batchSize, Duration.ofSeconds(5));
        AtomicInteger callCount = new AtomicInteger(0);

        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        SessionCredentialsProvider creds = new StaticSessionCredentialsProvider("", "");
        ClientConfiguration config = ClientConfiguration.newBuilder()
            .setEndpoints(fifoServer.getLocalEndpoints())
            .setCredentialProvider(creds)
            .build();

        try (PushConsumer ignore = provider.newPushConsumerBuilder()
            .setClientConfiguration(config)
            .setConsumerGroup("group")
            .setSubscriptionExpressions(Collections.singletonMap(topic, SUB_ALL))
            .setBatchMessageListener(messageViews -> {
                int count = callCount.incrementAndGet();
                return count == 1 ? ConsumeResult.FAILURE : ConsumeResult.SUCCESS;
            }, policy)
            .build()) {

            List<String> messages = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                messages.add("retry-msg-" + i);
            }
            fifoQueue.add(messages);

            await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                Assert.assertTrue("Should be called at least twice (1 fail + 1 success)",
                    callCount.get() >= 2);
                Assert.assertEquals(batchSize, fifoServer.ackCount.get());
            });
        }
    }
}

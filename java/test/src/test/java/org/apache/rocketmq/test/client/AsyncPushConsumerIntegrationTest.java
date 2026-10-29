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
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Digest;
import apache.rocketmq.v2.DigestType;
import apache.rocketmq.v2.Encoding;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.NotifyClientTerminationResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SystemProperties;
import apache.rocketmq.v2.TelemetryCommand;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.java.message.MessageIdCodec;
import org.apache.rocketmq.test.server.BaseMockServerImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Exercises the public asynchronous listener API through a local gRPC protocol stub, without an external broker.
 */
public class AsyncPushConsumerIntegrationTest {
    private static final String TOPIC = "async-topic";
    private static final String GROUP = "async-group";
    private static final String RECEIPT_HANDLE = "async-receipt-handle";

    private final CompletableFuture<ConsumeResult> processing = new CompletableFuture<>();
    private final CountDownLatch listenerInvoked = new CountDownLatch(1);
    private final AtomicInteger listenerCalls = new AtomicInteger();
    private ExecutorService serverExecutor;
    private ScheduledExecutorService responseScheduler;
    private LocalServer serverImpl;
    private Server server;
    private PushConsumer consumer;

    @Before
    public void setUp() throws Exception {
        serverExecutor = Executors.newFixedThreadPool(4);
        responseScheduler = Executors.newSingleThreadScheduledExecutor();
        serverImpl = new LocalServer(responseScheduler);
        server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
            .executor(serverExecutor).addService(serverImpl).build().start();
        serverImpl.setPort(server.getPort());
    }

    @After
    public void tearDown() throws Exception {
        processing.complete(ConsumeResult.SUCCESS);
        serverImpl.releaseAcknowledgements();
        try {
            closeConsumer();
        } finally {
            server.shutdownNow();
            responseScheduler.shutdownNow();
            serverExecutor.shutdownNow();
            Assert.assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
            Assert.assertTrue(responseScheduler.awaitTermination(5, TimeUnit.SECONDS));
            Assert.assertTrue(serverExecutor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testAcknowledgesOnlyOnceAfterProcessingCompletes() throws Exception {
        startConsumer();
        Assert.assertTrue(listenerInvoked.await(10, TimeUnit.SECONDS));
        Assert.assertTrue(serverImpl.receiveRequests.get(0).getAutoRenew());
        await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            Assert.assertTrue(serverImpl.ackRequests.isEmpty());
            Assert.assertTrue(serverImpl.retryRequests.isEmpty());
        });

        Assert.assertTrue(processing.complete(ConsumeResult.SUCCESS));
        Assert.assertFalse(processing.complete(ConsumeResult.FAILURE));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
            Assert.assertEquals(1, serverImpl.ackRequests.size()));
        closeConsumer();

        Assert.assertEquals(1, listenerCalls.get());
        Assert.assertEquals(1, serverImpl.ackRequests.size());
        Assert.assertTrue(serverImpl.retryRequests.isEmpty());
        AckMessageRequest request = serverImpl.ackRequests.get(0);
        Assert.assertEquals(GROUP, request.getGroup().getName());
        Assert.assertEquals(TOPIC, request.getTopic().getName());
        Assert.assertEquals(1, request.getEntriesCount());
        Assert.assertEquals(serverImpl.messageId, request.getEntries(0).getMessageId());
        Assert.assertEquals(RECEIPT_HANDLE, request.getEntries(0).getReceiptHandle());
    }

    @Test
    public void testExceptionalStageUsesNegativeAcknowledgementAndRetriesRpcFailure() throws Exception {
        serverImpl.failFirstRetryRequest.set(true);
        startConsumer();
        Assert.assertTrue(listenerInvoked.await(10, TimeUnit.SECONDS));
        processing.completeExceptionally(new IllegalStateException("processing failed"));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Assert.assertEquals(2, serverImpl.retryRequests.size());
            Assert.assertTrue(serverImpl.ackRequests.isEmpty());
        });
        closeConsumer();

        Assert.assertEquals(1, listenerCalls.get());
        Assert.assertTrue(serverImpl.ackRequests.isEmpty());
        Assert.assertEquals(2, serverImpl.retryRequests.size());
        for (ChangeInvisibleDurationRequest request : serverImpl.retryRequests) {
            Assert.assertEquals(GROUP, request.getGroup().getName());
            Assert.assertEquals(TOPIC, request.getTopic().getName());
            Assert.assertEquals(serverImpl.messageId, request.getMessageId());
            Assert.assertEquals(RECEIPT_HANDLE, request.getReceiptHandle());
            Assert.assertTrue(Durations.toMillis(request.getInvisibleDuration()) > 0);
        }
    }

    @Test
    public void testCloseWaitsForProcessingAndAcknowledgementResponse() throws Exception {
        serverImpl.holdAcknowledgements();
        startConsumer();
        Assert.assertTrue(listenerInvoked.await(10, TimeUnit.SECONDS));
        ExecutorService closingExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch closingStarted = new CountDownLatch(1);
        Future<?> closing = closingExecutor.submit(() -> {
            closingStarted.countDown();
            consumer.close();
            return null;
        });
        try {
            Assert.assertTrue(closingStarted.await(5, TimeUnit.SECONDS));
            assertStillClosing(closing);
            Assert.assertTrue(serverImpl.ackRequests.isEmpty());

            processing.complete(ConsumeResult.SUCCESS);
            Assert.assertTrue(serverImpl.ackReceived.await(5, TimeUnit.SECONDS));
            assertStillClosing(closing);
            Assert.assertEquals(1, serverImpl.ackRequests.size());

            serverImpl.releaseAcknowledgements();
            closing.get(5, TimeUnit.SECONDS);
            consumer = null;
            Assert.assertEquals(1, serverImpl.ackRequests.size());
        } finally {
            processing.complete(ConsumeResult.SUCCESS);
            serverImpl.releaseAcknowledgements();
            try {
                closing.get(10, TimeUnit.SECONDS);
                consumer = null;
            } finally {
                closingExecutor.shutdownNow();
                Assert.assertTrue(closingExecutor.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    private void startConsumer() throws Exception {
        ClientConfiguration configuration = ClientConfiguration.newBuilder()
            .setEndpoints(serverImpl.getLocalEndpoints()).enableSsl(false)
            .setRequestTimeout(Duration.ofSeconds(5))
            .build();
        consumer = ClientServiceProvider.loadService().newPushConsumerBuilder()
            .setClientConfiguration(configuration).setConsumerGroup(GROUP).setConsumptionThreadCount(1)
            .setSubscriptionExpressions(Collections.singletonMap(TOPIC, SUB_ALL))
            .setAsyncMessageListener(messageView -> {
                listenerCalls.incrementAndGet();
                listenerInvoked.countDown();
                return processing;
            }).build();
    }

    private void closeConsumer() throws Exception {
        if (null != consumer) {
            consumer.close();
            consumer = null;
        }
    }

    private void assertStillClosing(Future<?> closing) throws Exception {
        try {
            // Exceed the existing one-second shutdown delay so the assertion checks the outstanding operation.
            closing.get(1500, TimeUnit.MILLISECONDS);
            Assert.fail("Close must wait for outstanding processing and acknowledgement operations");
        } catch (TimeoutException expected) {
            // The test explicitly releases the operation after this assertion.
        }
    }

    private static class LocalServer extends BaseMockServerImpl {
        private final ScheduledExecutorService scheduler;
        private final String messageId = MessageIdCodec.getInstance().nextMessageId().toString();
        private final AtomicBoolean messageDelivered = new AtomicBoolean();
        private final AtomicBoolean failFirstRetryRequest = new AtomicBoolean();
        private final List<ReceiveMessageRequest> receiveRequests = new CopyOnWriteArrayList<>();
        private final List<AckMessageRequest> ackRequests = new CopyOnWriteArrayList<>();
        private final List<ChangeInvisibleDurationRequest> retryRequests = new CopyOnWriteArrayList<>();
        private final CountDownLatch ackReceived = new CountDownLatch(1);
        private final List<StreamObserver<AckMessageResponse>> heldAcknowledgements = new ArrayList<>();
        private boolean acknowledgementsHeld;

        private LocalServer(ScheduledExecutorService scheduler) {
            super(TOPIC);
            this.scheduler = scheduler;
        }

        @Override
        public StreamObserver<TelemetryCommand> telemetry(StreamObserver<TelemetryCommand> responseObserver) {
            return super.telemetry(new StreamObserver<TelemetryCommand>() {
                @Override
                public void onNext(TelemetryCommand command) {
                    responseObserver.onNext(command.toBuilder().setSettings(command.getSettings().toBuilder()
                        .setSubscription(command.getSettings().getSubscription().toBuilder()
                            .setReceiveBatchSize(1).setLongPollingTimeout(Durations.fromMillis(100)))).build());
                }

                @Override
                public void onError(Throwable error) {
                    responseObserver.onError(error);
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            });
        }

        @Override
        public void receiveMessage(ReceiveMessageRequest request,
            StreamObserver<ReceiveMessageResponse> responseObserver) {
            receiveRequests.add(request);
            if (messageDelivered.compareAndSet(false, true)) {
                responseObserver.onNext(ReceiveMessageResponse.newBuilder().setStatus(mockStatus).build());
                responseObserver.onNext(ReceiveMessageResponse.newBuilder().setMessage(message()).build());
                responseObserver.onNext(ReceiveMessageResponse.newBuilder()
                    .setDeliveryTimestamp(Timestamps.fromMillis(System.currentTimeMillis())).build());
                responseObserver.onCompleted();
                return;
            }
            // End empty long polls asynchronously so the stub does not spin or occupy its RPC workers.
            scheduler.schedule(() -> {
                responseObserver.onNext(ReceiveMessageResponse.newBuilder().setStatus(messageNotFound).build());
                responseObserver.onCompleted();
            }, 100, TimeUnit.MILLISECONDS);
        }

        @Override
        public synchronized void ackMessage(AckMessageRequest request,
            StreamObserver<AckMessageResponse> responseObserver) {
            ackRequests.add(request);
            ackReceived.countDown();
            if (acknowledgementsHeld) {
                heldAcknowledgements.add(responseObserver);
            } else {
                acknowledge(responseObserver);
            }
        }

        private synchronized void holdAcknowledgements() {
            acknowledgementsHeld = true;
        }

        private synchronized void releaseAcknowledgements() {
            acknowledgementsHeld = false;
            for (StreamObserver<AckMessageResponse> observer : heldAcknowledgements) {
                acknowledge(observer);
            }
            heldAcknowledgements.clear();
        }

        private void acknowledge(StreamObserver<AckMessageResponse> observer) {
            observer.onNext(AckMessageResponse.newBuilder().setStatus(mockStatus).build());
            observer.onCompleted();
        }

        @Override
        public void changeInvisibleDuration(ChangeInvisibleDurationRequest request,
            StreamObserver<ChangeInvisibleDurationResponse> responseObserver) {
            retryRequests.add(request);
            apache.rocketmq.v2.Status status = failFirstRetryRequest.compareAndSet(true, false)
                ? mockStatus.toBuilder().setCode(Code.INTERNAL_SERVER_ERROR).build() : mockStatus;
            responseObserver.onNext(ChangeInvisibleDurationResponse.newBuilder().setStatus(status)
                .setReceiptHandle(RECEIPT_HANDLE).build());
            responseObserver.onCompleted();
        }

        @Override
        public void notifyClientTermination(NotifyClientTerminationRequest request,
            StreamObserver<NotifyClientTerminationResponse> responseObserver) {
            responseObserver.onNext(NotifyClientTerminationResponse.newBuilder().setStatus(mockStatus).build());
            responseObserver.onCompleted();
        }

        private Message message() {
            return Message.newBuilder().setTopic(Resource.newBuilder().setName(TOPIC))
                .setBody(ByteString.copyFrom("foobar", StandardCharsets.UTF_8))
                .setSystemProperties(SystemProperties.newBuilder()
                    .setMessageId(messageId).setMessageType(MessageType.NORMAL)
                    .setBornHost("127.0.0.1").setDeliveryAttempt(1)
                    .setBodyDigest(Digest.newBuilder().setType(DigestType.CRC32).setChecksum("9EF61F95"))
                    .setBodyEncoding(Encoding.IDENTITY).setReceiptHandle(RECEIPT_HANDLE)
                    .setInvisibleDuration(Durations.fromSeconds(30))).build();
        }
    }
}

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import apache.rocketmq.v2.Address;
import apache.rocketmq.v2.Assignment;
import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Endpoints;
import apache.rocketmq.v2.ExponentialBackoff;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.Permission;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.TelemetryCommand;
import com.google.protobuf.Duration;
import io.grpc.stub.StreamObserver;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.test.helper.ResponseWriter;
import org.apache.rocketmq.test.server.GrpcServerIntegrationTest;
import org.apache.rocketmq.test.server.MockServer;
import org.junit.Before;
import org.junit.Test;

public class AttemptIdIntegrationTest extends GrpcServerIntegrationTest {
    private final String topic = "topic";
    private final String broker = "broker";
    private final ResponseWriter responseWriter = ResponseWriter.getInstance();
    private final Status mockStatus = Status.newBuilder()
        .setCode(Code.OK)
        .setMessage("mock test")
        .build();

    private final List<String> attemptIdList = new CopyOnWriteArrayList<>();
    private final AtomicBoolean serverDeadlineFlag = new AtomicBoolean(true);

    @Before
    public void setUp() throws Exception {
        MockServer serverImpl = new MockServer() {
            @Override
            public void queryRoute(QueryRouteRequest request, StreamObserver<QueryRouteResponse> responseObserver) {
                responseWriter.write(responseObserver, QueryRouteResponse.newBuilder()
                    .setStatus(mockStatus)
                    .addMessageQueues(MessageQueue.newBuilder()
                        .setTopic(Resource.newBuilder()
                            .setName(topic).build())
                        .setId(0)
                        .setPermission(Permission.READ_WRITE)
                        .setBroker(Broker.newBuilder()
                            .setName(broker)
                            .setId(0)
                            .setEndpoints(Endpoints.newBuilder()
                                .addAddresses(Address.newBuilder()
                                    .setHost("127.0.0.1")
                                    .setPort(port)
                                    .build())
                                .build())
                            .build())
                        .addAcceptMessageTypes(MessageType.NORMAL)
                        .build())
                    .build());
            }

            @Override
            public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
                responseWriter.write(responseObserver, HeartbeatResponse.newBuilder().setStatus(mockStatus)
                    .build());
            }

            @Override
            public void queryAssignment(QueryAssignmentRequest request,
                StreamObserver<QueryAssignmentResponse> responseObserver) {
                responseWriter.write(responseObserver, QueryAssignmentResponse.newBuilder().setStatus(mockStatus)
                        .addAssignments(Assignment.newBuilder()
                            .setMessageQueue(MessageQueue.newBuilder()
                                .setTopic(Resource.newBuilder()
                                    .setName(topic).build())
                                .setId(0)
                                .setPermission(Permission.READ_WRITE)
                                .setBroker(Broker.newBuilder()
                                    .setName(broker)
                                    .setId(0)
                                    .setEndpoints(Endpoints.newBuilder()
                                        .addAddresses(Address.newBuilder()
                                            .setHost("127.0.0.1")
                                            .setPort(port)
                                            .build())
                                        .build())
                                    .build())
                                .addAcceptMessageTypes(MessageType.NORMAL)
                                .build())
                            .build())
                    .build());
            }

            @Override
            public void receiveMessage(ReceiveMessageRequest request,
                StreamObserver<ReceiveMessageResponse> responseObserver) {
                // prevent too much request
                if (attemptIdList.size() >= 3) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                attemptIdList.add(request.getAttemptId());
                if (serverDeadlineFlag.compareAndSet(true, false)) {
                    // timeout
                } else {
                    responseObserver.onNext(ReceiveMessageResponse.newBuilder().setStatus(mockStatus).build());
                    responseObserver.onCompleted();
                }
            }

            @Override
            public StreamObserver<TelemetryCommand> telemetry(StreamObserver<TelemetryCommand> responseObserver) {
                return new StreamObserver<TelemetryCommand>() {
                    @Override
                    public void onNext(TelemetryCommand value) {
                        responseObserver.onNext(value.toBuilder().setStatus(mockStatus)
                            .setSettings(value.getSettings().toBuilder()
                                .setBackoffPolicy(value.getSettings().getBackoffPolicy().toBuilder()
                                    .setMaxAttempts(16)
                                    .setExponentialBackoff(ExponentialBackoff.newBuilder()
                                        .setInitial(Duration.newBuilder()
                                            .setSeconds(1).build())
                                        .setMax(Duration.newBuilder()
                                            .setSeconds(10).build())
                                        .setMultiplier(1.5f)
                                        .build()))).build());
                        responseObserver.onCompleted();
                    }

                    @Override
                    public void onError(Throwable t) {
                    }

                    @Override
                    public void onCompleted() {
                        responseObserver.onCompleted();
                    }
                };
            }
        };

        setUpServer(serverImpl, port);
        serverImpl.setPort(port);
    }

    @Test
    public void test() throws Exception {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        String accessKey = "yourAccessKey";
        String secretKey = "yourSecretKey";
        SessionCredentialsProvider sessionCredentialsProvider =
            new StaticSessionCredentialsProvider(accessKey, secretKey);

        String endpoints = "127.0.0.1" + ":" + port;
        int timeout = 1000;
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(endpoints)
            .setCredentialProvider(sessionCredentialsProvider)
            .setRequestTimeout(java.time.Duration.of(timeout, ChronoUnit.MILLIS))
            .build();
        String tag = "yourMessageTagA";
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        String consumerGroup = "yourConsumerGroup";
        PushConsumer pushConsumer = provider.newPushConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            .setConsumerGroup(consumerGroup)
            .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
            .setMessageListener(messageView -> ConsumeResult.SUCCESS)
            .build();
        try {
            await().atMost(java.time.Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(attemptIdList.size()).isGreaterThanOrEqualTo(3);
                assertThat(attemptIdList.get(0)).isEqualTo(attemptIdList.get(1));
                assertThat(attemptIdList.get(0)).isNotEqualTo(attemptIdList.get(2));
            });
        } finally {
            pushConsumer.close();
        }
    }
}

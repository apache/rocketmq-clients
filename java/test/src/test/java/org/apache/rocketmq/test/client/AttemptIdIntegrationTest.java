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

import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
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
import org.apache.rocketmq.test.server.BaseMockServerImpl;
import org.apache.rocketmq.test.server.GrpcServerIntegrationTest;
import org.apache.rocketmq.test.server.MockServer;
import org.junit.Before;
import org.junit.Test;

public class AttemptIdIntegrationTest extends GrpcServerIntegrationTest {
    private final String topic = "topic";
    private MockServer serverImpl;

     static class MockServerImpl extends BaseMockServerImpl {
        public final List<String> attemptIdList = new CopyOnWriteArrayList<>();
        public final AtomicBoolean serverDeadlineFlag = new AtomicBoolean(true);

        public MockServerImpl(String topic) {
            super(topic);
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
    }

    @Before
    public void setUp() throws Terminate {
        serverImpl = new MockServerImpl(topic);
        setUpServer(serverImpl, port);
        serverImpl.setPort(port);
    }

    @Test
    public void test() throws Terminate {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        String accessKey = "yourAccessKey";
        String secretKey = "yourSecretKey";
        SessionCredentialsProvider sessionCredentialsProvider =
            new StaticSessionCredentialsProvider(accessKey, secretKey);

        int timeout = 1000;
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(serverImpl.getLocalEndpoints())
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
                List<String> attemptIdList = ((MockServerImpl) serverImpl).attemptIdList;
                assertThat(attemptIdList.size()).isGreaterThanOrEqualTo(3);
                assertThat(attemptIdList.get(0)).isEqualTo(attemptIdList.get(1));
                assertThat(attemptIdList.get(0)).isNotEqualTo(attemptIdList.get(2));
            });
        } finally {
            pushConsumer.close();
        }
    }
}

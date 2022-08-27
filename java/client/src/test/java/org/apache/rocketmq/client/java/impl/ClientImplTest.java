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

package org.apache.rocketmq.client.java.impl;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.Permission;
import apache.rocketmq.v2.PrintThreadStackTraceCommand;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.VerifyMessageCommand;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.TopicRouteData;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;
import org.mockito.Mockito;

@SuppressWarnings("unchecked")
public class ClientImplTest extends TestBase {
    private final ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
        .setEndpoints(FAKE_ENDPOINTS).build();

    private ClientImpl createClient() {
        return Mockito.spy(new ClientImpl(clientConfiguration, new HashSet<>()) {
            @Override
            public Settings getSettings() {
                return null;
            }

            @Override
            public NotifyClientTerminationRequest wrapNotifyClientTerminationRequest() {
                return null;
            }

            @Override
            public HeartbeatRequest wrapHeartbeatRequest() {
                return null;
            }
        });
    }

    @Test
    public void testTelemetry() throws ClientException {
        final Endpoints endpoints = fakeEndpoints();
        TelemetryCommand command = TelemetryCommand.newBuilder().build();
        final StreamObserver<TelemetryCommand> observer =
            (StreamObserver<TelemetryCommand>) Mockito.mock(StreamObserver.class);
        final ClientImpl client = createClient();
        doReturn(observer).when(client).telemetry(any(Endpoints.class), any(StreamObserver.class));
        doNothing().when(observer).onNext(any(TelemetryCommand.class));
        client.telemetry(endpoints, command);
        verify(client, times(1)).telemetry(any(Endpoints.class), any(StreamObserver.class));
        verify(observer, times(1)).onNext(eq(command));
    }

    @Test
    public void testOnPrintThreadStackTraceCommand() throws ClientException {
        PrintThreadStackTraceCommand command = PrintThreadStackTraceCommand.newBuilder().build();
        final Endpoints endpoints = fakeEndpoints();
        final StreamObserver<TelemetryCommand> observer =
            (StreamObserver<TelemetryCommand>) Mockito.mock(StreamObserver.class);
        final ClientImpl client = createClient();
        doReturn(observer).when(client).telemetry(any(Endpoints.class), any(StreamObserver.class));
        doNothing().when(observer).onNext(any(TelemetryCommand.class));
        client.onPrintThreadStackTraceCommand(endpoints, command);
        await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> {
            verify(client, times(1)).telemetry(any(Endpoints.class), any(StreamObserver.class));
            verify(observer, times(1)).onNext(any(TelemetryCommand.class));
        });
    }

    @Test
    public void testOnVerifyMessageCommand() throws ClientException {
        VerifyMessageCommand command = VerifyMessageCommand.newBuilder().build();
        final Endpoints endpoints = fakeEndpoints();
        final StreamObserver<TelemetryCommand> observer =
            (StreamObserver<TelemetryCommand>) Mockito.mock(StreamObserver.class);
        final ClientImpl client = createClient();
        doReturn(observer).when(client).telemetry(any(Endpoints.class), any(StreamObserver.class));
        doNothing().when(observer).onNext(any(TelemetryCommand.class));
        client.onVerifyMessageCommand(endpoints, command);
        verify(client, times(1)).telemetry(any(Endpoints.class), any(StreamObserver.class));
        verify(observer, times(1)).onNext(any(TelemetryCommand.class));
    }

    @Test
    public void testOnTopicRouteDataFetchedFailure() throws ClientException {
        String topic = FAKE_TOPIC_0;
        List<MessageQueue> messageQueueList = new ArrayList<>();
        final apache.rocketmq.v2.Endpoints pbEndpoints = fakePbEndpoints0();
        MessageQueue mq = MessageQueue.newBuilder().setTopic(Resource.newBuilder().setName(topic))
            .setPermission(Permission.READ_WRITE)
            .addAcceptMessageTypes(MessageType.NORMAL)
            .setBroker(Broker.newBuilder().setName(FAKE_BROKER_NAME_0).setEndpoints(pbEndpoints))
            .setId(0).build();
        messageQueueList.add(mq);
        final TopicRouteData topicRouteData = new TopicRouteData(messageQueueList);
        final StreamObserver<TelemetryCommand> observer =
            (StreamObserver<TelemetryCommand>) Mockito.mock(StreamObserver.class);
        final ClientImpl client = createClient();
        doReturn(observer).when(client).telemetry(any(Endpoints.class), any(StreamObserver.class));
        doNothing().when(observer).onNext(any(TelemetryCommand.class));
        TelemetryCommand settingsCommand = TelemetryCommand.newBuilder().build();
        doReturn(settingsCommand).when(client).settingsCommand();
        try {
            client.onTopicRouteDataFetched(topic, topicRouteData);
            fail();
        } catch (Throwable t) {
            verify(client, times(1)).telemetry(any(Endpoints.class), any(StreamObserver.class));
            verify(observer, times(1)).onNext(any(TelemetryCommand.class));
        }
    }
}
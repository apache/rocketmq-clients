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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.Permission;
import apache.rocketmq.v2.Publishing;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.TelemetryCommand;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.java.impl.ClientManagerImpl;
import org.apache.rocketmq.client.java.impl.ClientManagerRegistry;
import org.apache.rocketmq.client.java.impl.TelemetrySession;
import org.apache.rocketmq.client.java.misc.ThreadFactoryImpl;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.rpc.InvocationContext;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProducerImplTest extends TestBase {
    @Mock
    private ClientManagerImpl clientManager;
    @Mock
    private StreamObserver<TelemetryCommand> telemetryRequestObserver;
    @SuppressWarnings("unused")
    @InjectMocks
    private ClientManagerRegistry clientManagerRegistry = ClientManagerRegistry.getInstance();

    private final ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
        .setEndpoints(FAKE_ACCESS_POINT).build();

    private final String[] str = {FAKE_TOPIC_0};
    private final Set<String> set = new HashSet<>(Arrays.asList(str));

    private final int messageMaxBodySize = 1024 * 1024 * 4;

    @InjectMocks
    private final ProducerImpl producer = new ProducerImpl(clientConfiguration, set, 1, null);

    @InjectMocks
    private final ProducerImpl producerWithoutTopicBinding = new ProducerImpl(clientConfiguration, new HashSet<>(), 1,
        null);

    private void start(ProducerImpl producer) throws ClientException {
        SettableFuture<InvocationContext<QueryRouteResponse>> future0 = SettableFuture.create();
        Status status = Status.newBuilder().setCode(Code.OK).build();
        List<MessageQueue> messageQueueList = new ArrayList<>();
        MessageQueue mq = MessageQueue.newBuilder().setTopic(Resource.newBuilder().setName(FAKE_TOPIC_0))
            .setPermission(Permission.READ_WRITE)
            .setBroker(Broker.newBuilder().setName(FAKE_BROKER_NAME_0).setEndpoints(fakePbEndpoints0()))
            .setId(0).build();
        messageQueueList.add(mq);
        QueryRouteResponse resp = QueryRouteResponse.newBuilder().setStatus(status)
            .addAllMessageQueues(messageQueueList).build();
        final InvocationContext<QueryRouteResponse> invocationContext =
            new InvocationContext<>(resp, fakeRpcContext());
        future0.set(invocationContext);
        when(clientManager.queryRoute(any(Endpoints.class), any(Metadata.class), any(QueryRouteRequest.class),
            any(Duration.class)))
            .thenReturn(future0);
        when(clientManager.telemetry(any(Endpoints.class), any(Metadata.class), any(Duration.class),
            any(TelemetrySession.class)))
            .thenReturn(telemetryRequestObserver);
        final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl(
            "TestScheduler"));
        when(clientManager.getScheduler()).thenReturn(scheduler);
        doNothing().when(telemetryRequestObserver).onNext(any(TelemetryCommand.class));

        Publishing publishing = Publishing.newBuilder().setMaxBodySize(messageMaxBodySize).build();
        Settings settings = Settings.newBuilder().setPublishing(publishing).build();
        final Service service = producer.startAsync();
        producer.getClientSettings().applySettingsCommand(settings);
        service.awaitRunning();
    }

    private void shutdown(ProducerImpl producer) {
        final Service clientManagerService = mock(Service.class);
        when(clientManager.stopAsync()).thenReturn(clientManagerService);
        doNothing().when(clientManagerService).awaitTerminated();
        producer.stopAsync().awaitTerminated();
    }

    @Test(expected = IllegalStateException.class)
    public void testSendWithoutStart() throws ClientException {
        final Message message = fakeMessage(FAKE_TOPIC_0);
        producer.send(message);
    }

    @Test
    public void testSendWithTopicBinding() throws ClientException, ExecutionException, InterruptedException {
        start(producer);
        verify(clientManager, times(1)).queryRoute(any(Endpoints.class), any(Metadata.class),
            any(QueryRouteRequest.class), any(Duration.class));
        verify(clientManager, times(1)).telemetry(any(Endpoints.class), any(Metadata.class),
            any(Duration.class), any(TelemetrySession.class));
        final Message message = fakeMessage(FAKE_TOPIC_0);
        final ListenableFuture<InvocationContext<SendMessageResponse>> future =
            okSendMessageResponseFutureWithSingleEntry();
        when(clientManager.sendMessage(any(Endpoints.class), any(Metadata.class), any(SendMessageRequest.class),
            any(Duration.class))).thenReturn(future);
        final SendMessageResponse response = future.get().getResp();
        assertEquals(1, response.getEntriesCount());
        final apache.rocketmq.v2.SendResultEntry receipt = response.getEntriesList().iterator().next();
        final SendReceipt sendReceipt = producer.send(message);
        assertEquals(receipt.getMessageId(), sendReceipt.getMessageId().toString());
        shutdown(producer);
    }

    @Test
    public void testSendWithoutTopicBinding() throws ClientException, ExecutionException, InterruptedException {
        start(producerWithoutTopicBinding);
        verify(clientManager, never()).queryRoute(any(Endpoints.class), any(Metadata.class),
            any(QueryRouteRequest.class), any(Duration.class));
        verify(clientManager, never()).telemetry(any(Endpoints.class), any(Metadata.class), any(Duration.class),
            any(TelemetrySession.class));
        final Message message = fakeMessage(FAKE_TOPIC_0);
        final ListenableFuture<InvocationContext<SendMessageResponse>> future =
            okSendMessageResponseFutureWithSingleEntry();
        when(clientManager.sendMessage(any(Endpoints.class), any(Metadata.class), any(SendMessageRequest.class),
            any(Duration.class))).thenReturn(future);
        final SendMessageResponse response = future.get().getResp();
        assertEquals(1, response.getEntriesCount());
        final SendReceipt sendReceipt = producerWithoutTopicBinding.send(message);
        verify(clientManager, times(1)).queryRoute(any(Endpoints.class), any(Metadata.class),
            any(QueryRouteRequest.class), any(Duration.class));
        verify(clientManager, times(1)).telemetry(any(Endpoints.class), any(Metadata.class),
            any(Duration.class), any(TelemetrySession.class));
        final apache.rocketmq.v2.SendResultEntry receipt = response.getEntriesList().iterator().next();
        assertEquals(receipt.getMessageId(), sendReceipt.getMessageId().toString());
        shutdown(producerWithoutTopicBinding);
    }

    @Test(expected = ClientException.class)
    public void testSendMessageWithFailure() throws ClientException {
        start(producer);
        verify(clientManager, times(1)).queryRoute(any(Endpoints.class), any(Metadata.class),
            any(QueryRouteRequest.class), any(Duration.class));
        verify(clientManager, times(1)).telemetry(any(Endpoints.class), any(Metadata.class), any(Duration.class),
            any(TelemetrySession.class));
        final ListenableFuture<InvocationContext<SendMessageResponse>> future = failureSendMessageResponseFuture();
        when(clientManager.sendMessage(any(Endpoints.class), any(Metadata.class), any(SendMessageRequest.class),
            any(Duration.class))).thenReturn(future);
        Message message0 = fakeMessage(FAKE_TOPIC_0);
        try {
            producer.send(message0);
        } finally {
            shutdown(producer);
        }
    }
}
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

package org.apache.rocketmq.client.java.impl.consumer;

import static org.mockito.ArgumentMatchers.any;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import com.google.common.util.concurrent.ListenableFuture;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.java.impl.ClientManager;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.rpc.RpcFuture;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerImplTest extends TestBase {
    private final Map<String, FilterExpression> subscriptionExpressions = createSubscriptionExpressions(FAKE_TOPIC_0);
    private final MessageListener messageListener = messageView -> ConsumeResult.SUCCESS;
    private final ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
        .setEndpoints(FAKE_ENDPOINTS).build();

    @Test
    public void testReceiveMessage() throws ExecutionException, InterruptedException {
        int maxCacheMessageCount = 8;
        int maxCacheMessageSizeInBytes = 1024;
        int consumptionThreadCount = 4;
        PushConsumerImpl pushConsumer = Mockito.spy(new PushConsumerImpl(clientConfiguration, FAKE_CONSUMER_GROUP_0,
            subscriptionExpressions, messageListener, maxCacheMessageCount, maxCacheMessageSizeInBytes,
            consumptionThreadCount));
        final ClientManager clientManager = Mockito.mock(ClientManager.class);
        Mockito.doReturn(clientManager).when(pushConsumer).getClientManager();
        int receivedMessageCount = 1;
        final RpcFuture<ReceiveMessageRequest, List<ReceiveMessageResponse>> future =
            okReceiveMessageResponsesFuture(FAKE_TOPIC_0, receivedMessageCount);
        future.get();
        Mockito.doReturn(future).when(clientManager).receiveMessage(any(Endpoints.class),
            any(ReceiveMessageRequest.class), any(Duration.class));
        final MessageQueueImpl mq = fakeMessageQueueImpl(FAKE_TOPIC_0);
        final ReceiveMessageRequest request = pushConsumer.wrapReceiveMessageRequest(1,
            mq, new FilterExpression(), Duration.ofSeconds(15));
        final ListenableFuture<ReceiveMessageResult> future0 =
            pushConsumer.receiveMessage(request, mq, Duration.ofSeconds(15));
        final ReceiveMessageResult receiveMessageResult = future0.get();
        Assert.assertEquals(receiveMessageResult.getMessageViews().size(), receivedMessageCount);
    }

    @Test
    public void testAckMessage() throws ExecutionException, InterruptedException {
        int maxCacheMessageCount = 8;
        int maxCacheMessageSizeInBytes = 1024;
        int consumptionThreadCount = 4;
        PushConsumerImpl pushConsumer = Mockito.spy(new PushConsumerImpl(clientConfiguration, FAKE_CONSUMER_GROUP_0,
            subscriptionExpressions, messageListener, maxCacheMessageCount, maxCacheMessageSizeInBytes,
            consumptionThreadCount));
        final ClientManager clientManager = Mockito.mock(ClientManager.class);
        Mockito.doReturn(clientManager).when(pushConsumer).getClientManager();
        final RpcFuture<AckMessageRequest, AckMessageResponse> future =
            okAckMessageResponseFuture();
        Mockito.doReturn(future).when(clientManager).ackMessage(any(Endpoints.class),
            any(AckMessageRequest.class), any(Duration.class));
        final MessageViewImpl messageView = fakeMessageViewImpl();
        final RpcFuture<AckMessageRequest, AckMessageResponse> future0 =
            pushConsumer.ackMessage(messageView);
        final AckMessageResponse rpcInvocation = future0.get();
        Assert.assertEquals(rpcInvocation, future.get());
    }

    @Test
    public void testChangeInvisibleDuration() throws ExecutionException, InterruptedException {
        int maxCacheMessageCount = 8;
        int maxCacheMessageSizeInBytes = 1024;
        int consumptionThreadCount = 4;
        PushConsumerImpl pushConsumer = Mockito.spy(new PushConsumerImpl(clientConfiguration, FAKE_CONSUMER_GROUP_0,
            subscriptionExpressions, messageListener, maxCacheMessageCount, maxCacheMessageSizeInBytes,
            consumptionThreadCount));
        final ClientManager clientManager = Mockito.mock(ClientManager.class);
        Mockito.doReturn(clientManager).when(pushConsumer).getClientManager();
        final RpcFuture<ChangeInvisibleDurationRequest, ChangeInvisibleDurationResponse> future =
            okChangeInvisibleDurationCtxFuture();
        Mockito.doReturn(future).when(clientManager).changeInvisibleDuration(any(Endpoints.class),
            any(ChangeInvisibleDurationRequest.class), any(Duration.class));
        final MessageViewImpl messageView = fakeMessageViewImpl();
        final RpcFuture<ChangeInvisibleDurationRequest, ChangeInvisibleDurationResponse> future0 =
            pushConsumer.changeInvisibleDuration(messageView, Duration.ofSeconds(15));
        final ChangeInvisibleDurationResponse response = future0.get();
        Assert.assertEquals(response, future.get());
    }

}
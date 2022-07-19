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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.Code;
import com.google.common.util.concurrent.ListenableFuture;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.exception.BadRequestException;
import org.apache.rocketmq.client.java.exception.ForbiddenException;
import org.apache.rocketmq.client.java.exception.InternalErrorException;
import org.apache.rocketmq.client.java.exception.NotFoundException;
import org.apache.rocketmq.client.java.exception.ProxyTimeoutException;
import org.apache.rocketmq.client.java.exception.TooManyRequestsException;
import org.apache.rocketmq.client.java.exception.UnauthorizedException;
import org.apache.rocketmq.client.java.exception.UnsupportedException;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.rpc.RpcInvocation;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SimpleConsumerImplTest extends TestBase {
    @InjectMocks
    private final ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
        .setEndpoints(FAKE_ACCESS_POINT).build();

    private final Duration awaitDuration = Duration.ofSeconds(3);
    private final Map<String, FilterExpression> subExpressions = createSubscriptionExpressions(FAKE_TOPIC_0);

    private SimpleConsumerImpl simpleConsumer;

    @Test(expected = IllegalStateException.class)
    public void testReceiveWithoutStart() throws ClientException {
        simpleConsumer = new SimpleConsumerImpl(clientConfiguration, FAKE_GROUP_0, awaitDuration, subExpressions);
        simpleConsumer.receive(1, Duration.ofSeconds(1));
    }

    @Test(expected = IllegalStateException.class)
    public void testAckWithoutStart() throws ClientException {
        simpleConsumer = new SimpleConsumerImpl(clientConfiguration, FAKE_GROUP_0, awaitDuration, subExpressions);
        simpleConsumer.ack(fakeMessageViewImpl());
    }

    @Test(expected = IllegalStateException.class)
    public void testSubscribeWithoutStart() throws ClientException {
        simpleConsumer = new SimpleConsumerImpl(clientConfiguration, FAKE_GROUP_0, awaitDuration, subExpressions);
        simpleConsumer.subscribe(FAKE_TOPIC_1, FilterExpression.SUB_ALL);
    }

    @Test(expected = IllegalStateException.class)
    public void testUnsubscribeWithoutStart() {
        simpleConsumer = new SimpleConsumerImpl(clientConfiguration, FAKE_GROUP_0, awaitDuration, subExpressions);
        simpleConsumer.unsubscribe(FAKE_TOPIC_0);
    }

    @Test
    public void testReceiveAsyncWithZeroMaxMessageNum() throws InterruptedException {
        simpleConsumer = Mockito.spy(new SimpleConsumerImpl(clientConfiguration, FAKE_GROUP_0, awaitDuration,
            subExpressions));
        when(simpleConsumer.isRunning()).thenReturn(true);
        final CompletableFuture<List<MessageView>> future = simpleConsumer.receiveAsync(0,
            Duration.ofSeconds(3));
        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testAckAsync() throws ExecutionException, InterruptedException {
        simpleConsumer = Mockito.spy(new SimpleConsumerImpl(clientConfiguration, FAKE_GROUP_0, awaitDuration,
            subExpressions));
        when(simpleConsumer.isRunning()).thenReturn(true);
        final MessageViewImpl messageView = fakeMessageViewImpl(false);
        {
            doReturn(okAckMessageResponseFuture()).when(simpleConsumer).ackMessage(messageView);
            final CompletableFuture<Void> future = simpleConsumer.ackAsync(messageView);
            future.get();
        }
        {
            doReturn(ackMessageResponseFuture(Code.BAD_REQUEST)).when(simpleConsumer).ackMessage(messageView);
            final CompletableFuture<Void> future = simpleConsumer.ackAsync(messageView);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof BadRequestException);
            }
        }
        {
            doReturn(ackMessageResponseFuture(Code.ILLEGAL_TOPIC)).when(simpleConsumer).ackMessage(messageView);
            final CompletableFuture<Void> future = simpleConsumer.ackAsync(messageView);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof BadRequestException);
            }
        }
        {
            final ListenableFuture<RpcInvocation<AckMessageResponse>> respFuture =
                ackMessageResponseFuture(Code.ILLEGAL_CONSUMER_GROUP);
            doReturn(respFuture).when(simpleConsumer).ackMessage(messageView);
            final CompletableFuture<Void> future = simpleConsumer.ackAsync(messageView);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof BadRequestException);
            }
        }
        {
            final ListenableFuture<RpcInvocation<AckMessageResponse>> respFuture =
                ackMessageResponseFuture(Code.INVALID_RECEIPT_HANDLE);
            doReturn(respFuture).when(simpleConsumer).ackMessage(messageView);
            final CompletableFuture<Void> future = simpleConsumer.ackAsync(messageView);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof BadRequestException);
            }
        }
        {
            doReturn(ackMessageResponseFuture(Code.CLIENT_ID_REQUIRED)).when(simpleConsumer).ackMessage(messageView);
            final CompletableFuture<Void> future = simpleConsumer.ackAsync(messageView);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof BadRequestException);
            }
        }
        {
            doReturn(ackMessageResponseFuture(Code.UNAUTHORIZED)).when(simpleConsumer).ackMessage(messageView);
            final CompletableFuture<Void> future = simpleConsumer.ackAsync(messageView);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof UnauthorizedException);
            }
        }
        {
            doReturn(ackMessageResponseFuture(Code.FORBIDDEN)).when(simpleConsumer).ackMessage(messageView);
            final CompletableFuture<Void> future = simpleConsumer.ackAsync(messageView);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof ForbiddenException);
            }
        }
        {
            doReturn(ackMessageResponseFuture(Code.NOT_FOUND)).when(simpleConsumer).ackMessage(messageView);
            final CompletableFuture<Void> future = simpleConsumer.ackAsync(messageView);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof NotFoundException);
            }
        }
        {
            doReturn(ackMessageResponseFuture(Code.TOPIC_NOT_FOUND)).when(simpleConsumer).ackMessage(messageView);
            final CompletableFuture<Void> future = simpleConsumer.ackAsync(messageView);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof NotFoundException);
            }
        }
        {
            doReturn(ackMessageResponseFuture(Code.TOO_MANY_REQUESTS)).when(simpleConsumer).ackMessage(messageView);
            final CompletableFuture<Void> future = simpleConsumer.ackAsync(messageView);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof TooManyRequestsException);
            }
        }
        {
            doReturn(ackMessageResponseFuture(Code.INTERNAL_ERROR)).when(simpleConsumer).ackMessage(messageView);
            final CompletableFuture<Void> future = simpleConsumer.ackAsync(messageView);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof InternalErrorException);
            }
        }
        {
            final ListenableFuture<RpcInvocation<AckMessageResponse>> respFuture =
                ackMessageResponseFuture(Code.INTERNAL_SERVER_ERROR);
            doReturn(respFuture).when(simpleConsumer).ackMessage(messageView);
            final CompletableFuture<Void> future = simpleConsumer.ackAsync(messageView);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof InternalErrorException);
            }
        }
        {
            doReturn(ackMessageResponseFuture(Code.PROXY_TIMEOUT)).when(simpleConsumer).ackMessage(messageView);
            final CompletableFuture<Void> future = simpleConsumer.ackAsync(messageView);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof ProxyTimeoutException);
            }
        }
        {
            doReturn(ackMessageResponseFuture(Code.UNSUPPORTED)).when(simpleConsumer).ackMessage(messageView);
            final CompletableFuture<Void> future = simpleConsumer.ackAsync(messageView);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof UnsupportedException);
            }
        }
    }

    @Test
    public void testChangeInvisibleDurationAsync() throws ExecutionException, InterruptedException {
        simpleConsumer = Mockito.spy(new SimpleConsumerImpl(clientConfiguration, FAKE_GROUP_0, awaitDuration,
            subExpressions));
        when(simpleConsumer.isRunning()).thenReturn(true);
        final MessageViewImpl messageView = fakeMessageViewImpl(false);
        final Duration duration = Duration.ofSeconds(3);
        {
            final ListenableFuture<RpcInvocation<ChangeInvisibleDurationResponse>> respFuture =
                okChangeInvisibleDurationCtxFuture();
            doReturn(respFuture).when(simpleConsumer).changeInvisibleDuration(messageView, duration);
            final CompletableFuture<Void> future = simpleConsumer.changeInvisibleDurationAsync(messageView,
                duration);
            future.get();
        }
        {
            final ListenableFuture<RpcInvocation<ChangeInvisibleDurationResponse>> respFuture =
                changInvisibleDurationCtxFuture(Code.BAD_REQUEST);
            doReturn(respFuture).when(simpleConsumer).changeInvisibleDuration(messageView, duration);
            final CompletableFuture<Void> future = simpleConsumer.changeInvisibleDurationAsync(messageView,
                duration);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof BadRequestException);
            }
        }
        {
            final ListenableFuture<RpcInvocation<ChangeInvisibleDurationResponse>> respFuture =
                changInvisibleDurationCtxFuture(Code.ILLEGAL_TOPIC);
            doReturn(respFuture).when(simpleConsumer).changeInvisibleDuration(messageView, duration);
            final CompletableFuture<Void> future = simpleConsumer.changeInvisibleDurationAsync(messageView,
                duration);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof BadRequestException);
            }
        }
        {
            final ListenableFuture<RpcInvocation<ChangeInvisibleDurationResponse>> respFuture =
                changInvisibleDurationCtxFuture(Code.ILLEGAL_CONSUMER_GROUP);
            doReturn(respFuture).when(simpleConsumer).changeInvisibleDuration(messageView, duration);
            final CompletableFuture<Void> future = simpleConsumer.changeInvisibleDurationAsync(messageView,
                duration);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof BadRequestException);
            }
        }
        {
            final ListenableFuture<RpcInvocation<ChangeInvisibleDurationResponse>> respFuture =
                changInvisibleDurationCtxFuture(Code.ILLEGAL_INVISIBLE_TIME);
            doReturn(respFuture).when(simpleConsumer).changeInvisibleDuration(messageView, duration);
            final CompletableFuture<Void> future = simpleConsumer.changeInvisibleDurationAsync(messageView,
                duration);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof BadRequestException);
            }
        }
        {
            final ListenableFuture<RpcInvocation<ChangeInvisibleDurationResponse>> respFuture =
                changInvisibleDurationCtxFuture(Code.INVALID_RECEIPT_HANDLE);
            doReturn(respFuture).when(simpleConsumer).changeInvisibleDuration(messageView, duration);
            final CompletableFuture<Void> future = simpleConsumer.changeInvisibleDurationAsync(messageView,
                duration);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof BadRequestException);
            }
        }
        {
            final ListenableFuture<RpcInvocation<ChangeInvisibleDurationResponse>> respFuture =
                changInvisibleDurationCtxFuture(Code.CLIENT_ID_REQUIRED);
            doReturn(respFuture).when(simpleConsumer).changeInvisibleDuration(messageView, duration);
            final CompletableFuture<Void> future = simpleConsumer.changeInvisibleDurationAsync(messageView,
                duration);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof BadRequestException);
            }
        }
        {
            final ListenableFuture<RpcInvocation<ChangeInvisibleDurationResponse>> respFuture =
                changInvisibleDurationCtxFuture(Code.UNAUTHORIZED);
            doReturn(respFuture).when(simpleConsumer).changeInvisibleDuration(messageView, duration);
            final CompletableFuture<Void> future = simpleConsumer.changeInvisibleDurationAsync(messageView,
                duration);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof UnauthorizedException);
            }
        }
        {
            final ListenableFuture<RpcInvocation<ChangeInvisibleDurationResponse>> respFuture =
                changInvisibleDurationCtxFuture(Code.NOT_FOUND);
            doReturn(respFuture).when(simpleConsumer).changeInvisibleDuration(messageView, duration);
            final CompletableFuture<Void> future = simpleConsumer.changeInvisibleDurationAsync(messageView,
                duration);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof NotFoundException);
            }
        }
        {
            final ListenableFuture<RpcInvocation<ChangeInvisibleDurationResponse>> respFuture =
                changInvisibleDurationCtxFuture(Code.TOPIC_NOT_FOUND);
            doReturn(respFuture).when(simpleConsumer).changeInvisibleDuration(messageView, duration);
            final CompletableFuture<Void> future = simpleConsumer.changeInvisibleDurationAsync(messageView,
                duration);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof NotFoundException);
            }
        }
        {
            final ListenableFuture<RpcInvocation<ChangeInvisibleDurationResponse>> respFuture =
                changInvisibleDurationCtxFuture(Code.TOO_MANY_REQUESTS);
            doReturn(respFuture).when(simpleConsumer).changeInvisibleDuration(messageView, duration);
            final CompletableFuture<Void> future = simpleConsumer.changeInvisibleDurationAsync(messageView,
                duration);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof TooManyRequestsException);
            }
        }
        {
            final ListenableFuture<RpcInvocation<ChangeInvisibleDurationResponse>> respFuture =
                changInvisibleDurationCtxFuture(Code.INTERNAL_ERROR);
            doReturn(respFuture).when(simpleConsumer).changeInvisibleDuration(messageView, duration);
            final CompletableFuture<Void> future = simpleConsumer.changeInvisibleDurationAsync(messageView,
                duration);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof InternalErrorException);
            }
        }
        {
            final ListenableFuture<RpcInvocation<ChangeInvisibleDurationResponse>> respFuture =
                changInvisibleDurationCtxFuture(Code.INTERNAL_SERVER_ERROR);
            doReturn(respFuture).when(simpleConsumer).changeInvisibleDuration(messageView, duration);
            final CompletableFuture<Void> future = simpleConsumer.changeInvisibleDurationAsync(messageView,
                duration);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof InternalErrorException);
            }
        }
        {
            final ListenableFuture<RpcInvocation<ChangeInvisibleDurationResponse>> respFuture =
                changInvisibleDurationCtxFuture(Code.PROXY_TIMEOUT);
            doReturn(respFuture).when(simpleConsumer).changeInvisibleDuration(messageView, duration);
            final CompletableFuture<Void> future = simpleConsumer.changeInvisibleDurationAsync(messageView,
                duration);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof ProxyTimeoutException);
            }
        }
        {
            final ListenableFuture<RpcInvocation<ChangeInvisibleDurationResponse>> respFuture =
                changInvisibleDurationCtxFuture(Code.UNSUPPORTED);
            doReturn(respFuture).when(simpleConsumer).changeInvisibleDuration(messageView, duration);
            final CompletableFuture<Void> future = simpleConsumer.changeInvisibleDurationAsync(messageView,
                duration);
            try {
                future.get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof UnsupportedException);
            }
        }
    }
}
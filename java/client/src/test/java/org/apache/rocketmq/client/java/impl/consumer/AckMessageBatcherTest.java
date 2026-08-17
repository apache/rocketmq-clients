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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.AckMessageResultEntry;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Status;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.rpc.RpcFuture;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AckMessageBatcherTest extends TestBase {
    @Mock
    private PushConsumerImpl consumer;

    @Test
    @SuppressWarnings("unchecked")
    public void testFlushWhenBatchSizeReached() throws Exception {
        final MessageViewImpl messageView0 = fakeMessageViewImpl();
        final MessageViewImpl messageView1 = fakeMessageViewImpl();
        final AckMessageResponse response = response(Code.OK, messageView0, messageView1);
        when(consumer.ackMessage(anyList())).thenReturn(new RpcFuture<>(fakeRpcContext(), null,
            Futures.immediateFuture(response)));
        final AckMessageBatcher batcher = new AckMessageBatcher(consumer, SCHEDULER, 2, Duration.ofDays(1));

        final ListenableFuture<AckMessageResponse> future0 = batcher.enqueue(messageView0);
        assertFalse(future0.isDone());
        final ListenableFuture<AckMessageResponse> future1 = batcher.enqueue(messageView1);

        assertEquals(Code.OK, future0.get(1, TimeUnit.SECONDS).getStatus().getCode());
        assertEquals(Code.OK, future1.get(1, TimeUnit.SECONDS).getStatus().getCode());
        final ArgumentCaptor<List<MessageViewImpl>> captor = ArgumentCaptor.forClass(List.class);
        verify(consumer).ackMessage(captor.capture());
        assertEquals(2, captor.getValue().size());
    }

    @Test
    public void testFlushWhenDelayReached() throws Exception {
        final MessageViewImpl messageView = fakeMessageViewImpl();
        when(consumer.ackMessage(anyList())).thenReturn(new RpcFuture<>(fakeRpcContext(), null,
            Futures.immediateFuture(response(Code.OK, messageView))));
        final AckMessageBatcher batcher = new AckMessageBatcher(consumer, SCHEDULER, 1024,
            Duration.ofMillis(20));

        final ListenableFuture<AckMessageResponse> future = batcher.enqueue(messageView);

        assertEquals(Code.OK, future.get(1, TimeUnit.SECONDS).getStatus().getCode());
        verify(consumer).ackMessage(anyList());
    }

    @Test
    public void testMapIndividualResultsByMessageId() throws Exception {
        final MessageViewImpl messageView0 = fakeMessageViewImpl();
        final MessageViewImpl messageView1 = fakeMessageViewImpl();
        final Status ok = Status.newBuilder().setCode(Code.OK).build();
        final Status invalidHandle = Status.newBuilder().setCode(Code.INVALID_RECEIPT_HANDLE).build();
        final AckMessageResponse response = AckMessageResponse.newBuilder()
            .setStatus(Status.newBuilder().setCode(Code.MULTIPLE_RESULTS))
            .addEntries(entry(messageView1, invalidHandle))
            .addEntries(entry(messageView0, ok))
            .build();
        when(consumer.ackMessage(anyList())).thenReturn(new RpcFuture<>(fakeRpcContext(), null,
            Futures.immediateFuture(response)));
        final AckMessageBatcher batcher = new AckMessageBatcher(consumer, SCHEDULER, 2, Duration.ofDays(1));

        final ListenableFuture<AckMessageResponse> future0 = batcher.enqueue(messageView0);
        final ListenableFuture<AckMessageResponse> future1 = batcher.enqueue(messageView1);

        assertEquals(Code.OK, future0.get().getStatus().getCode());
        assertEquals(Code.INVALID_RECEIPT_HANDLE, future1.get().getStatus().getCode());
    }

    @Test
    public void testMissingIndividualResultsAreRetriedByCaller() throws Exception {
        final MessageViewImpl messageView0 = fakeMessageViewImpl();
        final MessageViewImpl messageView1 = fakeMessageViewImpl();
        final AckMessageResponse response = response(Code.OK, messageView0);
        when(consumer.ackMessage(anyList())).thenReturn(new RpcFuture<>(fakeRpcContext(), null,
            Futures.immediateFuture(response)));
        final AckMessageBatcher batcher = new AckMessageBatcher(consumer, SCHEDULER, 2, Duration.ofDays(1));

        final ListenableFuture<AckMessageResponse> future0 = batcher.enqueue(messageView0);
        final ListenableFuture<AckMessageResponse> future1 = batcher.enqueue(messageView1);

        assertEquals(Code.OK, future0.get().getStatus().getCode());
        assertEquals(Code.INTERNAL_ERROR, future1.get().getStatus().getCode());
    }

    @Test
    public void testFlushAndClosePartitionsDifferentEndpoints() throws Exception {
        final MessageViewImpl messageView0 = fakeMessageViewImpl(fakeMessageQueueImpl0());
        final MessageViewImpl messageView1 = fakeMessageViewImpl(fakeMessageQueueImpl1());
        when(consumer.ackMessage(anyList())).thenAnswer(invocation -> {
            final List<MessageViewImpl> messages = invocation.getArgument(0);
            return new RpcFuture<AckMessageRequest, AckMessageResponse>(fakeRpcContext(), null,
                Futures.immediateFuture(response(Code.OK, messages.toArray(new MessageViewImpl[0]))));
        });
        final AckMessageBatcher batcher = new AckMessageBatcher(consumer, SCHEDULER, 1024, Duration.ofDays(1));

        batcher.enqueue(messageView0);
        batcher.enqueue(messageView1);
        batcher.flushAndClose().get(1, TimeUnit.SECONDS);

        verify(consumer, times(2)).ackMessage(anyList());
        assertNull(batcher.enqueue(fakeMessageViewImpl()));
    }

    @Test
    public void testFlushAndCloseWaitsForInflightBatch() throws Exception {
        final MessageViewImpl messageView = fakeMessageViewImpl();
        final SettableFuture<AckMessageResponse> rpcResponse = SettableFuture.create();
        when(consumer.ackMessage(anyList())).thenReturn(new RpcFuture<>(fakeRpcContext(), null, rpcResponse));
        final AckMessageBatcher batcher = new AckMessageBatcher(consumer, SCHEDULER, 1, Duration.ofDays(1));

        final ListenableFuture<AckMessageResponse> messageFuture = batcher.enqueue(messageView);
        final ListenableFuture<List<AckMessageResponse>> closeFuture = batcher.flushAndClose();
        assertFalse(closeFuture.isDone());
        rpcResponse.set(response(Code.OK, messageView));

        assertEquals(Code.OK, messageFuture.get(1, TimeUnit.SECONDS).getStatus().getCode());
        assertEquals(1, closeFuture.get(1, TimeUnit.SECONDS).size());
    }

    @Test
    public void testBatchOnlyRecentlyConsumedNonLiteMessages() {
        final MessageViewImpl freshMessage = fakeMessageViewImpl();
        final MessageViewImpl slowMessage = Mockito.spy(fakeMessageViewImpl());
        Mockito.doReturn(System.currentTimeMillis() - AckMessageBatcher.MAX_BATCHABLE_DURATION.toMillis() - 1)
            .when(slowMessage).getDecodeTimestamp();
        when(consumer.isLiteConsumer()).thenReturn(false);
        final AckMessageBatcher batcher = new AckMessageBatcher(consumer, SCHEDULER);

        assertTrue(batcher.isBatchable(freshMessage));
        assertFalse(batcher.isBatchable(slowMessage));
        when(consumer.isLiteConsumer()).thenReturn(true);
        assertFalse(batcher.isBatchable(freshMessage));
    }

    private AckMessageResponse response(Code code, MessageViewImpl... messageViews) {
        final Status status = Status.newBuilder().setCode(code).build();
        final AckMessageResponse.Builder builder = AckMessageResponse.newBuilder().setStatus(status);
        for (MessageViewImpl messageView : messageViews) {
            builder.addEntries(entry(messageView, status));
        }
        return builder.build();
    }

    private AckMessageResultEntry entry(MessageViewImpl messageView, Status status) {
        return AckMessageResultEntry.newBuilder()
            .setMessageId(messageView.getMessageId().toString())
            .setReceiptHandle(messageView.getReceiptHandle())
            .setStatus(status)
            .build();
    }
}

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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.client.apis.consumer.AsyncMessageListener;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.java.hook.MessageHookPointsStatus;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.hook.MessageInterceptorContext;
import org.apache.rocketmq.client.java.message.GeneralMessage;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class AsyncConsumeTaskTest extends TestBase {
    private final MessageViewImpl messageView = fakeMessageViewImpl();
    private final MessageInterceptor interceptor = Mockito.mock(MessageInterceptor.class);

    @Test
    public void testInterceptorsFollowActualCompletion() throws Exception {
        final CompletableFuture<ConsumeResult> completion = new CompletableFuture<>();
        final AtomicBoolean afterCompleted = new AtomicBoolean();
        final AtomicBoolean futureObservedAfter = new AtomicBoolean();
        Mockito.doAnswer(invocation -> {
            afterCompleted.set(true);
            return null;
        }).when(interceptor).doAfter(any(), anyList());
        final ListenableFuture<ConsumeResult> result = task(message -> completion).invoke();
        result.addListener(() -> futureObservedAfter.set(afterCompleted.get()), Runnable::run);

        verify(interceptor).doBefore(any(), anyList());
        verify(interceptor, never()).doAfter(any(), anyList());
        assertFalse(result.isDone());

        completion.complete(ConsumeResult.SUCCESS);
        assertEquals(ConsumeResult.SUCCESS, result.get(5, TimeUnit.SECONDS));
        assertTrue(futureObservedAfter.get());
        final ArgumentCaptor<MessageInterceptorContext> context =
            ArgumentCaptor.forClass(MessageInterceptorContext.class);
        verify(interceptor).doAfter(context.capture(), anyList());
        assertEquals(MessageHookPointsStatus.OK, context.getValue().getStatus());
    }

    @Test
    public void testFailureResult() throws Exception {
        assertFailure(message -> CompletableFuture.completedFuture(ConsumeResult.FAILURE));
    }

    @Test
    public void testNullStageIsFailure() throws Exception {
        assertFailure(message -> null);
    }

    @Test
    public void testNullResultIsFailure() throws Exception {
        assertFailure(message -> CompletableFuture.completedFuture(null));
    }

    @Test
    public void testThrownExceptionIsFailure() throws Exception {
        assertFailure(message -> {
            throw new IllegalStateException("Listener failed before returning a stage");
        });
    }

    @Test
    public void testExceptionalCompletionIsFailure() throws Exception {
        final CompletableFuture<ConsumeResult> completion = new CompletableFuture<>();
        final ListenableFuture<ConsumeResult> result = task(message -> completion).invoke();
        final IllegalStateException failure = new IllegalStateException("Asynchronous processing failed");
        completion.completeExceptionally(failure);

        assertEquals(ConsumeResult.FAILURE, result.get(5, TimeUnit.SECONDS));
        final ArgumentCaptor<MessageInterceptorContext> context =
            ArgumentCaptor.forClass(MessageInterceptorContext.class);
        verify(interceptor).doAfter(context.capture(), anyList());
        assertEquals(MessageHookPointsStatus.ERROR, context.getValue().getStatus());
        assertSame(failure, context.getValue().getAttribute(ConsumeTask.CONSUME_ERROR_CONTEXT_KEY).get());
    }

    @Test
    public void testCancelledStageIsFailure() throws Exception {
        final CompletableFuture<ConsumeResult> completion = new CompletableFuture<>();
        final ListenableFuture<ConsumeResult> result = task(message -> completion).invoke();
        completion.cancel(false);

        assertEquals(ConsumeResult.FAILURE, result.get(5, TimeUnit.SECONDS));
        verify(interceptor).doAfter(any(), anyList());
    }

    @Test
    public void testBeforeInterceptorFailureDoesNotInvokeListener() throws Exception {
        final AsyncMessageListener listener = Mockito.mock(AsyncMessageListener.class);
        Mockito.doThrow(new IllegalStateException("Before interceptor failed"))
            .when(interceptor).doBefore(any(), anyList());

        assertEquals(ConsumeResult.FAILURE, task(listener).invoke().get(5, TimeUnit.SECONDS));
        verify(listener, never()).consumeAsync(any());
        verify(interceptor).doAfter(any(), anyList());
    }

    @Test
    public void testAfterInterceptorFailureCompletesAsFailure() throws Exception {
        Mockito.doThrow(new IllegalStateException("After interceptor failed"))
            .when(interceptor).doAfter(any(), anyList());

        assertEquals(ConsumeResult.FAILURE,
            task(message -> CompletableFuture.completedFuture(ConsumeResult.SUCCESS)).invoke()
                .get(5, TimeUnit.SECONDS));
    }

    @Test
    public void testConsumerGroupAndMessageRemainInCompletionContext() throws Exception {
        final CompletableFuture<ConsumeResult> completion = new CompletableFuture<>();
        final AtomicReference<MessageInterceptorContext> contextAfter = new AtomicReference<>();
        final MessageInterceptor capturingInterceptor = new MessageInterceptor() {
            @Override
            public void doBefore(MessageInterceptorContext context, List<GeneralMessage> messages) {
                assertEquals(FAKE_CONSUMER_GROUP_0,
                    context.getAttribute(ConsumeTask.CONSUMER_GROUP_CONTEXT_KEY).get());
                assertSame(messageView, context.getAttribute(ConsumeTask.MESSAGE_VIEW_CONTEXT_KEY).get());
            }

            @Override
            public void doAfter(MessageInterceptorContext context, List<GeneralMessage> messages) {
                contextAfter.set(context);
            }
        };
        final AsyncConsumeTask task = new AsyncConsumeTask(new ClientId(), FAKE_CONSUMER_GROUP_0,
            message -> completion, messageView, capturingInterceptor);
        final ListenableFuture<ConsumeResult> result = task.invoke();
        completion.complete(ConsumeResult.SUCCESS);
        result.get(5, TimeUnit.SECONDS);

        assertEquals(FAKE_CONSUMER_GROUP_0,
            contextAfter.get().getAttribute(ConsumeTask.CONSUMER_GROUP_CONTEXT_KEY).get());
        assertSame(messageView, contextAfter.get().getAttribute(ConsumeTask.MESSAGE_VIEW_CONTEXT_KEY).get());
    }

    private AsyncConsumeTask task(AsyncMessageListener listener) {
        return new AsyncConsumeTask(new ClientId(), FAKE_CONSUMER_GROUP_0, listener, messageView, interceptor);
    }

    private void assertFailure(AsyncMessageListener listener) throws Exception {
        assertEquals(ConsumeResult.FAILURE, task(listener).invoke().get(5, TimeUnit.SECONDS));
        final ArgumentCaptor<MessageInterceptorContext> context =
            ArgumentCaptor.forClass(MessageInterceptorContext.class);
        verify(interceptor).doAfter(context.capture(), anyList());
        assertEquals(MessageHookPointsStatus.ERROR, context.getValue().getStatus());
    }
}

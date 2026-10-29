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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.client.apis.consumer.AsyncMessageListener;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.java.hook.Attribute;
import org.apache.rocketmq.client.java.hook.MessageHookPoints;
import org.apache.rocketmq.client.java.hook.MessageHookPointsStatus;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.hook.MessageInterceptorContextImpl;
import org.apache.rocketmq.client.java.message.GeneralMessage;
import org.apache.rocketmq.client.java.message.GeneralMessageImpl;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.rpc.LoggingInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Invokes an asynchronous listener without waiting on its completion stage.
 */
class AsyncConsumeTask {
    private static final Logger log = LoggerFactory.getLogger(AsyncConsumeTask.class);

    private final ClientId clientId;
    private final String consumerGroup;
    private final AsyncMessageListener messageListener;
    private final MessageViewImpl messageView;
    private final MessageInterceptor messageInterceptor;

    AsyncConsumeTask(ClientId clientId, String consumerGroup, AsyncMessageListener messageListener,
        MessageViewImpl messageView, MessageInterceptor messageInterceptor) {
        this.clientId = clientId;
        this.consumerGroup = consumerGroup;
        this.messageListener = messageListener;
        this.messageView = messageView;
        this.messageInterceptor = messageInterceptor;
    }

    ListenableFuture<ConsumeResult> invoke() {
        SettableFuture<ConsumeResult> future = SettableFuture.create();
        AtomicBoolean completed = new AtomicBoolean();
        List<GeneralMessage> messages = Collections.singletonList(new GeneralMessageImpl(messageView));
        MessageInterceptorContextImpl context = new MessageInterceptorContextImpl(MessageHookPoints.CONSUME);
        context.putAttribute(ConsumeTask.REMOTE_ADDR_CONTEXT_KEY,
            Attribute.create(LoggingInterceptor.getInstance().getRemoteAddr()));
        context.putAttribute(ConsumeTask.MESSAGE_VIEW_CONTEXT_KEY, Attribute.create(messageView));
        context.putAttribute(ConsumeTask.CONSUMER_GROUP_CONTEXT_KEY, Attribute.create(consumerGroup));
        try {
            messageInterceptor.doBefore(context, messages);
            CompletionStage<ConsumeResult> stage = messageListener.consumeAsync(messageView);
            if (null == stage) {
                finish(null, new NullPointerException("Async message listener returned a null stage"),
                    context, messages, completed, future);
            } else {
                stage.whenComplete((result, error) -> finish(result, error, context, messages, completed, future));
            }
        } catch (Throwable error) {
            finish(null, error, context, messages, completed, future);
        }
        return future;
    }

    private void finish(ConsumeResult result, Throwable error, MessageInterceptorContextImpl beforeContext,
        List<GeneralMessage> messages, AtomicBoolean completed, SettableFuture<ConsumeResult> future) {
        if (!completed.compareAndSet(false, true)) {
            return;
        }
        ConsumeResult normalized = null == error && null != result ? result : ConsumeResult.FAILURE;
        MessageHookPointsStatus status = ConsumeResult.SUCCESS.equals(normalized) ? MessageHookPointsStatus.OK :
            MessageHookPointsStatus.ERROR;
        MessageInterceptorContextImpl context = new MessageInterceptorContextImpl(beforeContext, status);
        if (null != error) {
            context.putAttribute(ConsumeTask.CONSUME_ERROR_CONTEXT_KEY, Attribute.create(error));
            log.error("Async message listener failed, clientId={}, consumerGroup={}, messageId={}",
                clientId, consumerGroup, messageView.getMessageId(), error);
        }
        try {
            messageInterceptor.doAfter(context, messages);
        } catch (Throwable hookError) {
            log.error("Consumption interceptor failed, clientId={}, messageId={}",
                clientId, messageView.getMessageId(), hookError);
            normalized = ConsumeResult.FAILURE;
        } finally {
            future.set(normalized);
        }
    }
}

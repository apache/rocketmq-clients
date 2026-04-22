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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.rocketmq.client.apis.consumer.BatchMessageListener;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.hook.Attribute;
import org.apache.rocketmq.client.java.hook.AttributeKey;
import org.apache.rocketmq.client.java.hook.MessageHookPoints;
import org.apache.rocketmq.client.java.hook.MessageHookPointsStatus;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.hook.MessageInterceptorContextImpl;
import org.apache.rocketmq.client.java.message.GeneralMessage;
import org.apache.rocketmq.client.java.message.GeneralMessageImpl;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Callable} that invokes the {@link BatchMessageListener} with a batch of messages and
 * returns a single {@link ConsumeResult} for the entire batch.
 *
 * <p>This is the batch counterpart of {@link ConsumeTask}.
 */
public class BatchConsumeTask implements Callable<ConsumeResult> {
    static final AttributeKey<Integer> BATCH_SIZE_CONTEXT_KEY = AttributeKey.create("batchSize");
    static final AttributeKey<Throwable> CONSUME_ERROR_CONTEXT_KEY = AttributeKey.create("consumeError");

    private static final Logger log = LoggerFactory.getLogger(BatchConsumeTask.class);

    private final ClientId clientId;
    private final BatchMessageListener batchMessageListener;
    private final List<MessageViewImpl> messageViews;
    private final MessageInterceptor messageInterceptor;

    public BatchConsumeTask(ClientId clientId, BatchMessageListener batchMessageListener,
        List<MessageViewImpl> messageViews, MessageInterceptor messageInterceptor) {
        this.clientId = clientId;
        this.batchMessageListener = batchMessageListener;
        this.messageViews = messageViews;
        this.messageInterceptor = messageInterceptor;
    }

    /**
     * Invoke {@link BatchMessageListener} to consume a batch of messages.
     *
     * @return the consume result for the entire batch.
     */
    @Override
    public ConsumeResult call() {
        ConsumeResult consumeResult;
        final List<GeneralMessage> generalMessages = new ArrayList<>(messageViews.size());
        for (MessageViewImpl mv : messageViews) {
            generalMessages.add(new GeneralMessageImpl(mv));
        }

        MessageInterceptorContextImpl context = new MessageInterceptorContextImpl(MessageHookPoints.CONSUME);
        context.putAttribute(BATCH_SIZE_CONTEXT_KEY, Attribute.create(messageViews.size()));

        messageInterceptor.doBefore(context, generalMessages);
        Throwable throwable = null;
        try {
            final List<MessageView> views = new ArrayList<>(messageViews);
            consumeResult = batchMessageListener.consume(views);
        } catch (Throwable t) {
            log.error("Batch message listener raised an exception while consuming messages, clientId={}, "
                + "batchSize={}", clientId, messageViews.size(), t);
            consumeResult = ConsumeResult.FAILURE;
            throwable = t;
        }
        // Treat null return as failure.
        if (consumeResult == null) {
            consumeResult = ConsumeResult.FAILURE;
        }
        MessageHookPointsStatus status = ConsumeResult.SUCCESS.equals(consumeResult)
            ? MessageHookPointsStatus.OK : MessageHookPointsStatus.ERROR;
        context = new MessageInterceptorContextImpl(context, status);
        if (!ConsumeResult.SUCCESS.equals(consumeResult) && null != throwable) {
            context.putAttribute(CONSUME_ERROR_CONTEXT_KEY, Attribute.create(throwable));
        }
        messageInterceptor.doAfter(context, generalMessages);
        return consumeResult;
    }
}

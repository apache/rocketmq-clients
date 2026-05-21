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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.apis.consumer.BatchMessageListener;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.message.MessageView;
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
 * A callable task that invokes {@link BatchMessageListener#consume(List)} with a batch of messages.
 *
 * <p>This is analogous to {@link ConsumeTask} but operates on a list of messages instead of a single one.
 * If the listener throws an exception or returns null, the result is treated as {@link ConsumeResult#FAILURE}.
 */
public class BatchConsumeTask implements Callable<ConsumeResult> {
    private static final Logger log = LoggerFactory.getLogger(BatchConsumeTask.class);

    private final ClientId clientId;
    private final BatchMessageListener batchMessageListener;
    private final List<MessageViewImpl> messageViews;
    private final MessageInterceptor messageInterceptor;

    /**
     * Creates a new batch consume task.
     *
     * @param clientId             the client identifier for logging.
     * @param batchMessageListener the batch message listener to invoke.
     * @param messageViews         the batch of messages to consume.
     * @param messageInterceptor   the message interceptor for hook points.
     */
    public BatchConsumeTask(ClientId clientId, BatchMessageListener batchMessageListener,
        List<MessageViewImpl> messageViews, MessageInterceptor messageInterceptor) {
        this.clientId = clientId;
        this.batchMessageListener = batchMessageListener;
        this.messageViews = messageViews;
        this.messageInterceptor = messageInterceptor;
    }

    /**
     * Invokes the {@link BatchMessageListener} to consume the batch of messages.
     *
     * @return the consume result for the entire batch.
     */
    @Override
    public ConsumeResult call() {
        ConsumeResult consumeResult;
        final List<GeneralMessage> generalMessages = messageViews.stream()
            .map(msg -> (GeneralMessage) new GeneralMessageImpl(msg))
            .collect(Collectors.toList());
        MessageInterceptorContextImpl context = new MessageInterceptorContextImpl(MessageHookPoints.CONSUME);
        messageInterceptor.doBefore(context, generalMessages);
        try {
            final List<MessageView> views = Collections.unmodifiableList(
                messageViews.stream().map(msg -> (MessageView) msg).collect(Collectors.toList()));
            consumeResult = batchMessageListener.consume(views);
        } catch (Throwable t) {
            log.error("Batch message listener raised an exception while consuming messages, clientId={}, "
                + "batchSize={}", clientId, messageViews.size(), t);
            consumeResult = ConsumeResult.FAILURE;
        }
        if (consumeResult == null) {
            log.warn("Batch message listener returned null, treating as FAILURE, clientId={}, batchSize={}",
                clientId, messageViews.size());
            consumeResult = ConsumeResult.FAILURE;
        }
        MessageHookPointsStatus status = ConsumeResult.SUCCESS.equals(consumeResult) ? MessageHookPointsStatus.OK :
            MessageHookPointsStatus.ERROR;
        context = new MessageInterceptorContextImpl(context, status);
        messageInterceptor.doAfter(context, generalMessages);
        return consumeResult;
    }
}

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
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.java.hook.MessageHandler;
import org.apache.rocketmq.client.java.hook.MessageHandlerContextImpl;
import org.apache.rocketmq.client.java.hook.MessageHookPoints;
import org.apache.rocketmq.client.java.hook.MessageHookPointsStatus;
import org.apache.rocketmq.client.java.message.GeneralMessage;
import org.apache.rocketmq.client.java.message.GeneralMessageImpl;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumeTask implements Callable<ConsumeResult> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumeTask.class);

    private final String clientId;
    private final MessageListener messageListener;
    private final MessageViewImpl messageView;
    private final MessageHandler messageHandler;

    public ConsumeTask(String clientId, MessageListener messageListener, MessageViewImpl messageView,
        MessageHandler messageHandler) {
        this.clientId = clientId;
        this.messageListener = messageListener;
        this.messageView = messageView;
        this.messageHandler = messageHandler;
    }

    /**
     * Invoke {@link MessageListener} to consumer message.
     *
     * @return message(s) which is consumed successfully.
     */
    @Override
    public ConsumeResult call() {
        ConsumeResult consumeResult;
        final List<GeneralMessage> generalMessages = Collections.singletonList(new GeneralMessageImpl(messageView));
        MessageHandlerContextImpl context = new MessageHandlerContextImpl(MessageHookPoints.CONSUME);
        messageHandler.doBefore(context, generalMessages);
        try {
            consumeResult = messageListener.consume(messageView);
        } catch (Throwable t) {
            LOGGER.error("Message listener raised an exception while consuming messages, clientId={}", clientId, t);
            // If exception was thrown during the period of message consumption, mark it as failure.
            consumeResult = ConsumeResult.FAILURE;
        }
        MessageHookPointsStatus status = ConsumeResult.SUCCESS.equals(consumeResult) ? MessageHookPointsStatus.OK :
            MessageHookPointsStatus.ERROR;
        context = new MessageHandlerContextImpl(context, status);
        messageHandler.doAfter(context, generalMessages);
        // Make sure that the return value is the subset of messageViews.
        return consumeResult;
    }
}

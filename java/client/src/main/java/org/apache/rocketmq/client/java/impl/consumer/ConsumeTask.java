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

public class ConsumeTask implements Callable<ConsumeResult> {
    private static final Logger log = LoggerFactory.getLogger(ConsumeTask.class);

    private final ClientId clientId;
    private final MessageListener messageListener;
    private final MessageViewImpl messageView;
    private final MessageInterceptor messageInterceptor;

    public ConsumeTask(ClientId clientId, MessageListener messageListener, MessageViewImpl messageView,
        MessageInterceptor messageInterceptor) {
        this.clientId = clientId;
        this.messageListener = messageListener;
        this.messageView = messageView;
        this.messageInterceptor = messageInterceptor;
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
        MessageInterceptorContextImpl context = new MessageInterceptorContextImpl(MessageHookPoints.CONSUME);
        messageInterceptor.doBefore(context, generalMessages);
        try {
            consumeResult = messageListener.consume(messageView);
        } catch (Throwable t) {
            log.error("Message listener raised an exception while consuming messages, clientId={}", clientId, t);
            // If exception was thrown during the period of message consumption, mark it as failure.
            consumeResult = ConsumeResult.FAILURE;
        }
        MessageHookPointsStatus status = ConsumeResult.SUCCESS.equals(consumeResult) ? MessageHookPointsStatus.OK :
            MessageHookPointsStatus.ERROR;
        context = new MessageInterceptorContextImpl(context, status);
        messageInterceptor.doAfter(context, generalMessages);
        // Make sure that the return value is the subset of messageViews.
        return consumeResult;
    }
}

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

package org.apache.rocketmq.client.impl.consumer;

import com.google.common.base.Stopwatch;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.ConsumeContext;
import org.apache.rocketmq.client.consumer.ConsumeStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageHookPoint;
import org.apache.rocketmq.client.message.MessageHookPointStatus;
import org.apache.rocketmq.client.message.MessageInterceptor;
import org.apache.rocketmq.client.message.MessageInterceptorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumeTask implements Callable<ConsumeStatus> {
    private static final Logger log = LoggerFactory.getLogger(ConsumeTask.class);

    private final MessageInterceptor interceptor;
    private final MessageListener messageListener;
    private final List<MessageExt> messageExtList;

    public ConsumeTask(MessageInterceptor interceptor, MessageListener messageListener,
                       List<MessageExt> messageExtList) {
        this.interceptor = interceptor;
        this.messageListener = messageListener;
        this.messageExtList = messageExtList;
    }

    @Override
    public ConsumeStatus call() {
        // intercept before message consumption.
        for (MessageExt messageExt : messageExtList) {
            final MessageInterceptorContext context =
                    MessageInterceptorContext.builder().setAttempt(messageExt.getDeliveryAttempt()).build();
            interceptor.intercept(MessageHookPoint.PRE_MESSAGE_CONSUMPTION, messageExt, context);
        }

        ConsumeStatus status;

        final ConsumeContext consumeContext = new ConsumeContext();
        final Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            status = messageListener.consume(messageExtList, consumeContext);
        } catch (Throwable t) {
            status = ConsumeStatus.ERROR;
            log.error("Message listener raised an exception while consuming messages.", t);
        }
        if (null == status) {
            log.error("Message listener returns NPE for consume status");
            status = ConsumeStatus.ERROR;
        }

        // intercept after message consumption.
        final TimeUnit timeUnit = MessageInterceptor.DEFAULT_TIME_UNIT;
        final long duration = stopwatch.elapsed(timeUnit);
        final MessageHookPointStatus pointStatus = ConsumeStatus.OK.equals(status) ? MessageHookPointStatus.OK :
                                                   MessageHookPointStatus.ERROR;
        final int batchSize = messageExtList.size();
        for (MessageExt messageExt : messageExtList) {
            final int attempt = messageExt.getDeliveryAttempt();
            final MessageInterceptorContext context = MessageInterceptorContext.builder()
                                                                               .setAttempt(attempt)
                                                                               .setDuration(duration)
                                                                               .setTimeUnit(timeUnit)
                                                                               .setBatchSize(batchSize)
                                                                               .setStatus(pointStatus).build();
            interceptor.intercept(MessageHookPoint.POST_MESSAGE_CONSUMPTION, messageExt, context);
        }
        return status;
    }
}

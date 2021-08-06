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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.conf.TestBase;
import org.apache.rocketmq.client.consumer.ConsumeContext;
import org.apache.rocketmq.client.consumer.ConsumeStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageHookPoint;
import org.apache.rocketmq.client.message.MessageInterceptor;
import org.apache.rocketmq.client.message.MessageInterceptorContext;
import org.mockito.ArgumentMatchers;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConsumeTaskTest extends TestBase {
    private MessageInterceptor mockedInterceptor;
    private MessageListener mockedListener;
    private ConsumeTask consumerTask;

    @BeforeMethod
    public void beforeMethod() {
        this.mockedInterceptor = mock(MessageInterceptor.class);
        this.mockedListener = mock(MessageListener.class);

        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        messageExtList.add(getDummyMessageExt(1));

        this.consumerTask = new ConsumeTask(mockedInterceptor, mockedListener, messageExtList);
    }

    @Test
    public void testCallWithConsumptionOk() {
        final ConsumeStatus status = ConsumeStatus.OK;
        when(mockedListener.consume(ArgumentMatchers.<MessageExt>anyList(), ArgumentMatchers.<ConsumeContext>any()))
                .thenReturn(status);
        assertEquals(status, consumerTask.call());
        verify(mockedInterceptor, times(1)).intercept(eq(MessageHookPoint.PRE_MESSAGE_CONSUMPTION),
                                                      ArgumentMatchers.<MessageExt>any(),
                                                      ArgumentMatchers.<MessageInterceptorContext>any());
        verify(mockedInterceptor, times(1)).intercept(eq(MessageHookPoint.POST_MESSAGE_CONSUMPTION),
                                                      ArgumentMatchers.<MessageExt>any(),
                                                      ArgumentMatchers.<MessageInterceptorContext>any());
    }

    @Test
    public void testCallWithConsumptionError() {
        final ConsumeStatus status = ConsumeStatus.ERROR;
        when(mockedListener.consume(ArgumentMatchers.<MessageExt>anyList(), ArgumentMatchers.<ConsumeContext>any()))
                .thenReturn(status);
        assertEquals(status, consumerTask.call());
        verify(mockedInterceptor, times(1)).intercept(eq(MessageHookPoint.PRE_MESSAGE_CONSUMPTION),
                                                      ArgumentMatchers.<MessageExt>any(),
                                                      ArgumentMatchers.<MessageInterceptorContext>any());
        verify(mockedInterceptor, times(1)).intercept(eq(MessageHookPoint.POST_MESSAGE_CONSUMPTION),
                                                      ArgumentMatchers.<MessageExt>any(),
                                                      ArgumentMatchers.<MessageInterceptorContext>any());
    }

    @Test
    public void testCallWithConsumptionWithThrowable() {
        when(mockedListener.consume(ArgumentMatchers.<MessageExt>anyList(), ArgumentMatchers.<ConsumeContext>any()))
                .thenThrow(new RuntimeException());
        assertEquals(ConsumeStatus.ERROR, consumerTask.call());
        verify(mockedInterceptor, times(1)).intercept(eq(MessageHookPoint.PRE_MESSAGE_CONSUMPTION),
                                                      ArgumentMatchers.<MessageExt>any(),
                                                      ArgumentMatchers.<MessageInterceptorContext>any());
        verify(mockedInterceptor, times(1)).intercept(eq(MessageHookPoint.POST_MESSAGE_CONSUMPTION),
                                                      ArgumentMatchers.<MessageExt>any(),
                                                      ArgumentMatchers.<MessageInterceptorContext>any());
    }
}
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

import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;
import org.mockito.Mockito;

public class ConsumeTaskTest extends TestBase {

    @Test
    public void testCallWithConsumeSuccess() {
        ClientId clientId = new ClientId();
        final MessageViewImpl messageView = fakeMessageViewImpl();
        final MessageListener messageListener = Mockito.mock(MessageListener.class);
        Mockito.when(messageListener.consume(messageView)).thenReturn(ConsumeResult.SUCCESS);
        final MessageInterceptor messageInterceptor = Mockito.mock(MessageInterceptor.class);
        final ConsumeTask consumeTask = new ConsumeTask(clientId, messageListener, messageView, messageInterceptor);
        final ConsumeResult consumeResult = consumeTask.call();
        assertEquals(ConsumeResult.SUCCESS, consumeResult);
    }

    @Test
    public void testCallWithConsumeException() {
        ClientId clientId = new ClientId();
        final MessageViewImpl messageView = fakeMessageViewImpl();
        final MessageListener messageListener = Mockito.mock(MessageListener.class);
        Mockito.when(messageListener.consume(messageView)).thenThrow(new RuntimeException());
        final MessageInterceptor messageInterceptor = Mockito.mock(MessageInterceptor.class);
        final ConsumeTask consumeTask = new ConsumeTask(clientId, messageListener, messageView, messageInterceptor);
        final ConsumeResult consumeResult = consumeTask.call();
        assertEquals(ConsumeResult.FAILURE, consumeResult);
    }
}
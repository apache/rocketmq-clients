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

import java.util.Arrays;
import java.util.List;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;
import org.mockito.Mockito;

public class BatchConsumeTaskTest extends TestBase {
    private final ClientId clientId = new ClientId();
    private final MessageInterceptor interceptor = Mockito.mock(MessageInterceptor.class);

    @Test
    public void testConsumeSuccess() {
        List<MessageViewImpl> views = Arrays.asList(fakeMessageViewImpl(), fakeMessageViewImpl());
        BatchConsumeTask task = new BatchConsumeTask(clientId, messageViews -> ConsumeResult.SUCCESS,
            views, interceptor);
        assertEquals(ConsumeResult.SUCCESS, task.call());
    }

    @Test
    public void testConsumeFailure() {
        List<MessageViewImpl> views = Arrays.asList(fakeMessageViewImpl(), fakeMessageViewImpl());
        BatchConsumeTask task = new BatchConsumeTask(clientId, messageViews -> ConsumeResult.FAILURE,
            views, interceptor);
        assertEquals(ConsumeResult.FAILURE, task.call());
    }

    @Test
    public void testConsumeWithException() {
        List<MessageViewImpl> views = Arrays.asList(fakeMessageViewImpl(), fakeMessageViewImpl());
        BatchConsumeTask task = new BatchConsumeTask(clientId, messageViews -> {
            throw new RuntimeException("test exception");
        }, views, interceptor);
        assertEquals(ConsumeResult.FAILURE, task.call());
    }
}

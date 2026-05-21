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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import org.apache.rocketmq.client.apis.consumer.BatchMessageListener;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;

public class BatchConsumeTaskTest extends TestBase {

    @Test
    public void testCallWithSuccess() {
        ClientId clientId = new ClientId();
        List<MessageViewImpl> messageViews = Arrays.asList(fakeMessageViewImpl(), fakeMessageViewImpl());
        BatchMessageListener listener = mock(BatchMessageListener.class);
        when(listener.consume(any())).thenReturn(ConsumeResult.SUCCESS);
        MessageInterceptor interceptor = mock(MessageInterceptor.class);

        BatchConsumeTask task = new BatchConsumeTask(clientId, listener, messageViews, interceptor);
        ConsumeResult result = task.call();

        assertEquals(ConsumeResult.SUCCESS, result);
        verify(listener, times(1)).consume(any());
        verify(interceptor, times(1)).doBefore(any(), any());
        verify(interceptor, times(1)).doAfter(any(), any());
    }

    @Test
    public void testCallWithFailure() {
        ClientId clientId = new ClientId();
        List<MessageViewImpl> messageViews = Arrays.asList(fakeMessageViewImpl());
        BatchMessageListener listener = mock(BatchMessageListener.class);
        when(listener.consume(any())).thenReturn(ConsumeResult.FAILURE);
        MessageInterceptor interceptor = mock(MessageInterceptor.class);

        BatchConsumeTask task = new BatchConsumeTask(clientId, listener, messageViews, interceptor);
        ConsumeResult result = task.call();

        assertEquals(ConsumeResult.FAILURE, result);
    }

    @Test
    public void testCallWithException() {
        ClientId clientId = new ClientId();
        List<MessageViewImpl> messageViews = Arrays.asList(fakeMessageViewImpl());
        BatchMessageListener listener = mock(BatchMessageListener.class);
        when(listener.consume(any())).thenThrow(new RuntimeException("test error"));
        MessageInterceptor interceptor = mock(MessageInterceptor.class);

        BatchConsumeTask task = new BatchConsumeTask(clientId, listener, messageViews, interceptor);
        ConsumeResult result = task.call();

        assertEquals(ConsumeResult.FAILURE, result);
        verify(interceptor, times(1)).doBefore(any(), any());
        verify(interceptor, times(1)).doAfter(any(), any());
    }

    @Test
    public void testCallWithNullReturn() {
        ClientId clientId = new ClientId();
        List<MessageViewImpl> messageViews = Arrays.asList(fakeMessageViewImpl());
        BatchMessageListener listener = mock(BatchMessageListener.class);
        when(listener.consume(any())).thenReturn(null);
        MessageInterceptor interceptor = mock(MessageInterceptor.class);

        BatchConsumeTask task = new BatchConsumeTask(clientId, listener, messageViews, interceptor);
        ConsumeResult result = task.call();

        assertEquals(ConsumeResult.FAILURE, result);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMessageViewsPassedAsUnmodifiableList() {
        ClientId clientId = new ClientId();
        List<MessageViewImpl> messageViews = Arrays.asList(fakeMessageViewImpl(), fakeMessageViewImpl());
        BatchMessageListener listener = messages -> {
            try {
                messages.add(null);
                return ConsumeResult.FAILURE;
            } catch (UnsupportedOperationException e) {
                return ConsumeResult.SUCCESS;
            }
        };
        MessageInterceptor interceptor = mock(MessageInterceptor.class);

        BatchConsumeTask task = new BatchConsumeTask(clientId, listener, messageViews, interceptor);
        ConsumeResult result = task.call();

        assertEquals(ConsumeResult.SUCCESS, result);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBatchSizePassedCorrectly() {
        ClientId clientId = new ClientId();
        List<MessageViewImpl> messageViews = Arrays.asList(
            fakeMessageViewImpl(), fakeMessageViewImpl(), fakeMessageViewImpl());
        BatchMessageListener listener = messages -> {
            assertEquals(3, messages.size());
            return ConsumeResult.SUCCESS;
        };
        MessageInterceptor interceptor = mock(MessageInterceptor.class);

        BatchConsumeTask task = new BatchConsumeTask(clientId, listener, messageViews, interceptor);
        ConsumeResult result = task.call();

        assertEquals(ConsumeResult.SUCCESS, result);
    }
}

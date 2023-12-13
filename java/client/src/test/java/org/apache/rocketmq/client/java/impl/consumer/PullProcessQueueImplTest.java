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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import apache.rocketmq.v2.PullMessageRequest;
import com.google.common.util.concurrent.Futures;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PullProcessQueueImplTest extends TestBase {
    private final ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
        .setEndpoints(FAKE_ENDPOINTS).build();

    private final int maxCacheMessageCountEachQueue = 128;
    private final int maxCacheMessageSizeInBytesEachQueue = 128;

    private PullConsumerImpl spyConsumer;
    private PullProcessQueueImpl spyPq;

    @Before
    public void init() {
        int maxCacheMessageSizeInBytesTotalQueue = 128;
        int maxCacheMessageCountTotalQueue = 128;
        spyConsumer = spy(new PullConsumerImpl(clientConfiguration, FAKE_CONSUMER_GROUP_0, false,
            Duration.ofSeconds(3), maxCacheMessageCountTotalQueue, maxCacheMessageCountEachQueue,
            maxCacheMessageSizeInBytesTotalQueue, maxCacheMessageSizeInBytesEachQueue));
        spyPq = spy(new PullProcessQueueImpl(spyConsumer, fakeMessageQueueImpl0(), FilterExpression.SUB_ALL));
    }

    @Test
    public void testExpired() {
        Assert.assertFalse(spyPq.expired());
    }

    @Test
    public void testIsCacheFullWhenNoMessage() {
        Assert.assertFalse(spyPq.isCacheFull());
    }

    @Test
    public void testIsCacheFullWhenMessageCountExceeds() {
        doReturn(1L + maxCacheMessageCountEachQueue).when(spyPq).getCachedMessageCount();
        Assert.assertTrue(spyPq.isCacheFull());
    }

    @Test
    public void testIsCacheFullWhenMessageBytesExceeds() {
        doReturn(1L + maxCacheMessageSizeInBytesEachQueue).when(spyPq).getCachedMessageBytes();
        Assert.assertTrue(spyPq.isCacheFull());
    }

    @Test
    public void testPullMessageImmediatelyWhenConsumerIsNotRunning() {
        doReturn(false).when(spyConsumer).isRunning();
        spyPq.pullMessageImmediately();
        verify(spyConsumer, never()).getScheduler();
    }

    @Test
    public void testPullMessageImmediatelyWithOffsetNotFound() {
        doReturn(true).when(spyConsumer).isRunning();
        doReturn(okGetOffsetResponseFuture()).when(spyConsumer).getOffset(any(MessageQueueImpl.class));
        doNothing().when(spyPq).pullMessageImmediately(anyLong());
        spyPq.pullMessageImmediately();
        verify(spyPq, times(1)).pullMessageImmediately(anyLong());
    }

    @Test
    public void testPullMessageImmediatelyWithOffsetWhenConsumerIsNotRunning() {
        doReturn(false).when(spyConsumer).isRunning();
        spyPq.pullMessageImmediately(1);
        verify(spyPq, never()).isCacheFull();
    }

    @Test
    public void testPullMessageImmediatelyWithOffsetWithFullCache() {
        doReturn(true).when(spyConsumer).isRunning();
        doReturn(true).when(spyPq).isCacheFull();
        spyPq.pullMessageImmediately(1);
        verify(spyConsumer, never()).getScheduler();
    }

    @Test
    public void testPullMessageImmediatelyWithOffset() {
        doReturn(true).when(spyConsumer).isRunning();

        final Endpoints endpoints = fakeEndpoints();
        final MessageViewImpl messageView = fakeMessageViewImpl(fakeMessageQueueImpl0());
        List<MessageViewImpl> messageViewList = new ArrayList<>();
        messageViewList.add(messageView);
        long nextOffset = 100;
        final PullMessageResult pullMessageResult = new PullMessageResult(endpoints, messageViewList, nextOffset);

        doReturn(Futures.immediateFuture(pullMessageResult)).when(spyConsumer)
            .pullMessage(any(PullMessageRequest.class), any(MessageQueueImpl.class), any(Duration.class));
        doNothing().when(spyPq).onPullMessageResult(any(PullMessageResult.class), anyLong());
        spyPq.pullMessageImmediately(1);
        verify(spyPq, times(1)).onPullMessageResult(any(PullMessageResult.class), anyLong());
    }
}

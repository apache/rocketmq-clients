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

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import apache.rocketmq.v2.UpdateOffsetRequest;
import apache.rocketmq.v2.UpdateOffsetResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.TopicMessageQueueChangeListener;
import org.apache.rocketmq.client.apis.message.MessageQueue;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.route.TopicRouteData;
import org.apache.rocketmq.client.java.rpc.RpcFuture;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PullConsumerImplTest extends TestBase {

    private final ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
        .setEndpoints(FAKE_ENDPOINTS).build();

    private PullConsumerImpl pullConsumer;

    @Before
    public void init() {
        pullConsumer = spy(new PullConsumerImpl(clientConfiguration, FAKE_CONSUMER_GROUP_0, false,
            Duration.ofSeconds(3), Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE));
    }

    @Test
    public void testGetSubscription() {
        final Map<MessageQueueImpl, FilterExpression> map = pullConsumer.getSubscriptions();
        assertTrue(map.isEmpty());
    }

    @Test(expected = IllegalStateException.class)
    public void testAssignBeforeStartup() {
        final MessageQueue mq = fakeMessageQueueImpl0();
        List<MessageQueue> list = new ArrayList<>();
        list.add(mq);
        pullConsumer.assign(list);
    }

    @Test
    public void testAssign() {
        doReturn(true).when(pullConsumer).isRunning();
        final MessageQueue mq = fakeMessageQueueImpl0();
        List<MessageQueue> list = new ArrayList<>();
        list.add(mq);
        pullConsumer.assign(list);
    }

    @Test(expected = IllegalStateException.class)
    public void testRegisterMessageQueueChangeListenerByTopicBeforeStartup() throws ClientException {
        pullConsumer.registerMessageQueueChangeListenerByTopic("abc", (topic, messageQueues) -> {
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testPollBeforeStartup() throws InterruptedException {
        pullConsumer.poll(Duration.ofSeconds(3));
    }

    @Test(expected = IllegalStateException.class)
    public void testSeekBeforeStartup() {
        pullConsumer.seek(fakeMessageQueueImpl0(), 0);
    }

    @Test(expected = IllegalStateException.class)
    public void testPauseBeforeStartup() {
        final ArrayList<MessageQueue> list = new ArrayList<>();
        list.add(fakeMessageQueueImpl0());
        pullConsumer.pause(list);
    }

    @Test(expected = IllegalStateException.class)
    public void testResumeBeforeStartup() {
        final ArrayList<MessageQueue> list = new ArrayList<>();
        list.add(fakeMessageQueueImpl0());
        pullConsumer.resume(list);
    }

    @Test(expected = IllegalStateException.class)
    public void testOffsetForTimestampBeforeStartup() throws ClientException {
        final MessageQueueImpl mq = fakeMessageQueueImpl0();
        pullConsumer.offsetForTimestamp(mq, System.currentTimeMillis());
    }

    @Test(expected = IllegalStateException.class)
    public void testCommittedBeforeStartup() throws ClientException {
        final MessageQueueImpl mq = fakeMessageQueueImpl0();
        pullConsumer.committed(mq);
    }

    @Test(expected = IllegalStateException.class)
    public void testCommitBeforeStartup() throws ClientException {
        pullConsumer.commit();
    }

    @Test(expected = IllegalStateException.class)
    public void testSeekToBeginBeforeStartup() throws ClientException {
        pullConsumer.seekToBegin(fakeMessageQueueImpl0());
    }

    @Test(expected = IllegalStateException.class)
    public void testSeekToEndBeforeStartup() throws ClientException {
        pullConsumer.seekToEnd(fakeMessageQueueImpl0());
    }

    @Test
    public void testCacheMessages() {
        PullConsumerImpl pullConsumer = new PullConsumerImpl(clientConfiguration, FAKE_CONSUMER_GROUP_0,
            false, Duration.ofSeconds(3), Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
        final MessageQueueImpl messageQueueImpl = fakeMessageQueueImpl(FAKE_TOPIC_0);
        final PullProcessQueueImpl pq = new PullProcessQueueImpl(pullConsumer, messageQueueImpl,
            FilterExpression.SUB_ALL);
        final MessageViewImpl messageView = fakeMessageViewImpl(messageQueueImpl);
        List<MessageViewImpl> messageViewList = new ArrayList<>();
        messageViewList.add(messageView);
        pullConsumer.cacheMessages(pq, messageViewList);
        final List<MessageViewImpl> list = pullConsumer.peekCachedMessages(pq);
        Assert.assertEquals(list.size(), 1);
    }

    @Test
    public void testGetConsumerGroup() {
        PullConsumerImpl pullConsumer = new PullConsumerImpl(clientConfiguration, FAKE_CONSUMER_GROUP_0,
            false, Duration.ofSeconds(3), Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
        Assert.assertEquals(pullConsumer.getConsumerGroup(), FAKE_CONSUMER_GROUP_0);
    }

    @Test
    public void testGetMaxCacheMessageCountEachQueue() {
        int maxCacheMessageCountTotalQueue = 128;
        int maxCacheMessageCountEachQueue = 64;
        PullConsumerImpl consumer = new PullConsumerImpl(clientConfiguration, FAKE_CONSUMER_GROUP_0, false,
            Duration.ofSeconds(3),
            maxCacheMessageCountTotalQueue, maxCacheMessageCountEachQueue, Integer.MAX_VALUE, Integer.MAX_VALUE);
        Assert.assertEquals(maxCacheMessageCountEachQueue, consumer.getMaxCacheMessageCountEachQueue());

        maxCacheMessageCountTotalQueue = 256;
        maxCacheMessageCountEachQueue = 512;
        consumer = new PullConsumerImpl(clientConfiguration, FAKE_CONSUMER_GROUP_0, false, Duration.ofSeconds(3),
            maxCacheMessageCountTotalQueue, maxCacheMessageCountEachQueue, Integer.MAX_VALUE, Integer.MAX_VALUE);
        Assert.assertEquals(maxCacheMessageCountTotalQueue, consumer.getMaxCacheMessageCountEachQueue());
    }

    @Test
    public void testGetMaxCacheMessageSizeInBytesEachQueue() {
        int maxCacheMessageSizeInBytesTotalQueue = 128;
        int maxCacheMessageSizeInBytesEachQueue = 64;
        PullConsumerImpl consumer = new PullConsumerImpl(clientConfiguration, FAKE_CONSUMER_GROUP_0, false,
            Duration.ofSeconds(3), Integer.MAX_VALUE, Integer.MAX_VALUE, maxCacheMessageSizeInBytesTotalQueue,
            maxCacheMessageSizeInBytesEachQueue);
        Assert.assertEquals(maxCacheMessageSizeInBytesEachQueue, consumer.getMaxCacheMessageSizeInBytesEachQueue());

        maxCacheMessageSizeInBytesTotalQueue = 256;
        maxCacheMessageSizeInBytesEachQueue = 512;
        consumer = new PullConsumerImpl(clientConfiguration, FAKE_CONSUMER_GROUP_0, false, Duration.ofSeconds(3),
            Integer.MAX_VALUE, Integer.MAX_VALUE, maxCacheMessageSizeInBytesTotalQueue,
            maxCacheMessageSizeInBytesEachQueue);
        Assert.assertEquals(maxCacheMessageSizeInBytesTotalQueue, consumer.getMaxCacheMessageSizeInBytesEachQueue());
    }

    @Test
    public void testCreateProcessQueue() {
        PullConsumerImpl pullConsumer = new PullConsumerImpl(clientConfiguration, FAKE_CONSUMER_GROUP_0,
            false, Duration.ofSeconds(3), Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
        final MessageQueueImpl mq = fakeMessageQueueImpl(FAKE_TOPIC_0);
        Optional<PullProcessQueue> pq = pullConsumer.createProcessQueue(mq, FilterExpression.SUB_ALL);
        Assert.assertTrue(pq.isPresent());

        pq = pullConsumer.createProcessQueue(mq, FilterExpression.SUB_ALL);
        Assert.assertFalse(pq.isPresent());

        pullConsumer.dropProcessQueue(mq);
        pq = pullConsumer.createProcessQueue(mq, FilterExpression.SUB_ALL);
        Assert.assertTrue(pq.isPresent());
    }

    @Test
    public void testCreateProcessQueueWithOffset() {
        PullConsumerImpl pullConsumer = new PullConsumerImpl(clientConfiguration, FAKE_CONSUMER_GROUP_0,
            false, Duration.ofSeconds(3), Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
        final MessageQueueImpl mq = fakeMessageQueueImpl(FAKE_TOPIC_0);
        Optional<PullProcessQueue> pq = pullConsumer.createProcessQueue(mq, FilterExpression.SUB_ALL, 0);
        Assert.assertTrue(pq.isPresent());

        pq = pullConsumer.createProcessQueue(mq, FilterExpression.SUB_ALL, 0);
        Assert.assertFalse(pq.isPresent());

        pullConsumer.dropProcessQueue(mq);
        pq = pullConsumer.createProcessQueue(mq, FilterExpression.SUB_ALL, 0);
        Assert.assertTrue(pq.isPresent());
    }

    @Test
    public void testOnTopicRouteDataUpdateWithNullListener() {
        List<apache.rocketmq.v2.MessageQueue> pbMessageQueues = new ArrayList<>();
        pbMessageQueues.add(fakePbMessageQueue0());
        final TopicRouteData topicRouteData = new TopicRouteData(pbMessageQueues);
        pullConsumer.onTopicRouteDataUpdate0(FAKE_TOPIC_0, topicRouteData);
    }

    @Test
    public void testOnTopicRouteDataUpdate() throws ClientException {
        doReturn(true).when(pullConsumer).isRunning();
        final MessageQueueImpl mq = fakeMessageQueueImpl(FAKE_TOPIC_0);
        List<MessageQueue> mqs = new ArrayList<>();
        mqs.add(mq);
        doReturn(mqs).when(pullConsumer).fetchMessageQueues(anyString());
        TopicMessageQueueChangeListener listener = new TopicMessageQueueChangeListener() {
            @Override
            public void onChanged(String topic, Set<MessageQueue> messageQueues) {

            }
        };
        listener = spy(listener);
        pullConsumer.registerMessageQueueChangeListenerByTopic(FAKE_TOPIC_0, listener);
        verify(pullConsumer, times(1)).fetchMessageQueues(anyString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSeekWithMessageQueueIsNotContained() {
        doReturn(true).when(pullConsumer).isRunning();
        final MessageQueueImpl mq = fakeMessageQueueImpl(FAKE_TOPIC_0);
        pullConsumer.seek(mq, 1);
    }

    @Test
    public void testSeek() {
        doReturn(true).when(pullConsumer).isRunning();
        MessageQueueImpl mq = fakeMessageQueueImpl(FAKE_TOPIC_0);
        List<MessageQueue> mqs = new ArrayList<>();
        mqs.add(mq);
        pullConsumer.assign(mqs);
        pullConsumer.seek(mq, 1);
        verify(pullConsumer, times(1)).dropProcessQueue(any(MessageQueueImpl.class));
        verify(pullConsumer, times(1))
            .tryPullMessageByMessageQueueImmediately(any(MessageQueueImpl.class), any(FilterExpression.class),
                anyLong());
    }

    @Test
    public void testSeekToBegin() throws ClientException {
        doReturn(true).when(pullConsumer).isRunning();
        final MessageQueueImpl mq = fakeMessageQueueImpl(FAKE_TOPIC_0);
        List<MessageQueue> mqs = new ArrayList<>();
        mqs.add(mq);
        pullConsumer.assign(mqs);
        doReturn(okQueryOffsetResponseFuture()).when(pullConsumer).queryOffset(any(MessageQueueImpl.class),
            any(OffsetPolicy.class));
        pullConsumer.seekToBegin(mq);
        verify(pullConsumer, times(1)).queryOffset(any(MessageQueueImpl.class),
            any(OffsetPolicy.class));
    }

    @Test
    public void testSeekToEnd() throws ClientException {
        doReturn(true).when(pullConsumer).isRunning();
        final MessageQueueImpl mq = fakeMessageQueueImpl(FAKE_TOPIC_0);
        List<MessageQueue> mqs = new ArrayList<>();
        mqs.add(mq);
        pullConsumer.assign(mqs);
        doReturn(okQueryOffsetResponseFuture()).when(pullConsumer).queryOffset(any(MessageQueueImpl.class),
            any(OffsetPolicy.class));
        pullConsumer.seekToEnd(mq);
        verify(pullConsumer, times(1)).queryOffset(any(MessageQueueImpl.class),
            any(OffsetPolicy.class));
    }

    @Test
    public void testCommit() throws ClientException {
        List<RpcFuture<UpdateOffsetRequest, UpdateOffsetResponse>> futures = new ArrayList<>();
        final RpcFuture<UpdateOffsetRequest, UpdateOffsetResponse> future = okUpdateOffsetResponseFuture();
        futures.add(future);
        doReturn(futures).when(pullConsumer).commit0();
        pullConsumer.commit();
        verify(pullConsumer, times(1)).commit0();
    }
}

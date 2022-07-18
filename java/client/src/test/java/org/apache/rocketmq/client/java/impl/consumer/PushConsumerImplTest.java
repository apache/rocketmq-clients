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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.Collections;
import java.util.Map;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PushConsumerImplTest extends TestBase {
    private final Map<String, FilterExpression> subscriptionExpressions = createSubscriptionExpressions(FAKE_TOPIC_0);

    private final MessageListener messageListener = messageView -> ConsumeResult.SUCCESS;

    private final int maxCacheMessageCount = 8;
    private final int maxCacheMessageSizeInBytes = 1024;
    private final int consumptionThreadCount = 4;

    private final ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
        .setEndpoints(FAKE_ACCESS_POINT).build();

    @Spy
    private final PushConsumerImpl pushConsumer = new PushConsumerImpl(clientConfiguration, FAKE_GROUP_0,
        subscriptionExpressions, messageListener, maxCacheMessageCount, maxCacheMessageSizeInBytes,
        consumptionThreadCount);


    @Test(expected = IllegalStateException.class)
    public void testSubscribeBeforeStartup() throws ClientException {
        pushConsumer.subscribe(FAKE_TOPIC_0, new FilterExpression(FAKE_TOPIC_0));
    }

    @Test(expected = IllegalStateException.class)
    public void testUnsubscribeBeforeStartup() {
        pushConsumer.unsubscribe(FAKE_TOPIC_0);
    }

    @Test
    public void testQueryAssignment() {
        final PushConsumerImpl mockedPushConsumer = Mockito.mock(PushConsumerImpl.class);
        mockedPushConsumer.queryAssignment(FAKE_TOPIC_0);
    }

    @Test
    public void testScanAssignments() {
        final MessageQueueImpl messageQueue = fakeMessageQueueImpl0();
        final Assignment assignment = new Assignment(messageQueue);
        final Assignments assignments = new Assignments(Collections.singletonList(assignment));
        final ListenableFuture<Assignments> assignmentsFuture = Futures.immediateFuture(assignments);
        Mockito.when(pushConsumer.queryAssignment(FAKE_TOPIC_0)).thenReturn(assignmentsFuture);
        pushConsumer.scanAssignments();
        final ArgumentCaptor<String> topicCaptor0 = ArgumentCaptor.forClass(String.class);
        verify(pushConsumer, times(1))
            .syncProcessQueue(topicCaptor0.capture(), any(Assignments.class), any(FilterExpression.class));
        final String topic0 = topicCaptor0.getValue();
        assertEquals(topic0, FAKE_TOPIC_0);
        pushConsumer.scanAssignments();
        final ArgumentCaptor<String> topicCaptor1 = ArgumentCaptor.forClass(String.class);
        verify(pushConsumer, times(2))
            .syncProcessQueue(topicCaptor1.capture(), any(Assignments.class), any(FilterExpression.class));
        final String topic1 = topicCaptor1.getValue();
        assertEquals(topic1, FAKE_TOPIC_0);
    }

    @Test
    public void testScanAssignmentsWithoutResults() {
        final Assignments assignments = new Assignments(Collections.emptyList());
        final ListenableFuture<Assignments> assignmentsFuture = Futures.immediateFuture(assignments);
        Mockito.when(pushConsumer.queryAssignment(FAKE_TOPIC_0)).thenReturn(assignmentsFuture);
        pushConsumer.scanAssignments();
        verify(pushConsumer, never()).syncProcessQueue(any(String.class), any(Assignments.class),
            any(FilterExpression.class));
    }
}
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import apache.rocketmq.v2.LiteSubscriptionAction;
import com.google.common.util.concurrent.Futures;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.exception.LiteSubscriptionQuotaExceededException;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class LitePushConsumerImplTest {

    final String endpoints = "127.0.0.1:8080";

    LitePushConsumerSettings spySettings;

    private LitePushConsumerImpl consumer;

    @Before
    public void setUp() {
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder().setEndpoints(endpoints).build();

        LitePushConsumerBuilderImpl litePushConsumerBuilder = new LitePushConsumerBuilderImpl();
        litePushConsumerBuilder.setClientConfiguration(clientConfiguration);

        LitePushConsumerSettings realSettings = new LitePushConsumerSettings(litePushConsumerBuilder, new ClientId(),
            new Endpoints("127.0.0.1:8080"));

        spySettings = Mockito.spy(realSettings);

        MockitoAnnotations.openMocks(this);
        consumer = mock(LitePushConsumerImpl.class, CALLS_REAL_METHODS);
        // Set final field litePushConsumerSettings using reflection
        try {
            java.lang.reflect.Field field = LitePushConsumerImpl.class.getDeclaredField("litePushConsumerSettings");
            field.setAccessible(true);
            field.set(consumer, spySettings);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testSubscribeLiteNotRunning() throws ClientException {
        String liteTopic = "testLiteTopic";
        doThrow(new IllegalStateException("not running")).when(consumer).checkRunning();

        consumer.subscribeLite(liteTopic);
    }

    @Test
    public void testSubscribeLiteAlreadySubscribed() throws ClientException {
        String liteTopic = "testLiteTopic";
        doNothing().when(consumer).checkRunning();
        when(spySettings.containsLiteTopic(liteTopic)).thenReturn(true);

        consumer.subscribeLite(liteTopic);

        verify(consumer).checkRunning();
        verify(spySettings).containsLiteTopic(liteTopic);
        verify(consumer, never()).syncLiteSubscription(any(), any());
    }

    @Test
    public void testSubscribeLiteQuotaExceededThenUnsubscribeAndSubscribeAgain() throws ClientException {
        String liteTopic1 = "testLiteTopic1";
        String liteTopic2 = "testLiteTopic2";
        doNothing().when(consumer).checkRunning();
        doReturn(Futures.immediateVoidFuture()).when(consumer)
            .syncLiteSubscription(any(LiteSubscriptionAction.class), anyCollection());
        when(spySettings.getLiteSubscriptionQuota()).thenReturn(1);

        consumer.subscribeLite(liteTopic1);
        assertThat(spySettings.getLiteTopicSetSize()).isEqualTo(1);

        assertThatThrownBy(() -> consumer.subscribeLite(liteTopic2))
            .isInstanceOf(LiteSubscriptionQuotaExceededException.class);
        assertThat(spySettings.getLiteTopicSetSize()).isEqualTo(1);

        consumer.unsubscribeLite(liteTopic1);
        assertThat(spySettings.getLiteTopicSetSize()).isEqualTo(0);

        consumer.subscribeLite(liteTopic2);
        assertThat(spySettings.getLiteTopicSetSize()).isEqualTo(1);

        verify(spySettings, times(1)).addLiteTopic(liteTopic1);
        verify(spySettings, times(1)).removeLiteTopic(liteTopic1);
        verify(spySettings, times(1)).addLiteTopic(liteTopic2);
    }
}

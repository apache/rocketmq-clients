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

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;

public class LitePushConsumerBuilderImplTest extends TestBase {

    @Test(expected = IllegalArgumentException.class)
    public void testBindTopicWithNull() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        builder.bindTopic(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBindTopicWithBlank() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        builder.bindTopic("  ");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBindTopicWithEmpty() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        builder.bindTopic("");
    }

    @Test(expected = NullPointerException.class)
    public void testSetClientConfigurationWithNull() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        builder.setClientConfiguration(null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetConsumerGroupWithNull() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        builder.setConsumerGroup(null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetMessageListenerWithNull() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        builder.setMessageListener(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetNegativeMaxCacheMessageCount() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        builder.setMaxCacheMessageCount(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetNegativeMaxCacheMessageSizeInBytes() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        builder.setMaxCacheMessageSizeInBytes(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetNegativeConsumptionThreadCount() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        builder.setConsumptionThreadCount(-1);
    }

    @Test(expected = NullPointerException.class)
    public void testBuildWithoutClientConfiguration() throws ClientException {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        builder.setConsumerGroup(FAKE_CONSUMER_GROUP_0)
            .setMessageListener(messageView -> ConsumeResult.SUCCESS)
            .bindTopic("test-topic")
            .build();
    }

    @Test(expected = NullPointerException.class)
    public void testBuildWithoutConsumerGroup() throws ClientException {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        ClientConfiguration clientConfiguration =
            ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build();
        builder.setClientConfiguration(clientConfiguration)
            .setMessageListener(messageView -> ConsumeResult.SUCCESS)
            .bindTopic("test-topic")
            .build();
    }

    @Test(expected = NullPointerException.class)
    public void testBuildWithoutMessageListener() throws ClientException {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        ClientConfiguration clientConfiguration =
            ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build();
        builder.setClientConfiguration(clientConfiguration)
            .setConsumerGroup(FAKE_CONSUMER_GROUP_0)
            .bindTopic("test-topic")
            .build();
    }

    @Test(expected = NullPointerException.class)
    public void testBuildWithoutBindTopic() throws ClientException {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        ClientConfiguration clientConfiguration =
            ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build();
        builder.setClientConfiguration(clientConfiguration)
            .setConsumerGroup(FAKE_CONSUMER_GROUP_0)
            .setMessageListener(messageView -> ConsumeResult.SUCCESS)
            .build();
    }

}

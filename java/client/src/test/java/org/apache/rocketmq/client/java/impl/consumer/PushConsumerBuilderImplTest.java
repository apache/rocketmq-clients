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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.AsyncMessageListener;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;

public class PushConsumerBuilderImplTest extends TestBase {

    @Test(expected = NullPointerException.class)
    public void testSetClientConfigurationWithNull() {
        final PushConsumerBuilderImpl builder = new PushConsumerBuilderImpl();
        builder.setClientConfiguration(null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetConsumerGroupWithNull() {
        final PushConsumerBuilderImpl builder = new PushConsumerBuilderImpl();
        builder.setConsumerGroup(null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetMessageListenerWithNull() {
        final PushConsumerBuilderImpl builder = new PushConsumerBuilderImpl();
        builder.setMessageListener(null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetAsyncMessageListenerWithNull() {
        new PushConsumerBuilderImpl().setAsyncMessageListener(null);
    }

    @Test
    public void testAsyncListenerReplacesSynchronousListener() throws Exception {
        final PushConsumerBuilderImpl builder = new PushConsumerBuilderImpl();
        final MessageListener synchronousListener = message -> ConsumeResult.SUCCESS;
        final AsyncMessageListener asynchronousListener = message ->
            CompletableFuture.completedFuture(ConsumeResult.SUCCESS);

        builder.setMessageListener(synchronousListener).setAsyncMessageListener(asynchronousListener);

        assertNull(listenerField(builder, "messageListener"));
        assertSame(asynchronousListener, listenerField(builder, "asyncMessageListener"));
    }

    @Test
    public void testSynchronousListenerReplacesAsyncListener() throws Exception {
        final PushConsumerBuilderImpl builder = new PushConsumerBuilderImpl();
        final MessageListener synchronousListener = message -> ConsumeResult.SUCCESS;
        final AsyncMessageListener asynchronousListener = message ->
            CompletableFuture.completedFuture(ConsumeResult.SUCCESS);

        builder.setAsyncMessageListener(asynchronousListener).setMessageListener(synchronousListener);

        assertSame(synchronousListener, listenerField(builder, "messageListener"));
        assertNull(listenerField(builder, "asyncMessageListener"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetNegativeMaxCacheMessageCount() {
        final PushConsumerBuilderImpl builder = new PushConsumerBuilderImpl();
        builder.setMaxCacheMessageCount(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetNegativeMaxCacheMessageSizeInBytes() {
        final PushConsumerBuilderImpl builder = new PushConsumerBuilderImpl();
        builder.setMaxCacheMessageSizeInBytes(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetNegativeConsumptionThreadCount() {
        final PushConsumerBuilderImpl builder = new PushConsumerBuilderImpl();
        builder.setConsumptionThreadCount(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildWithoutExpressions() throws ClientException {
        final PushConsumerBuilderImpl builder = new PushConsumerBuilderImpl();
        ClientConfiguration clientConfiguration =
            ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build();
        builder.setClientConfiguration(clientConfiguration).setConsumerGroup(FAKE_CONSUMER_GROUP_0)
            .setMessageListener(messageView -> ConsumeResult.SUCCESS)
            .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildWithAsyncListenerWithoutExpressions() throws ClientException {
        new PushConsumerBuilderImpl()
            .setClientConfiguration(ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build())
            .setConsumerGroup(FAKE_CONSUMER_GROUP_0)
            .setAsyncMessageListener(message -> CompletableFuture.completedFuture(ConsumeResult.SUCCESS))
            .build();
    }

    @Test(expected = NullPointerException.class)
    public void testBuildWithoutEitherListener() throws ClientException {
        new PushConsumerBuilderImpl()
            .setClientConfiguration(ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build())
            .setConsumerGroup(FAKE_CONSUMER_GROUP_0)
            .setSubscriptionExpressions(createSubscriptionExpressions(FAKE_TOPIC_0))
            .build();
    }

    private Object listenerField(PushConsumerBuilderImpl builder, String fieldName) throws Exception {
        final Field field = PushConsumerBuilderImpl.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(builder);
    }
}

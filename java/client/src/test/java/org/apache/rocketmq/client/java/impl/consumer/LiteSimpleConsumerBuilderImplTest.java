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

import static org.junit.Assert.assertSame;

import java.time.Duration;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.LiteSimpleConsumerBuilder;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;

public class LiteSimpleConsumerBuilderImplTest extends TestBase {

    @Test(expected = IllegalArgumentException.class)
    public void testBindTopicWithBlankString() {
        final LiteSimpleConsumerBuilderImpl builder = new LiteSimpleConsumerBuilderImpl();
        builder.bindTopic("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBindTopicWithNull() {
        final LiteSimpleConsumerBuilderImpl builder = new LiteSimpleConsumerBuilderImpl();
        builder.bindTopic(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBindTopicWithWhitespaces() {
        final LiteSimpleConsumerBuilderImpl builder = new LiteSimpleConsumerBuilderImpl();
        builder.bindTopic("   ");
    }

    @Test(expected = NullPointerException.class)
    public void testSetClientConfigurationWithNull() {
        final LiteSimpleConsumerBuilderImpl builder = new LiteSimpleConsumerBuilderImpl();
        builder.setClientConfiguration(null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetConsumerGroupWithNull() {
        final LiteSimpleConsumerBuilderImpl builder = new LiteSimpleConsumerBuilderImpl();
        builder.setConsumerGroup(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetConsumerGroupWithInvalidPattern() {
        final LiteSimpleConsumerBuilderImpl builder = new LiteSimpleConsumerBuilderImpl();
        builder.setConsumerGroup("group with spaces");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetConsumerGroupWithSpecialCharacters() {
        final LiteSimpleConsumerBuilderImpl builder = new LiteSimpleConsumerBuilderImpl();
        builder.setConsumerGroup("group@with@at");
    }

    @Test(expected = NullPointerException.class)
    public void testSetAwaitDurationWithNull() {
        final LiteSimpleConsumerBuilderImpl builder = new LiteSimpleConsumerBuilderImpl();
        builder.setAwaitDuration(null);
    }

    @Test(expected = NullPointerException.class)
    public void testBuildWithoutClientConfiguration() throws ClientException {
        final LiteSimpleConsumerBuilderImpl builder = new LiteSimpleConsumerBuilderImpl();
        builder.bindTopic("test-topic")
            .setConsumerGroup("test-group")
            .setAwaitDuration(Duration.ofSeconds(30))
            .build();
    }

    @Test(expected = NullPointerException.class)
    public void testBuildWithoutConsumerGroup() throws ClientException {
        final LiteSimpleConsumerBuilderImpl builder = new LiteSimpleConsumerBuilderImpl();
        ClientConfiguration clientConfiguration =
            ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build();
        builder.setClientConfiguration(clientConfiguration)
            .bindTopic("test-topic")
            .setAwaitDuration(Duration.ofSeconds(30))
            .build();
    }

    @Test(expected = NullPointerException.class)
    public void testBuildWithoutAwaitDuration() throws ClientException {
        final LiteSimpleConsumerBuilderImpl builder = new LiteSimpleConsumerBuilderImpl();
        ClientConfiguration clientConfiguration =
            ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build();
        builder.setClientConfiguration(clientConfiguration)
            .setConsumerGroup("test-group")
            .bindTopic("test-topic")
            .build();
    }

    @Test(expected = NullPointerException.class)
    public void testBuildWithoutBindTopic() throws ClientException {
        final LiteSimpleConsumerBuilderImpl builder = new LiteSimpleConsumerBuilderImpl();
        ClientConfiguration clientConfiguration =
            ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build();
        builder.setClientConfiguration(clientConfiguration)
            .setConsumerGroup("test-group")
            .setAwaitDuration(Duration.ofSeconds(30))
            .build();
    }

    @Test
    public void testMethodChaining() {
        final LiteSimpleConsumerBuilderImpl builder = new LiteSimpleConsumerBuilderImpl();
        ClientConfiguration clientConfiguration =
            ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build();

        LiteSimpleConsumerBuilder result = builder
            .bindTopic("test-topic")
            .setClientConfiguration(clientConfiguration)
            .setConsumerGroup("test-group")
            .setAwaitDuration(Duration.ofSeconds(30));

        assertSame(builder, result);
    }

}
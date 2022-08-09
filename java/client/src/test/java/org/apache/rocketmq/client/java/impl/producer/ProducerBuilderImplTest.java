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

package org.apache.rocketmq.client.java.impl.producer;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.producer.TransactionResolution;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProducerBuilderImplTest {

    @Test(expected = NullPointerException.class)
    public void testSetClientConfigurationWithNull() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        builder.setClientConfiguration(null);
    }

    @SuppressWarnings("ConfusingArgumentToVarargsMethod")
    @Test(expected = NullPointerException.class)
    public void testSetTopicWithNull() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        builder.setTopics(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetIllegalTopic() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        builder.setTopics("\t");
    }

    @Test
    public void testSetTopic() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        builder.setTopics("abc");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetNegativeMaxAttempts() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        builder.setMaxAttempts(-1);
    }

    @Test
    public void testSetMaxAttempts() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        builder.setMaxAttempts(3);
    }

    @Test(expected = NullPointerException.class)
    public void testSetTransactionCheckerWithNull() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        builder.setTransactionChecker(null);
    }

    @Test
    public void testSetTransactionChecker() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        builder.setTransactionChecker(messageView -> TransactionResolution.COMMIT);
    }

    @Test(expected = NullPointerException.class)
    public void testBuildWithoutClientConfiguration() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        builder.build();
    }

    @Test
    public void testBuild() throws ClientException {
        ClientConfiguration clientConfiguration =
            ClientConfiguration.newBuilder().setEndpoints("foobar.com:8081").build();
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        builder.setClientConfiguration(clientConfiguration).build();
    }
}

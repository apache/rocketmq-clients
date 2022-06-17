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

package org.apache.rocketmq.client.consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.tools.TestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DefaultMQPushConsumerTest extends TestBase {

    private DefaultMQPushConsumer createPushConsumer(String consumerGroup, String topic) throws ClientException {
        final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener(
                new MessageListenerConcurrently() {
                    @Override
                    public ConsumeStatus consume(
                            List<MessageExt> messages, ConsumeContext context) {
                        return ConsumeStatus.OK;
                    }
                });
        return consumer;
    }

    @Test
    public void testStartWithoutListener() throws ClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(FAKE_GROUP_0);
        try {
            consumer.start();
            Assert.fail();
        } catch (IllegalStateException ignore) {
            // ignore on purpose.
        }
    }

    @Test
    public void testStartWithoutSubscription() throws ClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(FAKE_GROUP_0);
        consumer.registerMessageListener(
                new MessageListenerConcurrently() {
                    @Override
                    public ConsumeStatus consume(
                            List<MessageExt> messages, ConsumeContext context) {
                        return ConsumeStatus.OK;
                    }
                });
        consumer.start();
        consumer.shutdown();
    }

    @Test
    public void testStartAndShutdown() throws ClientException {
        DefaultMQPushConsumer consumer = createPushConsumer(FAKE_GROUP_0, FAKE_TOPIC_0);
        consumer.start();
        consumer.shutdown();
    }

    @Test
    public void testBroadCasting() throws ClientException {
        DefaultMQPushConsumer consumer = createPushConsumer(FAKE_GROUP_0, FAKE_TOPIC_0);
        consumer.setMessageModel(MessageModel.BROADCASTING);
        consumer.start();
        consumer.shutdown();
    }

    @Test
    public void testStartMultiConsumers() throws ClientException {
        {
            final DefaultMQPushConsumer consumer0 = createPushConsumer(FAKE_GROUP_1, FAKE_TOPIC_0);
            final DefaultMQPushConsumer consumer1 = createPushConsumer(FAKE_GROUP_2, FAKE_TOPIC_0);

            consumer0.start();
            consumer0.shutdown();
            consumer1.start();
            consumer1.shutdown();
        }
        {
            final DefaultMQPushConsumer consumer0 = createPushConsumer(FAKE_GROUP_1, FAKE_TOPIC_0);
            final DefaultMQPushConsumer consumer1 = createPushConsumer(FAKE_GROUP_2, FAKE_TOPIC_0);

            consumer0.start();
            consumer1.start();
            consumer0.shutdown();
            consumer1.shutdown();
        }
        {
            final DefaultMQPushConsumer consumer0 = createPushConsumer(FAKE_GROUP_1, FAKE_TOPIC_0);
            final DefaultMQPushConsumer consumer1 = createPushConsumer(FAKE_GROUP_2, FAKE_TOPIC_0);

            consumer0.start();
            consumer1.start();
            consumer0.shutdown();
            consumer1.shutdown();
        }
        {
            final DefaultMQPushConsumer consumer0 = createPushConsumer(FAKE_GROUP_1, FAKE_TOPIC_0);
            final DefaultMQPushConsumer consumer1 = createPushConsumer(FAKE_GROUP_2, FAKE_TOPIC_0);

            consumer0.start();
            consumer1.start();
            consumer1.shutdown();
            consumer0.shutdown();
        }
    }

    @Test(invocationCount = 16)
    public void testStartConsumersConcurrently() throws InterruptedException, ClientException {
        final DefaultMQPushConsumer consumer0 = createPushConsumer(FAKE_GROUP_1, FAKE_TOPIC_0);
        final DefaultMQPushConsumer consumer1 = createPushConsumer(FAKE_GROUP_2, FAKE_TOPIC_0);

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                2,
                2,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());

        final CountDownLatch latch = new CountDownLatch(2);
        final Runnable task0 = new Runnable() {
            @Override
            public void run() {
                try {
                    consumer0.start();
                    Thread.sleep(10);
                    consumer0.shutdown();
                    latch.countDown();
                } catch (Throwable ignore) {
                    // Ignore on purpose.
                }
            }
        };

        final Runnable task1 = new Runnable() {
            @Override
            public void run() {
                try {
                    consumer1.start();
                    Thread.sleep(10);
                    consumer1.shutdown();
                    latch.countDown();
                } catch (Throwable ignore) {
                    // Ignore on purpose.
                }
            }
        };

        final Random random = new Random();
        if (random.nextInt() % 2 == 0) {
            executor.submit(task0);
            executor.submit(task1);
        } else {
            executor.submit(task1);
            executor.submit(task0);
        }
        final boolean await = latch.await(8, TimeUnit.SECONDS);
        if (!await) {
            Assert.fail("Timeout to wait shutdown of consumer.");
        }
    }

    @Test
    public void testSetMessageModel() throws ClientException {
        DefaultMQPushConsumer consumer = createPushConsumer(FAKE_GROUP_0, FAKE_TOPIC_0);
        consumer.setMessageModel(MessageModel.BROADCASTING);
        assertEquals(consumer.getMessageModel(), MessageModel.BROADCASTING);
    }

    @Test
    public void testSetConsumerGroup() throws ClientException {
        DefaultMQPushConsumer consumer = createPushConsumer(FAKE_GROUP_0, FAKE_TOPIC_0);
        consumer.setConsumerGroup("FakeGroup");
        assertEquals(consumer.getConsumerGroup(), "FakeGroup");
    }

    @Test
    public void testSetNamespace() throws ClientException {
        DefaultMQPushConsumer consumer = createPushConsumer(FAKE_GROUP_0, FAKE_TOPIC_0);
        consumer.setNamespace("fakeNamespace");
        assertEquals(consumer.getNamespace(), "fakeNamespace");
    }

    @Test
    public void testSetConsumeMessageBatchMaxSize() throws ClientException {
        DefaultMQPushConsumer consumer = createPushConsumer(FAKE_GROUP_0, FAKE_TOPIC_0);
        consumer.setConsumeMessageBatchMaxSize(1);
        assertEquals(consumer.getConsumeMessageBatchMaxSize(), 1);
    }

    @Test
    public void testSetMaxAwaitBatchSizePerQueue() throws ClientException {
        DefaultMQPushConsumer consumer = createPushConsumer(FAKE_GROUP_0, FAKE_TOPIC_0);
        consumer.setMaxAwaitBatchSizePerQueue(1);
        assertEquals(consumer.getMaxAwaitBatchSizePerQueue(), 1);
    }

    @Test
    public void testSetMaxDeliveryAttempts() throws ClientException {
        DefaultMQPushConsumer consumer = createPushConsumer(FAKE_GROUP_0, FAKE_TOPIC_0);
        consumer.setMaxDeliveryAttempts(1);
        assertEquals(consumer.getMaxDeliveryAttempts(), 1);
    }

    @Test
    public void testSetFifoConsumptionSuspendTimeMillis() throws ClientException {
        DefaultMQPushConsumer consumer = createPushConsumer(FAKE_GROUP_0, FAKE_TOPIC_0);
        consumer.setFifoConsumptionSuspendTimeMillis(1);
        assertEquals(consumer.getFifoConsumptionSuspendTimeMillis(), 1);
    }

    @Test
    public void testSetMaxTotalCachedMessagesQuantityThreshold() throws ClientException {
        DefaultMQPushConsumer consumer = createPushConsumer(FAKE_GROUP_0, FAKE_TOPIC_0);
        consumer.setMaxTotalCachedMessagesQuantityThreshold(1);
        assertEquals(consumer.getMaxTotalCachedMessagesQuantityThreshold(), 1);
    }

    @Test
    public void testSetConsumptionTimeoutMillis() throws ClientException {
        DefaultMQPushConsumer consumer = createPushConsumer(FAKE_GROUP_0, FAKE_TOPIC_0);
        consumer.setConsumptionTimeoutMillis(1);
        assertEquals(consumer.getConsumptionTimeoutMillis(), 1);
    }

    @Test
    public void testSetMaxTotalCachedMessagesBytesThreshold() throws ClientException {
        DefaultMQPushConsumer consumer = createPushConsumer(FAKE_GROUP_0, FAKE_TOPIC_0);
        consumer.setMaxTotalCachedMessagesBytesThreshold(1);
        assertEquals(consumer.getMaxTotalCachedMessagesBytesThreshold(), 1);
    }

    @Test
    public void testSetConsumeFromWhere() throws ClientException {
        DefaultMQPushConsumer consumer = createPushConsumer(FAKE_GROUP_0, FAKE_TOPIC_0);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
        assertEquals(consumer.getConsumeFromWhere(), ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
    }

    @Test
    public void testSetConsumptionThreadsAmount() throws ClientException {
        DefaultMQPushConsumer consumer = createPushConsumer(FAKE_GROUP_0, FAKE_TOPIC_0);
        consumer.setConsumptionThreadsAmount(3);
        assertEquals(consumer.getConsumptionThreadsAmount(), 3);
    }

    @Test
    public void testSetMessageTracingEnabled() throws ClientException {
        DefaultMQPushConsumer consumer = createPushConsumer(FAKE_GROUP_0, FAKE_TOPIC_0);
        consumer.setMessageTracingEnabled(false);
        assertFalse(consumer.getMessageTracingEnabled());
    }
}

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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.java.impl.ClientManagerImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class PushConsumerRuntimeTuningTest extends TestBase {
    private final boolean virtualThreadsEnabled;
    private LocalPushConsumer consumer;

    public PushConsumerRuntimeTuningTest(boolean virtualThreadsEnabled) {
        this.virtualThreadsEnabled = virtualThreadsEnabled;
    }

    @Parameterized.Parameters(name = "virtualThreadsEnabled={0}")
    public static Collection<Object[]> executorModes() {
        return Arrays.asList(new Object[] {false}, new Object[] {true});
    }

    @Before
    public void setUp() {
        final ClientConfiguration configuration = ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS)
            .enableVirtualThreads(virtualThreadsEnabled).build();
        consumer = Mockito.spy(new LocalPushConsumer(configuration,
            createSubscriptionExpressions(FAKE_TOPIC_0)));
        Mockito.doReturn(2).when(consumer).getQueueSize();
    }

    @After
    public void tearDown() throws Exception {
        if (consumer != null) {
            consumer.releaseLocalResources();
        }
    }

    @Test
    public void testTuningPreservesIdentityExecutorAndSubscriptions() {
        final ClientId clientId = consumer.getClientId();
        final ExecutorService executor = consumer.getConsumptionExecutor();
        final Map<String, FilterExpression> subscriptions = new HashMap<>(consumer.getSubscriptionExpressions());
        final String consumerGroup = consumer.getConsumerGroup();

        consumer.updateRuntimeTuning(80, 800, 3);

        assertSame(clientId, consumer.getClientId());
        assertSame(executor, consumer.getConsumptionExecutor());
        assertEquals(subscriptions, consumer.getSubscriptionExpressions());
        assertEquals(consumerGroup, consumer.getConsumerGroup());
        assertEquals(40, consumer.cacheMessageCountThresholdPerQueue());
        assertEquals(400, consumer.cacheMessageBytesThresholdPerQueue());
        assertPlatformConcurrency(executor, 3);
    }

    @Test
    public void testCacheThresholdsFollowTuningAndQueueSize() {
        assertEquals(60, consumer.cacheMessageCountThresholdPerQueue());
        assertEquals(600, consumer.cacheMessageBytesThresholdPerQueue());

        consumer.updateRuntimeTuning(60, 600, 2);
        assertEquals(30, consumer.cacheMessageCountThresholdPerQueue());
        assertEquals(300, consumer.cacheMessageBytesThresholdPerQueue());
        Mockito.doReturn(8).when(consumer).getQueueSize();
        assertEquals(7, consumer.cacheMessageCountThresholdPerQueue());
        assertEquals(75, consumer.cacheMessageBytesThresholdPerQueue());
        Mockito.doReturn(1000).when(consumer).getQueueSize();
        assertEquals(1, consumer.cacheMessageCountThresholdPerQueue());
        assertEquals(1, consumer.cacheMessageBytesThresholdPerQueue());
        Mockito.doReturn(0).when(consumer).getQueueSize();
        assertEquals(0, consumer.cacheMessageCountThresholdPerQueue());
        assertEquals(0, consumer.cacheMessageBytesThresholdPerQueue());
    }

    @Test
    public void testTuningResizesExistingExecutorWithoutInterruptingTasks() throws Exception {
        final ExecutorService executor = consumer.getConsumptionExecutor();
        final CountDownLatch firstStarted = new CountDownLatch(1);
        final CountDownLatch secondStarted = new CountDownLatch(1);
        final CountDownLatch thirdStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Future<Integer> first = executor.submit(() -> {
            firstStarted.countDown();
            release.await();
            return 1;
        });
        final Future<Integer> second = executor.submit(() -> {
            secondStarted.countDown();
            release.await();
            return 2;
        });
        try {
            assertTrue(firstStarted.await(5, TimeUnit.SECONDS));
            assertTrue(secondStarted.await(5, TimeUnit.SECONDS));
            final Future<Integer> third = executor.submit(() -> {
                thirdStarted.countDown();
                return 3;
            });
            assertFalse(thirdStarted.await(100, TimeUnit.MILLISECONDS));

            consumer.updateRuntimeTuning(150, 1500, 3);

            assertSame(executor, consumer.getConsumptionExecutor());
            assertTrue(thirdStarted.await(5, TimeUnit.SECONDS));
            assertFalse(first.isDone());
            assertFalse(second.isDone());
            assertEquals(Integer.valueOf(3), third.get(5, TimeUnit.SECONDS));
            release.countDown();
            assertEquals(Integer.valueOf(1), first.get(5, TimeUnit.SECONDS));
            assertEquals(Integer.valueOf(2), second.get(5, TimeUnit.SECONDS));
        } finally {
            release.countDown();
        }
    }

    @Test
    public void testInvalidArgumentsLeaveCacheAndConcurrencyUnchanged() throws Exception {
        final ExecutorService executor = consumer.getConsumptionExecutor();
        final CountDownLatch firstStarted = new CountDownLatch(1);
        final CountDownLatch secondStarted = new CountDownLatch(1);
        final CountDownLatch thirdStarted = new CountDownLatch(1);
        final CountDownLatch releaseFirst = new CountDownLatch(1);
        final CountDownLatch releaseSecond = new CountDownLatch(1);
        final Future<Integer> first = executor.submit(() -> {
            firstStarted.countDown();
            releaseFirst.await();
            return 1;
        });
        final Future<Integer> second = executor.submit(() -> {
            secondStarted.countDown();
            releaseSecond.await();
            return 2;
        });
        try {
            assertTrue(firstStarted.await(5, TimeUnit.SECONDS));
            assertTrue(secondStarted.await(5, TimeUnit.SECONDS));
            final Future<Integer> third = executor.submit(() -> {
                thirdStarted.countDown();
                return 3;
            });
            final List<int[]> invalidTunings = Arrays.asList(
                new int[] {0, 770, 3}, new int[] {-1, 770, 3},
                new int[] {77, 0, 3}, new int[] {77, -1, 3},
                new int[] {77, 770, 0}, new int[] {77, 770, -1});
            for (int[] tuning : invalidTunings) {
                try {
                    consumer.updateRuntimeTuning(tuning[0], tuning[1], tuning[2]);
                    fail("Invalid runtime tuning should be rejected");
                } catch (IllegalArgumentException expected) {
                    assertEquals(60, consumer.cacheMessageCountThresholdPerQueue());
                    assertEquals(600, consumer.cacheMessageBytesThresholdPerQueue());
                    assertSame(executor, consumer.getConsumptionExecutor());
                    assertPlatformConcurrency(executor, 2);
                }
            }
            assertFalse("Invalid updates must not increase concurrency",
                thirdStarted.await(100, TimeUnit.MILLISECONDS));
            assertFalse(first.isDone());
            assertFalse(second.isDone());
            releaseFirst.countDown();
            assertEquals(Integer.valueOf(1), first.get(5, TimeUnit.SECONDS));
            assertTrue("The original second slot must remain available", thirdStarted.await(5, TimeUnit.SECONDS));
            assertFalse(second.isDone());
            assertEquals(Integer.valueOf(3), third.get(5, TimeUnit.SECONDS));
            releaseSecond.countDown();
            assertEquals(Integer.valueOf(2), second.get(5, TimeUnit.SECONDS));
        } finally {
            releaseFirst.countDown();
            releaseSecond.countDown();
        }
    }

    private void assertPlatformConcurrency(ExecutorService executor, int expected) {
        if (executor instanceof ThreadPoolExecutor) {
            assertEquals(expected, ((ThreadPoolExecutor) executor).getCorePoolSize());
            assertEquals(expected, ((ThreadPoolExecutor) executor).getMaximumPoolSize());
        }
    }

    private static class LocalPushConsumer extends PushConsumerImpl {
        LocalPushConsumer(ClientConfiguration configuration, Map<String, FilterExpression> subscriptions) {
            super(configuration, FAKE_CONSUMER_GROUP_0, subscriptions, message -> ConsumeResult.SUCCESS, null,
                120, 1200, 2, false, false);
        }

        void releaseLocalResources() throws Exception {
            // No service was started: dispose the constructor-owned resources without network calls.
            final Field workerField = ClientManagerImpl.class.getDeclaredField("asyncWorker");
            workerField.setAccessible(true);
            final ExecutorService asyncWorker = (ExecutorService) workerField.get(getClientManager());
            final List<ExecutorService> executors = Arrays.asList(getConsumptionExecutor(), getScheduler(),
                clientCallbackExecutor, telemetryCommandExecutor, asyncWorker);
            for (ExecutorService executor : executors) {
                executor.shutdownNow();
            }
            for (ExecutorService executor : executors) {
                assertTrue("Consumer executor did not terminate", executor.awaitTermination(5, TimeUnit.SECONDS));
            }
            clientMeterManager.shutdown();
        }
    }
}

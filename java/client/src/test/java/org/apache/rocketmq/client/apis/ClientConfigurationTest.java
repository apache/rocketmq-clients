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

package org.apache.rocketmq.client.apis;

import java.util.function.IntConsumer;
import org.junit.Assert;
import org.junit.Test;

public class ClientConfigurationTest {

    @Test
    public void testInternalThreadCountsUnsetByDefault() {
        ClientConfiguration configuration = ClientConfiguration.newBuilder()
            .setEndpoints("localhost:8081")
            .build();

        Assert.assertFalse(configuration.getSchedulerThreadCount().isPresent());
        Assert.assertFalse(configuration.getAsyncWorkerThreadCount().isPresent());
        Assert.assertFalse(configuration.getCallbackThreadCount().isPresent());
    }

    @Test
    public void testInternalThreadCountsAreIndependent() {
        ClientConfiguration schedulerConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints("localhost:8081").setSchedulerThreadCount(1).build();
        Assert.assertEquals(Integer.valueOf(1), schedulerConfiguration.getSchedulerThreadCount().get());
        Assert.assertFalse(schedulerConfiguration.getAsyncWorkerThreadCount().isPresent());
        Assert.assertFalse(schedulerConfiguration.getCallbackThreadCount().isPresent());

        ClientConfiguration asyncConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints("localhost:8081").setAsyncWorkerThreadCount(2).build();
        Assert.assertEquals(Integer.valueOf(2), asyncConfiguration.getAsyncWorkerThreadCount().get());
        Assert.assertFalse(asyncConfiguration.getSchedulerThreadCount().isPresent());
        Assert.assertFalse(asyncConfiguration.getCallbackThreadCount().isPresent());

        ClientConfiguration callbackConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints("localhost:8081").setCallbackThreadCount(3).build();
        Assert.assertEquals(Integer.valueOf(3), callbackConfiguration.getCallbackThreadCount().get());
        Assert.assertFalse(callbackConfiguration.getSchedulerThreadCount().isPresent());
        Assert.assertFalse(callbackConfiguration.getAsyncWorkerThreadCount().isPresent());
    }

    @Test
    public void testInternalThreadCountsAreImmutableAfterBuild() {
        ClientConfigurationBuilder builder = ClientConfiguration.newBuilder().setEndpoints("localhost:8081")
            .setSchedulerThreadCount(1).setAsyncWorkerThreadCount(2).setCallbackThreadCount(3);
        ClientConfiguration first = builder.build();
        ClientConfiguration second = builder.setSchedulerThreadCount(4).setAsyncWorkerThreadCount(5)
            .setCallbackThreadCount(6).build();

        Assert.assertEquals(Integer.valueOf(1), first.getSchedulerThreadCount().get());
        Assert.assertEquals(Integer.valueOf(2), first.getAsyncWorkerThreadCount().get());
        Assert.assertEquals(Integer.valueOf(3), first.getCallbackThreadCount().get());
        Assert.assertEquals(Integer.valueOf(4), second.getSchedulerThreadCount().get());
        Assert.assertEquals(Integer.valueOf(5), second.getAsyncWorkerThreadCount().get());
        Assert.assertEquals(Integer.valueOf(6), second.getCallbackThreadCount().get());
    }

    @Test
    public void testInvalidInternalThreadCountsLeaveBuilderUnchanged() {
        ClientConfigurationBuilder builder = ClientConfiguration.newBuilder().setEndpoints("localhost:8081")
            .setSchedulerThreadCount(1).setAsyncWorkerThreadCount(2).setCallbackThreadCount(3);
        assertRejectsNonPositiveThreadCounts(builder::setSchedulerThreadCount);
        assertRejectsNonPositiveThreadCounts(builder::setAsyncWorkerThreadCount);
        assertRejectsNonPositiveThreadCounts(builder::setCallbackThreadCount);

        ClientConfiguration configuration = builder.build();
        Assert.assertEquals(Integer.valueOf(1), configuration.getSchedulerThreadCount().get());
        Assert.assertEquals(Integer.valueOf(2), configuration.getAsyncWorkerThreadCount().get());
        Assert.assertEquals(Integer.valueOf(3), configuration.getCallbackThreadCount().get());
    }

    private void assertRejectsNonPositiveThreadCounts(IntConsumer setter) {
        for (int count : new int[] {0, -1, Integer.MIN_VALUE}) {
            try {
                setter.accept(count);
                Assert.fail("Non-positive thread count should be rejected: " + count);
            } catch (IllegalArgumentException expected) {
                // Invalid input must not mutate the previous setting.
            }
        }
    }

    @Test
    public void testVirtualThreadsDisabledByDefault() {
        ClientConfiguration configuration = ClientConfiguration.newBuilder()
            .setEndpoints("localhost:8081")
            .build();

        Assert.assertFalse(configuration.isVirtualThreadsEnabled());
    }

    @Test
    public void testEnableVirtualThreads() {
        ClientConfiguration configuration = ClientConfiguration.newBuilder()
            .setEndpoints("localhost:8081")
            .enableVirtualThreads(true)
            .build();

        Assert.assertTrue(configuration.isVirtualThreadsEnabled());
    }
}

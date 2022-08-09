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

package org.apache.rocketmq.client.java.retry;

import apache.rocketmq.v2.CustomizedBackoff;
import apache.rocketmq.v2.ExponentialBackoff;
import com.google.protobuf.util.Durations;
import java.time.Duration;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Assert;
import org.junit.Test;

public class ExponentialBackoffRetryPolicyTest extends TestBase {

    @Test
    public void testNextAttemptDelayForImmediatelyRetryPolicy() {
        final ExponentialBackoffRetryPolicy retryPolicy = ExponentialBackoffRetryPolicy.immediatelyRetryPolicy(3);
        Assert.assertEquals(retryPolicy.getNextAttemptDelay(1), Duration.ZERO);
        Assert.assertEquals(retryPolicy.getNextAttemptDelay(2), Duration.ZERO);
        Assert.assertEquals(retryPolicy.getNextAttemptDelay(3), Duration.ZERO);
        Assert.assertEquals(retryPolicy.getNextAttemptDelay(4), Duration.ZERO);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNextAttemptDelayWithIllegalAttempt() {
        Duration initialBackoff = Duration.ofMillis(5);
        Duration maxBackoff = Duration.ofSeconds(1);
        double backoffMultiplier = 5;
        final ExponentialBackoffRetryPolicy retryPolicy = new ExponentialBackoffRetryPolicy(3,
            initialBackoff, maxBackoff, backoffMultiplier);
        retryPolicy.getNextAttemptDelay(0);
    }

    @Test
    public void testGetNextAttemptDelay() {
        Duration initialBackoff = Duration.ofMillis(5);
        Duration maxBackoff = Duration.ofSeconds(1);
        double backoffMultiplier = 5;
        final ExponentialBackoffRetryPolicy retryPolicy = new ExponentialBackoffRetryPolicy(3,
            initialBackoff, maxBackoff, backoffMultiplier);
        Assert.assertEquals(retryPolicy.getNextAttemptDelay(1), Duration.ofMillis(5));
        Assert.assertEquals(retryPolicy.getNextAttemptDelay(2), Duration.ofMillis(25));
        Assert.assertEquals(retryPolicy.getNextAttemptDelay(3), Duration.ofMillis(125));
        Assert.assertEquals(retryPolicy.getNextAttemptDelay(4), Duration.ofMillis(625));
        Assert.assertEquals(retryPolicy.getNextAttemptDelay(5), Duration.ofSeconds(1));
    }

    @Test
    public void testFromProtobuf() {
        final Duration initialBackoff = Duration.ofMillis(5);
        final Duration maxBackoff = Duration.ofSeconds(1);
        com.google.protobuf.Duration initialBackoff0 = Durations.fromNanos(initialBackoff.toNanos());
        com.google.protobuf.Duration maxBackoff0 = Durations.fromNanos(maxBackoff.toNanos());
        float backoffMultiplier = 5;
        int maxAttempts = 3;
        ExponentialBackoff exponentialBackoff = ExponentialBackoff.newBuilder()
            .setInitial(initialBackoff0).setMax(maxBackoff0).setMultiplier(backoffMultiplier).build();
        apache.rocketmq.v2.RetryPolicy retryPolicy = apache.rocketmq.v2.RetryPolicy.newBuilder()
            .setMaxAttempts(maxAttempts)
            .setExponentialBackoff(exponentialBackoff).build();
        ExponentialBackoffRetryPolicy exponentialBackoffRetryPolicy =
            ExponentialBackoffRetryPolicy.fromProtobuf(retryPolicy);
        Assert.assertEquals(exponentialBackoffRetryPolicy.getMaxAttempts(), maxAttempts);
        Assert.assertEquals(exponentialBackoffRetryPolicy.getInitialBackoff(), initialBackoff);
        Assert.assertEquals(exponentialBackoffRetryPolicy.getMaxBackoff(), maxBackoff);
        Assert.assertEquals(exponentialBackoffRetryPolicy.getBackoffMultiplier(), backoffMultiplier, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromProtobufWithoutExponentialBackoff() {
        int maxAttempts = 3;
        CustomizedBackoff customizedBackoff = CustomizedBackoff.newBuilder().addNext(Durations.fromSeconds(1)).build();
        apache.rocketmq.v2.RetryPolicy retryPolicy = apache.rocketmq.v2.RetryPolicy.newBuilder()
            .setMaxAttempts(maxAttempts).setCustomizedBackoff(customizedBackoff).build();
        ExponentialBackoffRetryPolicy.fromProtobuf(retryPolicy);
    }

    @Test
    public void testToProtobuf() {
        Duration initialBackoff = Duration.ofMillis(5);
        Duration maxBackoff = Duration.ofSeconds(1);
        double backoffMultiplier = 5;
        int maxAttempts = 3;
        final ExponentialBackoffRetryPolicy retryPolicy = new ExponentialBackoffRetryPolicy(maxAttempts,
            initialBackoff, maxBackoff, backoffMultiplier);
        final apache.rocketmq.v2.RetryPolicy retryPolicy0 = retryPolicy.toProtobuf();
        Assert.assertTrue(retryPolicy0.hasExponentialBackoff());
        final ExponentialBackoff exponentialBackoff = retryPolicy0.getExponentialBackoff();
        com.google.protobuf.Duration initialBackoff0 = Durations.fromNanos(initialBackoff.toNanos());
        com.google.protobuf.Duration maxBackoff0 = Durations.fromNanos(maxBackoff.toNanos());
        Assert.assertEquals(exponentialBackoff.getInitial(), initialBackoff0);
        Assert.assertEquals(exponentialBackoff.getMax(), maxBackoff0);
        Assert.assertEquals(exponentialBackoff.getMultiplier(), backoffMultiplier, 0);
        Assert.assertEquals(retryPolicy0.getMaxAttempts(), maxAttempts);
    }

    @Test
    public void testInheritBackoff() {
        Duration initialBackoff = Duration.ofMillis(5);
        Duration maxBackoff = Duration.ofSeconds(1);
        double backoffMultiplier = 5;
        int maxAttempts = 3;
        final ExponentialBackoffRetryPolicy retryPolicy = new ExponentialBackoffRetryPolicy(maxAttempts,
            initialBackoff, maxBackoff, backoffMultiplier);

        Duration initialBackoff0 = Duration.ofMillis(10);
        Duration maxBackoff0 = Duration.ofSeconds(3);
        double backoffMultiplier0 = 10;
        ExponentialBackoff exponentialBackoff = ExponentialBackoff.newBuilder()
            .setInitial(Durations.fromNanos(initialBackoff0.toNanos()))
            .setMax(Durations.fromNanos(maxBackoff0.toNanos())).setMultiplier((float) backoffMultiplier0).build();
        apache.rocketmq.v2.RetryPolicy retryPolicy0 = apache.rocketmq.v2.RetryPolicy.newBuilder()
            .setExponentialBackoff(exponentialBackoff).build();
        final RetryPolicy retryPolicy1 = retryPolicy.inheritBackoff(retryPolicy0);
        Assert.assertTrue(retryPolicy1 instanceof ExponentialBackoffRetryPolicy);
        ExponentialBackoffRetryPolicy exponentialBackoffRetryPolicy = (ExponentialBackoffRetryPolicy) retryPolicy1;
        Assert.assertEquals(exponentialBackoffRetryPolicy.getInitialBackoff(), initialBackoff0);
        Assert.assertEquals(exponentialBackoffRetryPolicy.getMaxBackoff(), maxBackoff0);
        Assert.assertEquals(exponentialBackoffRetryPolicy.getBackoffMultiplier(), backoffMultiplier0, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInheritBackoffWithoutExponentialBackoff() {
        int maxAttempts = 3;
        CustomizedBackoff customizedBackoff = CustomizedBackoff.newBuilder().addNext(Durations.fromSeconds(1)).build();
        apache.rocketmq.v2.RetryPolicy retryPolicy = apache.rocketmq.v2.RetryPolicy.newBuilder()
            .setMaxAttempts(maxAttempts).setCustomizedBackoff(customizedBackoff).build();

        Duration initialBackoff = Duration.ofMillis(5);
        Duration maxBackoff = Duration.ofSeconds(1);
        double backoffMultiplier = 5;
        final ExponentialBackoffRetryPolicy exponentialBackoffRetryPolicy =
            new ExponentialBackoffRetryPolicy(maxAttempts,
                initialBackoff, maxBackoff, backoffMultiplier);
        exponentialBackoffRetryPolicy.inheritBackoff(retryPolicy);
    }
}
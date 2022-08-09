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

import static apache.rocketmq.v2.RetryPolicy.StrategyCase.CUSTOMIZED_BACKOFF;
import static org.junit.Assert.assertEquals;

import apache.rocketmq.v2.CustomizedBackoff;
import apache.rocketmq.v2.ExponentialBackoff;
import com.google.protobuf.util.Durations;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

public class CustomizedBackoffRetryPolicyTest {

    @Test
    public void testNextAttemptDelay() {
        int maxAttempt = 3;
        Duration duration0 = Duration.ofSeconds(1);
        Duration duration1 = Duration.ofSeconds(2);
        List<Duration> durations = new ArrayList<>();
        durations.add(duration0);
        durations.add(duration1);
        final CustomizedBackoffRetryPolicy policy = new CustomizedBackoffRetryPolicy(durations, maxAttempt);
        assertEquals(maxAttempt, policy.getMaxAttempts());
        assertEquals(duration0, policy.getNextAttemptDelay(1));
        assertEquals(duration1, policy.getNextAttemptDelay(2));
        assertEquals(duration1, policy.getNextAttemptDelay(3));
        assertEquals(duration1, policy.getNextAttemptDelay(4));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNextAttemptDelayWithIllegalAttempt() {
        int maxAttempt = 3;
        Duration duration0 = Duration.ofSeconds(1);
        Duration duration1 = Duration.ofSeconds(2);
        List<Duration> durations = new ArrayList<>();
        durations.add(duration0);
        durations.add(duration1);
        final CustomizedBackoffRetryPolicy retryPolicy = new CustomizedBackoffRetryPolicy(durations, maxAttempt);
        retryPolicy.getNextAttemptDelay(0);
    }

    @Test
    public void testFromProtobuf() {
        com.google.protobuf.Duration duration0 = Durations.fromSeconds(1);
        com.google.protobuf.Duration duration1 = Durations.fromSeconds(2);
        com.google.protobuf.Duration duration2 = Durations.fromSeconds(3);
        List<com.google.protobuf.Duration> durations = new ArrayList<>();
        durations.add(duration0);
        durations.add(duration1);
        durations.add(duration2);
        CustomizedBackoff customizedBackoff = CustomizedBackoff.newBuilder().addAllNext(durations).build();
        apache.rocketmq.v2.RetryPolicy retryPolicy =
            apache.rocketmq.v2.RetryPolicy.newBuilder().setCustomizedBackoff(customizedBackoff).setMaxAttempts(3)
                .build();
        CustomizedBackoffRetryPolicy retryPolicy0 = CustomizedBackoffRetryPolicy.fromProtobuf(retryPolicy);
        final List<Duration> durations0 = durations.stream()
            .map(duration -> Duration.ofNanos(Durations.toNanos(duration))).collect(Collectors.toList());
        Assert.assertEquals(retryPolicy0.getDurations(), durations0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromProtobufWithoutCustomizedBackoff() {
        com.google.protobuf.Duration initialBackoff0 = Durations.fromSeconds(1);
        com.google.protobuf.Duration maxBackoff0 = Durations.fromSeconds(1);
        float backoffMultiplier = 1.0f;
        int maxAttempts = 3;
        ExponentialBackoff exponentialBackoff = ExponentialBackoff.newBuilder()
            .setInitial(initialBackoff0).setMax(maxBackoff0).setMultiplier(backoffMultiplier).build();
        apache.rocketmq.v2.RetryPolicy retryPolicy = apache.rocketmq.v2.RetryPolicy.newBuilder()
            .setMaxAttempts(maxAttempts)
            .setExponentialBackoff(exponentialBackoff).build();
        CustomizedBackoffRetryPolicy.fromProtobuf(retryPolicy);
    }

    @Test
    public void testToProtobuf() {
        int maxAttempt = 3;
        Duration duration0 = Duration.ofSeconds(1);
        Duration duration1 = Duration.ofSeconds(2);
        List<Duration> durations = new ArrayList<>();
        durations.add(duration0);
        durations.add(duration1);
        final CustomizedBackoffRetryPolicy policy = new CustomizedBackoffRetryPolicy(durations, maxAttempt);
        final apache.rocketmq.v2.RetryPolicy protobuf = policy.toProtobuf();
        assertEquals(maxAttempt, protobuf.getMaxAttempts());
        assertEquals(CUSTOMIZED_BACKOFF, protobuf.getStrategyCase());
        final CustomizedBackoff backoff = protobuf.getCustomizedBackoff();
        final List<com.google.protobuf.Duration> next = backoff.getNextList();
        assertEquals(durations.size(), next.size());
        for (int i = 0; i < durations.size(); i++) {
            assertEquals(durations.get(i).toNanos(), Durations.toNanos(next.get(i)));
        }
    }

    @Test
    public void testInheritBackoff() {
        List<com.google.protobuf.Duration> durations0 = new ArrayList<>();
        com.google.protobuf.Duration duration0 = Durations.fromSeconds(1);
        com.google.protobuf.Duration duration1 = Durations.fromSeconds(2);
        com.google.protobuf.Duration duration2 = Durations.fromSeconds(3);
        durations0.add(duration0);
        durations0.add(duration1);
        durations0.add(duration2);
        int maxAttempt0 = 5;
        CustomizedBackoff customizedBackoff = CustomizedBackoff.newBuilder().addAllNext(durations0).build();
        apache.rocketmq.v2.RetryPolicy retryPolicy = apache.rocketmq.v2.RetryPolicy.newBuilder()
            .setCustomizedBackoff(customizedBackoff)
            .setMaxAttempts(maxAttempt0)
            .build();
        int maxAttempt = 3;
        Duration duration3 = Duration.ofSeconds(3);
        Duration duration4 = Duration.ofSeconds(2);
        Duration duration5 = Duration.ofSeconds(1);
        List<Duration> durations1 = new ArrayList<>();
        durations1.add(duration3);
        durations1.add(duration4);
        durations1.add(duration5);
        final CustomizedBackoffRetryPolicy retryPolicy0 = new CustomizedBackoffRetryPolicy(durations1, maxAttempt);
        final RetryPolicy retryPolicy1 = retryPolicy0.inheritBackoff(retryPolicy);
        Assert.assertTrue(retryPolicy1 instanceof CustomizedBackoffRetryPolicy);
        CustomizedBackoffRetryPolicy customizedBackoffRetryPolicy = (CustomizedBackoffRetryPolicy) retryPolicy1;
        final List<Duration> durations2 = durations0.stream()
            .map(duration -> Duration.ofNanos(Durations.toNanos(duration))).collect(Collectors.toList());
        Assert.assertEquals(customizedBackoffRetryPolicy.getDurations(), durations2);
        Assert.assertEquals(customizedBackoffRetryPolicy.getMaxAttempts(), maxAttempt);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInheritBackoffWithoutCustomizedBackoff() {
        int maxAttempt = 3;
        Duration duration3 = Duration.ofSeconds(3);
        Duration duration4 = Duration.ofSeconds(2);
        Duration duration5 = Duration.ofSeconds(1);
        List<Duration> durations1 = new ArrayList<>();
        durations1.add(duration3);
        durations1.add(duration4);
        durations1.add(duration5);
        final CustomizedBackoffRetryPolicy retryPolicy0 = new CustomizedBackoffRetryPolicy(durations1, maxAttempt);
        ExponentialBackoff exponentialBackoff = ExponentialBackoff.newBuilder().build();
        apache.rocketmq.v2.RetryPolicy retryPolicy = apache.rocketmq.v2.RetryPolicy.newBuilder()
            .setExponentialBackoff(exponentialBackoff).build();
        retryPolicy0.inheritBackoff(retryPolicy);
    }
}
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
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import org.apache.rocketmq.client.apis.consumer.BatchPolicy;
import org.junit.Test;

public class BatchPolicyTest {

    @Test
    public void testConstructorWithValidParams() {
        BatchPolicy policy = new BatchPolicy(16, 1024, Duration.ofSeconds(2));
        assertEquals(16, policy.getMaxBatchSize());
        assertEquals(1024, policy.getMaxBatchBytes());
        assertEquals(Duration.ofSeconds(2), policy.getMaxWaitTime());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithZeroBatchSize() {
        new BatchPolicy(0, 1024, Duration.ofSeconds(1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNegativeBatchSize() {
        new BatchPolicy(-1, 1024, Duration.ofSeconds(1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithZeroBatchBytes() {
        new BatchPolicy(10, 0, Duration.ofSeconds(1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNegativeBatchBytes() {
        new BatchPolicy(10, -1, Duration.ofSeconds(1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullWaitTime() {
        new BatchPolicy(10, 1024, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithZeroWaitTime() {
        new BatchPolicy(10, 1024, Duration.ZERO);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNegativeWaitTime() {
        new BatchPolicy(10, 1024, Duration.ofMillis(-100));
    }

    @Test
    public void testBuilderWithDefaults() {
        BatchPolicy policy = BatchPolicy.newBuilder().build();
        assertEquals(BatchPolicy.DEFAULT_MAX_BATCH_SIZE, policy.getMaxBatchSize());
        assertEquals(BatchPolicy.DEFAULT_MAX_BATCH_BYTES, policy.getMaxBatchBytes());
        assertEquals(BatchPolicy.DEFAULT_MAX_WAIT_TIME, policy.getMaxWaitTime());
    }

    @Test
    public void testBuilderWithCustomValues() {
        BatchPolicy policy = BatchPolicy.newBuilder()
            .setMaxBatchSize(64)
            .setMaxBatchBytes(8 * 1024 * 1024)
            .setMaxWaitTime(Duration.ofMillis(500))
            .build();
        assertEquals(64, policy.getMaxBatchSize());
        assertEquals(8 * 1024 * 1024, policy.getMaxBatchBytes());
        assertEquals(Duration.ofMillis(500), policy.getMaxWaitTime());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuilderWithInvalidBatchSize() {
        BatchPolicy.newBuilder().setMaxBatchSize(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuilderWithInvalidBatchBytes() {
        BatchPolicy.newBuilder().setMaxBatchBytes(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuilderWithInvalidWaitTime() {
        BatchPolicy.newBuilder().setMaxWaitTime(Duration.ZERO);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuilderWithNullWaitTime() {
        BatchPolicy.newBuilder().setMaxWaitTime(null);
    }

    @Test
    public void testToString() {
        BatchPolicy policy = new BatchPolicy(32, 4096, Duration.ofSeconds(5));
        String str = policy.toString();
        assertTrue(str.contains("maxBatchSize=32"));
        assertTrue(str.contains("maxBatchBytes=4096"));
        assertTrue(str.contains("maxWaitTime=PT5S"));
    }
}

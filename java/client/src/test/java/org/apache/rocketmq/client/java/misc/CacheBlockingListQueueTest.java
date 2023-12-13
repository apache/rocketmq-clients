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

package org.apache.rocketmq.client.java.misc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

public class CacheBlockingListQueueTest {
    @Test
    public void testCache() {
        CacheBlockingListQueue<String, Integer> cacheQueue = new CacheBlockingListQueue<>();

        List<Integer> data = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            data.add(i);
        }

        cacheQueue.cache("A", data);
        assertEquals(1, cacheQueue.size());
    }

    @Test
    public void testPoll() throws InterruptedException {
        CacheBlockingListQueue<String, Integer> cacheQueue = new CacheBlockingListQueue<>();

        List<Integer> data = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            data.add(i);
        }

        cacheQueue.cache("A", data);
        assertEquals(1, cacheQueue.size());

        final Pair<String, List<Integer>> pair = cacheQueue.poll(Duration.ofSeconds(1));
        assertEquals(data, pair.getValue());
    }

    @Test
    public void testDrop() {
        CacheBlockingListQueue<String, Integer> cacheQueue = new CacheBlockingListQueue<>();

        List<Integer> data = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            data.add(i);
        }

        cacheQueue.cache("A", data);
        assertEquals(1, cacheQueue.size());

        cacheQueue.drop("A");
        assertEquals(0, cacheQueue.size());
        assertNull(cacheQueue.poll()); // Poll after dropping should return null
    }
}
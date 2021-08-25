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

package org.apache.rocketmq.utility;

import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class ThreadFactoryImplTest {

    @Test
    public void testNewThread() {
        String threadName = "TestThread";
        ThreadFactoryImpl factory = new ThreadFactoryImpl(threadName);
        final Thread thread = factory.newThread(new Runnable() {
            @Override
            public void run() {
            }
        });
        assertNotEquals(thread.getName(), threadName);
        assertTrue(thread.getName().contains(threadName));
        assertTrue(thread.getName().startsWith(ThreadFactoryImpl.THREAD_PREFIX + threadName));
    }
}
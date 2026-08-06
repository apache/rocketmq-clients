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

import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;

public class ExecutorServicesTest {

    @Test
    public void testVirtualThreadsDisabled() throws Exception {
        ExecutorService executor = ExecutorServices.newExecutorService(false, Executors::newSingleThreadExecutor);
        try {
            Future<Boolean> future = executor.submit(() -> isVirtual(Thread.currentThread()));
            Assert.assertFalse(future.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testVirtualThreadsEnabledWhenSupported() throws Exception {
        AtomicBoolean platformExecutorCreated = new AtomicBoolean(false);
        ExecutorService executor = ExecutorServices.newExecutorService(true, () -> {
            platformExecutorCreated.set(true);
            return Executors.newSingleThreadExecutor();
        });
        try {
            Future<Boolean> future = executor.submit(() -> isVirtual(Thread.currentThread()));
            Assert.assertEquals(ExecutorServices.isVirtualThreadSupported(), future.get());
            Assert.assertEquals(!ExecutorServices.isVirtualThreadSupported(), platformExecutorCreated.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testConcurrencyLimitedExecutorService() throws Exception {
        ExecutorService executor = ExecutorServices.newConcurrencyLimitedExecutorService(
            true, 1, Executors::newCachedThreadPool);
        CountDownLatch firstTaskStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstTask = new CountDownLatch(1);
        CountDownLatch secondTaskStarted = new CountDownLatch(1);
        try {
            Future<Boolean> first = executor.submit(() -> {
                firstTaskStarted.countDown();
                releaseFirstTask.await();
                return isVirtual(Thread.currentThread());
            });
            Assert.assertTrue(firstTaskStarted.await(5, TimeUnit.SECONDS));

            Future<Boolean> second = executor.submit(() -> {
                secondTaskStarted.countDown();
                return isVirtual(Thread.currentThread());
            });
            Assert.assertFalse(secondTaskStarted.await(200, TimeUnit.MILLISECONDS));

            releaseFirstTask.countDown();
            Assert.assertEquals(ExecutorServices.isVirtualThreadSupported(), first.get(5, TimeUnit.SECONDS));
            Assert.assertEquals(ExecutorServices.isVirtualThreadSupported(), second.get(5, TimeUnit.SECONDS));
            Assert.assertTrue(secondTaskStarted.await(5, TimeUnit.SECONDS));
        } finally {
            releaseFirstTask.countDown();
            executor.shutdownNow();
        }
    }

    private static boolean isVirtual(Thread thread) throws Exception {
        Method method;
        try {
            method = Thread.class.getMethod("isVirtual");
        } catch (NoSuchMethodException ignored) {
            return false;
        }
        return (Boolean) method.invoke(thread);
    }
}

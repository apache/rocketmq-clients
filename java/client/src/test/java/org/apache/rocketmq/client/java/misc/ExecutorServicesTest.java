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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Assume;
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

    @Test
    public void testUpdatePlatformConcurrencyLimitWhileTasksAreRunning() throws Exception {
        assertConcurrencyLimitCanGrow(false);
        assertConcurrencyLimitCanShrink(false);
    }

    @Test
    public void testUpdateVirtualConcurrencyLimitWhileTasksAreRunning() throws Exception {
        // On older JDKs this also exercises the concurrency-limited platform fallback.
        assertConcurrencyLimitCanGrow(true);
        assertConcurrencyLimitCanShrink(true);
    }

    @Test
    public void testUpdateConcurrencyLimitResizesPlatformFallback() {
        Assume.assumeFalse(ExecutorServices.isVirtualThreadSupported());
        AtomicReference<ThreadPoolExecutor> fallback = new AtomicReference<>();
        ExecutorService executor = ExecutorServices.newConcurrencyLimitedExecutorService(true, 1, () -> {
            ThreadPoolExecutor pool = newThreadPool(1);
            fallback.set(pool);
            return pool;
        });
        try {
            ExecutorServices.updateConcurrencyLimit(executor, 3);
            Assert.assertEquals(3, fallback.get().getCorePoolSize());
            Assert.assertEquals(3, fallback.get().getMaximumPoolSize());
            ExecutorServices.updateConcurrencyLimit(executor, 1);
            Assert.assertEquals(1, fallback.get().getCorePoolSize());
            Assert.assertEquals(1, fallback.get().getMaximumPoolSize());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testUpdateConcurrencyLimitRejectsNonPositiveLimits() {
        ExecutorService platform = newThreadPool(1);
        ExecutorService virtual = ExecutorServices.newConcurrencyLimitedExecutorService(true, 1,
            () -> newThreadPool(1));
        try {
            for (ExecutorService executor : new ExecutorService[] {platform, virtual}) {
                for (int invalidLimit : new int[] {0, -1}) {
                    try {
                        ExecutorServices.updateConcurrencyLimit(executor, invalidLimit);
                        Assert.fail("Expected an invalid concurrency limit to be rejected");
                    } catch (IllegalArgumentException expected) {
                        Assert.assertEquals("maxConcurrency should be positive", expected.getMessage());
                    }
                }
            }
            Assert.assertEquals(1, ((ThreadPoolExecutor) platform).getCorePoolSize());
            Assert.assertEquals(1, ((ThreadPoolExecutor) platform).getMaximumPoolSize());
        } finally {
            platform.shutdownNow();
            virtual.shutdownNow();
        }
    }

    @Test
    public void testUpdateConcurrencyLimitRejectsUnsupportedExecutor() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            ExecutorServices.updateConcurrencyLimit(executor, 2);
            Assert.fail("Expected an unsupported executor to be rejected");
        } catch (UnsupportedOperationException expected) {
            Assert.assertTrue(expected.getMessage().contains("does not support runtime concurrency updates"));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test(expected = NullPointerException.class)
    public void testUpdateConcurrencyLimitRejectsNullExecutor() {
        ExecutorServices.updateConcurrencyLimit(null, 1);
    }

    private static void assertConcurrencyLimitCanGrow(boolean virtualThreadsEnabled) throws Exception {
        AtomicReference<ThreadPoolExecutor> platform = new AtomicReference<>();
        ExecutorService executor = ExecutorServices.newConcurrencyLimitedExecutorService(virtualThreadsEnabled, 1,
            () -> {
                ThreadPoolExecutor pool = newThreadPool(1);
                platform.set(pool);
                return pool;
            });
        CountDownLatch firstTaskStarted = new CountDownLatch(1);
        CountDownLatch secondTaskStarted = new CountDownLatch(1);
        CountDownLatch releaseTasks = new CountDownLatch(1);
        try {
            Future<Boolean> first = executor.submit(() -> {
                firstTaskStarted.countDown();
                releaseTasks.await();
                return isVirtual(Thread.currentThread());
            });
            Assert.assertTrue(firstTaskStarted.await(5, TimeUnit.SECONDS));
            Future<Boolean> second = executor.submit(() -> {
                secondTaskStarted.countDown();
                releaseTasks.await();
                return isVirtual(Thread.currentThread());
            });
            Assert.assertFalse(secondTaskStarted.await(200, TimeUnit.MILLISECONDS));

            ExecutorServices.updateConcurrencyLimit(executor, 2);
            Assert.assertTrue(secondTaskStarted.await(5, TimeUnit.SECONDS));
            assertPlatformPoolSize(platform.get(), 2);
            if (!virtualThreadsEnabled) {
                Assert.assertSame(platform.get(), executor);
            }
            releaseTasks.countDown();
            boolean expectVirtual = virtualThreadsEnabled && ExecutorServices.isVirtualThreadSupported();
            Assert.assertEquals(expectVirtual, first.get(5, TimeUnit.SECONDS));
            Assert.assertEquals(expectVirtual, second.get(5, TimeUnit.SECONDS));
        } finally {
            releaseTasks.countDown();
            executor.shutdownNow();
        }
    }

    private static void assertConcurrencyLimitCanShrink(boolean virtualThreadsEnabled) throws Exception {
        AtomicReference<ThreadPoolExecutor> platform = new AtomicReference<>();
        ExecutorService executor = ExecutorServices.newConcurrencyLimitedExecutorService(virtualThreadsEnabled, 3,
            () -> {
                ThreadPoolExecutor pool = newThreadPool(3);
                platform.set(pool);
                return pool;
            });
        CountDownLatch initialTasksStarted = new CountDownLatch(3);
        CountDownLatch fourthTaskStarted = new CountDownLatch(1);
        List<CountDownLatch> releaseTasks = new ArrayList<>();
        List<Future<Boolean>> runningTasks = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                CountDownLatch releaseTask = new CountDownLatch(1);
                releaseTasks.add(releaseTask);
                runningTasks.add(executor.submit(() -> {
                    initialTasksStarted.countDown();
                    releaseTask.await();
                    return isVirtual(Thread.currentThread());
                }));
            }
            Assert.assertTrue(initialTasksStarted.await(5, TimeUnit.SECONDS));

            ExecutorServices.updateConcurrencyLimit(executor, 1);
            assertPlatformPoolSize(platform.get(), 1);
            if (!virtualThreadsEnabled) {
                Assert.assertSame(platform.get(), executor);
            }
            Future<Boolean> fourth = executor.submit(() -> {
                fourthTaskStarted.countDown();
                return isVirtual(Thread.currentThread());
            });
            Assert.assertFalse(fourthTaskStarted.await(200, TimeUnit.MILLISECONDS));

            boolean expectVirtual = virtualThreadsEnabled && ExecutorServices.isVirtualThreadSupported();
            for (int i = 0; i < 3; i++) {
                releaseTasks.get(i).countDown();
                Assert.assertEquals(expectVirtual, runningTasks.get(i).get(5, TimeUnit.SECONDS));
                if (i < 2) {
                    Assert.assertFalse(fourthTaskStarted.await(200, TimeUnit.MILLISECONDS));
                }
            }
            Assert.assertTrue(fourthTaskStarted.await(5, TimeUnit.SECONDS));
            Assert.assertEquals(expectVirtual, fourth.get(5, TimeUnit.SECONDS));
        } finally {
            for (CountDownLatch releaseTask : releaseTasks) {
                releaseTask.countDown();
            }
            executor.shutdownNow();
        }
    }

    private static ThreadPoolExecutor newThreadPool(int concurrency) {
        return new ThreadPoolExecutor(concurrency, concurrency, 0, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>());
    }

    private static void assertPlatformPoolSize(ThreadPoolExecutor executor, int concurrency) {
        if (executor != null) {
            Assert.assertEquals(concurrency, executor.getCorePoolSize());
            Assert.assertEquals(concurrency, executor.getMaximumPoolSize());
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

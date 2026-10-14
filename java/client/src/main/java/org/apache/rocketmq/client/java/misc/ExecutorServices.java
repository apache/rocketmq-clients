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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorServices {
    private static final Logger log = LoggerFactory.getLogger(ExecutorServices.class);
    private static final Method NEW_VIRTUAL_THREAD_PER_TASK_EXECUTOR = findVirtualThreadExecutorFactory();
    private static final AtomicBoolean VIRTUAL_THREAD_FALLBACK_LOGGED = new AtomicBoolean(false);
    private static final Object THREAD_POOL_RESIZE_LOCK = new Object();

    private ExecutorServices() {
    }

    /**
     * Creates a virtual-thread-per-task executor when requested and supported by the runtime. Reflection keeps the
     * client binary compatible with Java 8 while allowing it to use the JDK 21 API when available.
     */
    public static ExecutorService newExecutorService(boolean virtualThreadsEnabled,
        Supplier<ExecutorService> platformExecutorSupplier) {
        if (!virtualThreadsEnabled) {
            return platformExecutorSupplier.get();
        }
        if (null == NEW_VIRTUAL_THREAD_PER_TASK_EXECUTOR) {
            logVirtualThreadFallback(null);
            return platformExecutorSupplier.get();
        }
        try {
            return (ExecutorService) NEW_VIRTUAL_THREAD_PER_TASK_EXECUTOR.invoke(null);
        } catch (ReflectiveOperationException | RuntimeException e) {
            logVirtualThreadFallback(e);
            return platformExecutorSupplier.get();
        }
    }

    /**
     * Creates an executor which uses one virtual thread for each task when requested and supported, while limiting the
     * number of concurrently running tasks. A semaphore is used instead of pooling virtual threads so tasks waiting for
     * a permit do not occupy carrier threads.
     */
    public static ExecutorService newConcurrencyLimitedExecutorService(boolean virtualThreadsEnabled,
        int maxConcurrency, Supplier<ExecutorService> platformExecutorSupplier) {
        if (!virtualThreadsEnabled) {
            return platformExecutorSupplier.get();
        }
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("maxConcurrency should be positive");
        }
        return new ConcurrencyLimitedExecutorService(
            newExecutorService(true, platformExecutorSupplier), maxConcurrency);
    }

    /**
     * Updates the concurrency limit of an existing executor without replacing it or interrupting running tasks.
     * A reduced limit takes effect as running tasks finish. For virtual-thread executors created by
     * {@link #newConcurrencyLimitedExecutorService(boolean, int, Supplier)}, tasks wait for an available permit.
     *
     * @param executor a thread pool or an executor created by the concurrency-limited factory.
     * @param maxConcurrency the new positive concurrency limit.
     * @throws IllegalArgumentException if the limit is not positive.
     * @throws UnsupportedOperationException if the executor does not expose a supported concurrency limit.
     */
    public static void updateConcurrencyLimit(ExecutorService executor, int maxConcurrency) {
        Objects.requireNonNull(executor, "executor");
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("maxConcurrency should be positive");
        }
        if (executor instanceof ConcurrencyLimitedExecutorService) {
            ((ConcurrencyLimitedExecutorService) executor).updateConcurrencyLimit(maxConcurrency);
            return;
        }
        if (executor instanceof ThreadPoolExecutor) {
            resizeThreadPool((ThreadPoolExecutor) executor, maxConcurrency);
            return;
        }
        throw new UnsupportedOperationException("Executor does not support runtime concurrency updates: "
            + executor.getClass().getName());
    }

    private static void resizeThreadPool(ThreadPoolExecutor executor, int maxConcurrency) {
        synchronized (THREAD_POOL_RESIZE_LOCK) {
            // Raise the maximum before the core size; reduce the core size before the maximum.
            if (maxConcurrency > executor.getMaximumPoolSize()) {
                executor.setMaximumPoolSize(maxConcurrency);
            }
            executor.setCorePoolSize(maxConcurrency);
            if (executor.getMaximumPoolSize() != maxConcurrency) {
                executor.setMaximumPoolSize(maxConcurrency);
            }
        }
    }

    static boolean isVirtualThreadSupported() {
        return null != NEW_VIRTUAL_THREAD_PER_TASK_EXECUTOR;
    }

    private static Method findVirtualThreadExecutorFactory() {
        try {
            return java.util.concurrent.Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
        } catch (NoSuchMethodException | SecurityException ignored) {
            return null;
        }
    }

    private static void logVirtualThreadFallback(Throwable t) {
        if (!VIRTUAL_THREAD_FALLBACK_LOGGED.compareAndSet(false, true)) {
            return;
        }
        if (null == t) {
            log.warn("Virtual threads were enabled, but the runtime does not provide them; falling back to platform "
                + "threads");
            return;
        }
        log.warn("Failed to create a virtual-thread executor; falling back to platform threads", t);
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public static boolean awaitTerminated(ExecutorService executor) throws InterruptedException {
        return executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    private static class ConcurrencyLimitedExecutorService extends AbstractExecutorService {
        private final ExecutorService delegate;
        private final ResizableSemaphore semaphore;
        private int maxConcurrency;

        private ConcurrencyLimitedExecutorService(ExecutorService delegate, int maxConcurrency) {
            this.delegate = delegate;
            this.semaphore = new ResizableSemaphore(maxConcurrency);
            this.maxConcurrency = maxConcurrency;
        }

        private synchronized void updateConcurrencyLimit(int newMaxConcurrency) {
            // A runtime without virtual threads falls back to a platform executor. Resize that executor too.
            if (delegate instanceof ThreadPoolExecutor) {
                resizeThreadPool((ThreadPoolExecutor) delegate, newMaxConcurrency);
            }
            final int difference = newMaxConcurrency - maxConcurrency;
            if (difference > 0) {
                semaphore.release(difference);
            } else if (difference < 0) {
                semaphore.reduce(-difference);
            }
            maxConcurrency = newMaxConcurrency;
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            Objects.requireNonNull(command, "command");
            delegate.execute(() -> {
                boolean acquired = false;
                try {
                    semaphore.acquire();
                    acquired = true;
                    command.run();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    if (command instanceof Future<?>) {
                        // AbstractExecutorService.submit() wraps tasks in a FutureTask before execute(); cancel it so
                        // callers are not left waiting.
                        ((Future<?>) command).cancel(false);
                    }
                } finally {
                    if (acquired) {
                        semaphore.release();
                    }
                }
            });
        }
    }

    private static class ResizableSemaphore extends Semaphore {
        private static final long serialVersionUID = 1L;

        private ResizableSemaphore(int permits) {
            super(permits, true);
        }

        private void reduce(int permits) {
            // Negative available permits keep new tasks waiting until enough existing tasks have completed.
            reducePermits(permits);
        }
    }
}

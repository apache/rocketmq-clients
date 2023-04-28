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

import static com.google.common.base.Preconditions.checkNotNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CacheBlockingListQueue is a custom implementation of a linked blocking queue which caches pairs of keys and lists
 * of elements. It is an extension of {@link CustomLinkedBlockingQueue}. It is thread-safe and supports operations
 * for caching, polling, and dropping items.
 *
 * @param <T> The type of the key used for caching.
 * @param <E> The type of the elements present in the list.
 */
public class CacheBlockingListQueue<T, E> extends CustomLinkedBlockingQueue<Pair<T, List<E>>> {
    private static final long serialVersionUID = 3505724726193519379L;
    private static final Logger log = LoggerFactory.getLogger(CacheBlockingListQueue.class);
    private final Map<T, Pair<T, List<E>>> cacheMap;
    private final ReentrantReadWriteLock lock;

    /**
     * Constructs a new CacheBlockingListQueue with an empty cache map and a read-write lock.
     */
    public CacheBlockingListQueue() {
        this.cacheMap = new HashMap<>();
        this.lock = new ReentrantReadWriteLock();
    }

    public List<E> peek(T t) {
        lock.readLock().lock();
        try {
            final Pair<T, List<E>> pair = cacheMap.get(t);
            if (null == pair) {
                return new ArrayList<>();
            }
            final List<E> value = pair.getValue();
            if (null == value) {
                return new ArrayList<>();
            }
            return Collections.unmodifiableList(value);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Caches a key with a linked list of elements as a pair.
     *
     * @param t             The key to be cached.
     * @param linkedElement The list of elements to be associated with the key.
     */
    @SuppressWarnings("UnusedReturnValue")
    public void cache(T t, List<E> linkedElement) {
        final Pair<T, List<E>> pair = Pair.of(t, linkedElement);
        this.offer(pair);
    }

    /**
     * Retrieves and removes next available queue from the head of the queue,
     * waiting up to the specified duration if necessary.
     *
     * @param duration Time duration to wait before giving up.
     * @throws InterruptedException if the current thread is interrupted while waiting.
     */
    public Pair<T, List<E>> poll(Duration duration) throws InterruptedException {
        return poll(duration.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Removes the key and its associated list of elements from the cache, if it exists.
     *
     * @param key The key to be dropped from the cache.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void drop(T key) {
        fullyLock();
        lock.writeLock().lock();
        try {
            final Pair<T, List<E>> queue = cacheMap.remove(key);
            remove(queue);
        } finally {
            lock.writeLock().unlock();
            fullyUnlock();
        }
    }

    @Override
    protected Pair<T, List<E>> dequeue() {
        lock.writeLock().lock();
        try {
            final Pair<T, List<E>> queue = super.dequeue();
            final T key = queue.getKey();
            cacheMap.remove(key);
            return queue;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Inserts the specified element at the tail of the queue, respecting the key constraints.
     * This method is thread-safe and achieves its goal by acquiring a write lock.
     *
     * @param e The element to be added to the tail of the queue.
     * @return true if the element is added successfully, false otherwise.
     */
    @SuppressWarnings("NullableProblems")
    @Override
    public boolean offer(Pair<T, List<E>> e) {
        checkNotNull(e);
        final T key = e.getKey();
        putLock.lock();
        lock.writeLock().lock();
        final int c;
        try {
            final Pair<T, List<E>> exist = cacheMap.get(key);
            if (exist != null) {
                log.debug("Key exists in the cache, key={}", key);
                exist.getValue().addAll(e.getValue());
                return false;
            }
            log.debug("Key doesn't exist in the cache, key={}", key);
            cacheMap.put(key, e);
            final AtomicInteger count = this.count;
            if (count.get() == capacity) {
                return false;
            }
            final CustomLinkedBlockingQueue.Node<Pair<T, List<E>>> node = new CustomLinkedBlockingQueue.Node<>(e);
            if (count.get() == capacity) {
                return false;
            }
            enqueue(node);
            c = count.getAndIncrement();
            if (c + 1 < capacity) {
                notFull.signal();
            }
        } finally {
            lock.writeLock().unlock();
            putLock.unlock();
        }
        if (c == 0) {
            signalNotEmpty();
        }
        return true;

    }
}

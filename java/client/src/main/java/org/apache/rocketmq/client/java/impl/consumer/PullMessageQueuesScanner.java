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

import com.google.common.util.concurrent.AbstractIdleService;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a scanner to detect any changes made to the assigned message queues by the pull consumer.
 */
public class PullMessageQueuesScanner extends AbstractIdleService {
    private static final Logger log = LoggerFactory.getLogger(PullMessageQueuesScanner.class);
    private static final Duration SCAN_INITIAL_DELAY = Duration.ofSeconds(0);
    private static final Duration SCAN_PERIOD = Duration.ofSeconds(3);
    private final PullConsumerImpl consumer;
    private final AtomicBoolean scanTaskInQueueFlag;
    private final AtomicBoolean scannerStarted;
    private final ClientId clientId;

    public PullMessageQueuesScanner(PullConsumerImpl consumer) {
        this.consumer = consumer;
        this.scanTaskInQueueFlag = new AtomicBoolean(false);
        this.scannerStarted = new AtomicBoolean(false);
        this.clientId = consumer.getClientId();
    }

    /**
     * Starts the scanning process and logs a successful message if the process starts correctly.
     */
    @Override
    protected void startUp() {
        log.info("Begin to start the pull message queues scanner, clientId={}", clientId);
        log.info("The pull message queues scanner starts successfully, clientId={}", clientId);
    }

    void signal() {
        if (scannerStarted.compareAndSet(false, true)) {
            log.info("Begin to execute pull message queues scan task periodically, clientId={}", clientId);
            consumer.getScheduler().scheduleWithFixedDelay(this::signal0, SCAN_INITIAL_DELAY.toNanos(),
                SCAN_PERIOD.toNanos(), TimeUnit.NANOSECONDS);
        }
        signal0();
    }

    private void signal0() {
        if (scanTaskInQueueFlag.compareAndSet(false, true)) {
            consumer.getScheduler().submit(this::scan);
        }
    }

    private void scan() {
        scanTaskInQueueFlag.compareAndSet(true, false);
        scan0();
    }

    /**
     * Shuts down the scanning process and logs a successful message once the shutdown is complete.
     */
    @Override
    protected void shutDown() {
        log.info("Begin to shutdown the pull message queues scanner, clientId={}", clientId);
        log.info("Shutdown the pull message queues scanner successfully, clientId={}", clientId);
    }

    /**
     * Scans the assigned message queues to the pull consumer to detect any changes made and act accordingly.
     */
    private void scan0() {
        final ConcurrentMap<MessageQueueImpl, PullProcessQueue> processQueueTable = consumer.getProcessQueueTable();
        final Map<MessageQueueImpl, FilterExpression> subscriptions = consumer.getSubscriptions();
        final Set<MessageQueueImpl> latest = subscriptions.keySet();
        final Set<MessageQueueImpl> existed = processQueueTable.keySet();

        if (latest.isEmpty() && existed.isEmpty()) {
            log.debug("Message queues are empty, clientId={}", clientId);
            return;
        }

        if (!latest.equals(existed)) {
            log.info("Message queues have been changed, {} => {}, clientId={}", existed, subscriptions, clientId);
        }

        Set<MessageQueueImpl> activeMqs = new HashSet<>();
        for (Map.Entry<MessageQueueImpl, PullProcessQueue> entry : processQueueTable.entrySet()) {
            final MessageQueueImpl mq = entry.getKey();
            final PullProcessQueue pq = entry.getValue();
            if (!subscriptions.containsKey(mq)) {
                consumer.dropProcessQueue(mq);
                continue;
            }
            if (null != pq && pq.expired()) {
                consumer.dropProcessQueue(mq);
                continue;
            }
            activeMqs.add(mq);
        }
        for (Map.Entry<MessageQueueImpl, FilterExpression> entry : subscriptions.entrySet()) {
            final MessageQueueImpl mq = entry.getKey();
            if (activeMqs.contains(mq)) {
                continue;
            }
            final FilterExpression filterExpression = entry.getValue();
            consumer.tryPullMessageByMessageQueueImmediately(mq, filterExpression);
        }
    }
}

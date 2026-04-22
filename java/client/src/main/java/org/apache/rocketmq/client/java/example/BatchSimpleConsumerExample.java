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

package org.apache.rocketmq.client.java.example;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.BatchPolicy;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example demonstrating how to use the batch-receive capability of {@link SimpleConsumer}.
 *
 * <h3>Key concepts</h3>
 * <ul>
 *   <li><strong>Demand-driven fetching</strong>: The internal background thread only calls the
 *       server-side {@code receive()} API when there is at least one pending
 *       {@code batchReceive} / {@code batchReceiveAsync} call.  If no one is consuming, the
 *       background thread idles and produces <em>zero</em> server traffic.</li>
 *   <li><strong>FIFO ordering</strong>: If multiple threads call {@code batchReceive} /
 *       {@code batchReceiveAsync} concurrently, requests are queued internally and fulfilled
 *       strictly in the order they were submitted.</li>
 *   <li><strong>Batch acknowledge</strong>: Collect all successfully-processed messages in a
 *       list, then call {@code batchAck()} once.  The underlying network layer sends all ack
 *       entries in a single RPC per (broker, topic) group, which is much more efficient than
 *       acking one-by-one.</li>
 *   <li><strong>Graceful shutdown</strong>: When the consumer is closed, any messages still
 *       buffered internally have their invisible duration shortened to 1 second, so that the
 *       server makes them visible to other consumers promptly.</li>
 * </ul>
 */
public class BatchSimpleConsumerExample {
    private static final Logger log = LoggerFactory.getLogger(BatchSimpleConsumerExample.class);

    private BatchSimpleConsumerExample() {
    }

    @SuppressWarnings({"resource", "InfiniteLoopStatement"})
    public static void main(String[] args) throws ClientException {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();

        // Credential provider is optional for client configuration.
        String accessKey = "yourAccessKey";
        String secretKey = "yourSecretKey";
        SessionCredentialsProvider sessionCredentialsProvider =
            new StaticSessionCredentialsProvider(accessKey, secretKey);

        String endpoints = "foobar.com:8080";
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(endpoints)
            .setCredentialProvider(sessionCredentialsProvider)
            .build();
        String consumerGroup = "yourConsumerGroup";
        Duration awaitDuration = Duration.ofSeconds(30);
        String tag = "yourMessageTagA";
        String topic = "yourTopic";
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);

        // BatchPolicy controls when a batch is released to the caller:
        //   - up to 512 messages per batch, OR
        //   - up to 4 MB total message body size per batch, OR
        //   - at most 5 seconds of wait time after the first message arrives
        //   (whichever comes first).
        //
        // The maxBatchBytes limit (4 MB here) is critical for preventing client-side OOM
        // when messages have large bodies.  Adjust this value based on your JVM heap size.
        //
        // IMPORTANT: the server-side invisible duration = invisibleDuration + maxWaitTime.
        // The extra maxWaitTime covers the batching window so that messages do not become
        // visible to other consumers while still being aggregated locally.
        BatchPolicy batchPolicy = BatchPolicy.builder()
            .setMaxBatchCount(512)
            .setMaxBatchBytes(4L * 1024 * 1024)
            .setMaxWaitTime(Duration.ofSeconds(5))
            .build();

        SimpleConsumer consumer = provider.newSimpleConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            .setConsumerGroup(consumerGroup)
            .setAwaitDuration(awaitDuration)
            .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
            // Enable client-side batching.  Without this, batchReceive / batchReceiveAsync
            // will throw UnsupportedOperationException.
            .setBatchPolicy(batchPolicy)
            .build();

        // Set message invisible duration after it is received.
        Duration invisibleDuration = Duration.ofSeconds(150);

        // ------------------------------------------------------------------
        // Synchronous batch receive loop
        // ------------------------------------------------------------------
        // batchReceive() blocks the calling thread until a batch is ready (size or timeout).
        // The background thread is demand-driven:
        //   - It only calls server-side receive() when a batchReceive request is pending.
        //   - If you pause calling batchReceive(), the background thread idles automatically.
        //   - A single consumer thread is recommended for this pattern.
        //
        // batchAck() sends all ack entries in a single network round-trip per (broker, topic)
        // group, which is much more efficient than acking messages one-by-one.  Collect all
        // successfully-processed messages first, then call batchAck() once.
        do {
            final List<MessageView> messages = consumer.batchReceive(invisibleDuration);
            log.info("Received {} message(s)", messages.size());

            // Process messages and collect those that should be acknowledged.
            final List<MessageView> toAck = new ArrayList<>();
            for (MessageView message : messages) {
                try {
                    // ... your business logic here ...
                    toAck.add(message);
                } catch (Throwable t) {
                    log.error("Failed to process message, messageId={}", message.getMessageId(), t);
                    // Not added to toAck — the message will become visible again after the
                    // invisible duration expires and will be re-delivered.
                }
            }

            // Batch-acknowledge all successfully-processed messages in one shot.
            // Under the hood this groups entries by (broker endpoint, topic) and splits
            // each group into bounded chunks (default 512 entries per RPC) to avoid
            // oversized payloads, while still being much more efficient than per-message ack.
            if (!toAck.isEmpty()) {
                try {
                    consumer.batchAck(toAck);
                    log.info("Batch acked {} message(s)", toAck.size());
                } catch (Throwable t) {
                    log.error("Batch ack failed for {} message(s)", toAck.size(), t);
                }
            }
        } while (true);

        // ------------------------------------------------------------------
        // Asynchronous batch receive (alternative pattern)
        // ------------------------------------------------------------------
        // batchReceiveAsync() returns a CompletableFuture immediately.  Multiple async
        // calls are queued and fulfilled in FIFO order.
        //
        // CompletableFuture<List<MessageView>> future = consumer.batchReceiveAsync(invisibleDuration);
        // future.thenAccept(msgs -> {
        //     log.info("Async received {} message(s)", msgs.size());
        //     List<MessageView> ackList = new ArrayList<>();
        //     for (MessageView message : msgs) {
        //         try {
        //             // ... your business logic here ...
        //             ackList.add(message);
        //         } catch (Throwable t) {
        //             log.error("Ack failed, messageId={}", message.getMessageId(), t);
        //         }
        //     }
        //     if (!ackList.isEmpty()) {
        //         consumer.batchAckAsync(ackList);
        //     }
        // });

        // Close the consumer when you don't need it anymore.
        // On close(), buffered messages are released (invisible duration set to 1 second)
        // so that they become available to other consumers quickly.
        // consumer.close();
    }
}

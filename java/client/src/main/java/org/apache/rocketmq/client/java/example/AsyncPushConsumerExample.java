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

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncPushConsumerExample {
    private static final Logger log = LoggerFactory.getLogger(AsyncPushConsumerExample.class);

    private AsyncPushConsumerExample() {
    }

    public static void main(String[] args) throws ClientException, InterruptedException, IOException {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        final ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints("foobar.com:8080")
            .build();
        final ExecutorService applicationExecutor = Executors.newFixedThreadPool(8);
        try (PushConsumer consumer = provider.newPushConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            .setConsumerGroup("yourConsumerGroup")
            .setSubscriptionExpressions(Collections.singletonMap("yourTopic", FilterExpression.SUB_ALL))
            // With an asynchronous listener, this limits unfinished consumption stages.
            .setConsumptionThreadCount(8)
            .setAsyncMessageListener(message -> {
                // Return promptly. Do not call get() or join() on the stage in the listener.
                // An existing asynchronous application API can return its CompletionStage directly.
                return CompletableFuture.supplyAsync(() -> {
                    log.info("Process message={}", message);
                    // Return SUCCESS only after application processing has actually completed.
                    // FAILURE, exceptional completion, or cancellation follows the retry path.
                    return ConsumeResult.SUCCESS;
                }, applicationExecutor);
            })
            .build()) {
            // Keep the example running; interrupt this thread to shut it down.
            new CountDownLatch(1).await();
        } finally {
            // Consumer.close() waits for processing and acknowledgment before this executor shuts down.
            // Applications using other asynchronous resources must keep those resources alive too.
            applicationExecutor.shutdown();
            if (!applicationExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                applicationExecutor.shutdownNow();
            }
        }
    }
}

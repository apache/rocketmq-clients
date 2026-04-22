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
import java.time.Duration;
import java.util.Collections;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.BatchPolicy;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example of using batch consumption with PushConsumer.
 *
 * <p>Messages are accumulated locally according to the {@link BatchPolicy} and dispatched to the
 * batch listener in batches. A batch is flushed when any of the following conditions is met:
 * <ol>
 *   <li>The buffered message count reaches {@code maxBatchCount}.</li>
 *   <li>The total body size (bytes) reaches {@code maxBatchBytes}.</li>
 *   <li>The time since the first buffered message reaches {@code maxWaitTime}.</li>
 * </ol>
 */
public class BatchPushConsumerExample {
    private static final Logger log = LoggerFactory.getLogger(BatchPushConsumerExample.class);

    private BatchPushConsumerExample() {
    }

    public static void main(String[] args) throws ClientException, InterruptedException, IOException {
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

        String tag = "yourMessageTagA";
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        String consumerGroup = "yourConsumerGroup";
        String topic = "yourTopic";

        // Build a BatchPolicy: flush when 32 messages or 4 MB accumulated, or 5 seconds elapsed.
        BatchPolicy batchPolicy = BatchPolicy.builder()
            .setMaxBatchCount(32)
            .setMaxBatchBytes(4 * 1024 * 1024)
            .setMaxWaitTime(Duration.ofSeconds(5))
            .build();

        // In most case, you don't need to create too many consumers, singleton pattern is recommended.
        PushConsumer pushConsumer = provider.newPushConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            // Set the consumer group name.
            .setConsumerGroup(consumerGroup)
            .setMaxCacheMessageCount(1024)
            // Set the subscription for the consumer.
            .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
            // Use setBatchMessageListener instead of setMessageListener for batch consumption.
            .setBatchMessageListener(messageViews -> {
                // Handle the received batch of messages and return consume result for the entire batch.
                log.info("Received a batch of {} messages", messageViews.size());
                for (MessageView messageView : messageViews) {
                    log.info("  Message: topic={}, messageId={}", messageView.getTopic(),
                        messageView.getMessageId());
                }
                return ConsumeResult.SUCCESS;
            }, batchPolicy)
            .build();

        // Block the main thread, no need for production environment.
        Thread.sleep(Long.MAX_VALUE);
        // Close the push consumer when you don't need it anymore.
        pushConsumer.close();
    }
}

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        // Configure the batch policy using the builder with sensible defaults.
        // Messages will be accumulated and delivered in batches when any of the following is met:
        //   - 64 messages buffered (maxBatchSize)
        //   - 2 MB accumulated (maxBatchBytes)
        //   - 1 second since first buffered message (maxWaitTime)
        BatchPolicy batchPolicy = BatchPolicy.newBuilder()
            .setMaxBatchSize(64)
            .setMaxBatchBytes(2 * 1024 * 1024)
            .setMaxWaitTime(Duration.ofSeconds(1))
            .build();

        // Create a push consumer with batch message listener.
        // The listener receives a list of messages and returns a single ConsumeResult for the entire batch.
        PushConsumer pushConsumer = provider.newPushConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            .setConsumerGroup(consumerGroup)
            .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
            .setBatchMessageListener(messages -> {
                log.info("Received batch of {} messages", messages.size());
                try {
                    // Process the batch of messages together.
                    // For example, batch insert into database or batch write to file.
                    for (int i = 0; i < messages.size(); i++) {
                        log.info("  Message[{}]: messageId={}, body size={}",
                            i, messages.get(i).getMessageId(), messages.get(i).getBody().remaining());
                    }
                    return ConsumeResult.SUCCESS;
                } catch (Exception e) {
                    log.error("Failed to process batch", e);
                    // Returning FAILURE will cause:
                    // - Standard mode: each message is nacked and redelivered later by the server.
                    // - FIFO mode: the entire batch is retried as a whole until max attempts exhausted.
                    return ConsumeResult.FAILURE;
                }
            }, batchPolicy)
            .build();

        // Block the main thread, no need for production environment.
        Thread.sleep(Long.MAX_VALUE);
        // Close the push consumer when you don't need it anymore.
        pushConsumer.close();
    }
}

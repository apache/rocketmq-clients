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
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.LitePushConsumer;
import org.apache.rocketmq.client.java.exception.LiteSubscriptionQuotaExceededException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LitePushConsumerExample {
    private static final Logger log = LoggerFactory.getLogger(LitePushConsumerExample.class);

    private LitePushConsumerExample() {
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
            // On some Windows platforms, you may encounter SSL compatibility issues. Try turning off the SSL option in
            // client configuration to solve the problem please if SSL is not essential.
            // .enableSsl(false)
            .setCredentialProvider(sessionCredentialsProvider)
            .build();
        String consumerGroup = "yourConsumerGroup";
        String topic = "yourParentTopic";
        // In most case, you don't need to create too many consumers, singleton pattern is recommended.
        LitePushConsumer litePushConsumer = provider.newLitePushConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            // Set the consumer group name.
            .setConsumerGroup(consumerGroup)
            // Bind to the parent topic
            .bindTopic(topic)
            .setMessageListener(messageView -> {
                // Handle the received message and return consume result.
                log.info("Consume message={}", messageView);
                return ConsumeResult.SUCCESS;
            })
            .build();

        try {
            /*
            The subscribeLite() method initiates network requests and performs quota verification, so it may fail.
            It's important to check the result of this call to ensure that the subscription was successfully added.
            Possible failure scenarios include:
            1. Network request errors, which can be retried.
            2. Quota verification failures, indicated by LiteSubscriptionQuotaExceededException. In this case,
               evaluate whether the quota is insufficient and promptly unsubscribe from unused subscriptions
               using unsubscribeLite() to free up resources.
            */
            litePushConsumer.subscribeLite("lite-topic-1");
            litePushConsumer.subscribeLite("lite-topic-2");
            litePushConsumer.subscribeLite("lite-topic-3");
        } catch (LiteSubscriptionQuotaExceededException e) {
            // 1. Evaluate and increase the lite topic resource limit.
            // 2. Unsubscribe unused lite topics in time
            // litePushConsumer.unsubscribeLite("lite-topic-3");
            log.error("Lite subscription quota exceeded", e);
        } catch (Throwable t) {
            // should retry later
            log.error("Failed to subscribe lite topic", t);
        }

        // Block the main thread, no need for production environment.
        Thread.sleep(Long.MAX_VALUE);
        // Close the push consumer when you don't need it anymore.
        // You could close it manually or add this into the JVM shutdown hook.
        litePushConsumer.close();
    }
}

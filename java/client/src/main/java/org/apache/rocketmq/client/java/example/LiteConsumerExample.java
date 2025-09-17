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
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.LitePushConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LiteConsumerExample {
    private static final Logger log = LoggerFactory.getLogger(LiteConsumerExample.class);

    public static void main(String[] args) throws ClientException, InterruptedException, IOException {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();

        // Credential provider is optional for client configuration.
        String accessKey = "yourAccessKey";
        String secretKey = "yourSecretKey";
        SessionCredentialsProvider sessionCredentialsProvider =
            new StaticSessionCredentialsProvider(accessKey, secretKey);

        String endpoints = "127.0.0.1:8081";
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(endpoints)
            // On some Windows platforms, you may encounter SSL compatibility issues. Try turning off the SSL option in
            // client configuration to solve the problem please if SSL is not essential.
            .enableSsl(false)
            //            .setCredentialProvider(sessionCredentialsProvider)
            .build();
        AtomicInteger atomicInteger = new AtomicInteger(0);
        String tag = "yourMessageTagA";
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        String consumerGroup = "group_quan_0";
        // In most case, you don't need to create too many consumers, singleton pattern is recommended.
        LitePushConsumer litePushConsumer = provider.newLitePushConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            .bindTopic(LiteProducerExample.TOPIC)
            // Set the consumer group name.
            .setConsumerGroup(consumerGroup)
            .setMessageListener(messageView -> {
                // Handle the received message and return consume result.
                System.out.printf("[%s] [%s] Consume message=%s %n", new Date(), Thread.currentThread().getName(),
                    messageView);
                log.info("Consume message={}", messageView);
                //                try {
                //                    Thread.sleep(3000);
                //                } catch (InterruptedException e) {
                //                    throw new RuntimeException(e);
                //                }
                return ConsumeResult.SUCCESS;
            })
            .build();

        //        try {
        //            for (int i = 0; i < LiteProducerExample.LITE_TOPIC_NUM; i++) {
        //                litePushConsumer.subscribeLite(LiteProducerExample.LITE_TOPIC_PREFIX + i);
        //            }
        //        } catch (Exception e) {
        //            e.printStackTrace();
        //        }

        //        litePushConsumer.unsubscribeLite("liteTopic1");

        // Block the main thread, no need for production environment.
        Thread.sleep(Long.MAX_VALUE);
        // Close the push consumer when you don't need it anymore.
        // You could close it manually or add this into the JVM shutdown hook.
        litePushConsumer.close();
    }
}

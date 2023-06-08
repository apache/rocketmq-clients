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
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.ProducerBuilder;
import org.apache.rocketmq.client.apis.producer.TransactionChecker;

public class ProducerSingleton {
    private volatile static Producer PRODUCER;
    private static final String ACCESS_KEY = "yourAccessKey";
    private static final String SECRET_KEY = "yourSecretKey";
    private static final String ENDPOINTS = "foobar.com:8080";

    private ProducerSingleton() {
    }

    public static Producer getInstance(String... topics) throws ClientException {
        return getInstance(null, topics);
    }

    public static Producer getInstance(TransactionChecker checker, String... topics) throws ClientException {
        if (null == PRODUCER) {
            synchronized (ProducerSingleton.class) {
                if (null == PRODUCER) {
                    final ClientServiceProvider provider = ClientServiceProvider.loadService();
                    // Credential provider is optional for client configuration.
                    SessionCredentialsProvider sessionCredentialsProvider =
                        new StaticSessionCredentialsProvider(ACCESS_KEY, SECRET_KEY);
                    ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                        .setEndpoints(ENDPOINTS)
                        .setCredentialProvider(sessionCredentialsProvider)
                        .build();
                    final ProducerBuilder builder = provider.newProducerBuilder()
                        .setClientConfiguration(clientConfiguration)
                        // Set the topic name(s), which is optional but recommended. It makes producer could prefetch
                        // the topic route before message publishing.
                        .setTopics(topics);
                    if (checker != null) {
                        builder.setTransactionChecker(checker);
                    }
                    PRODUCER = builder.build();
                }
            }
        }
        return PRODUCER;
    }

    public static void shutdown() throws IOException {
        if (null != PRODUCER) {
            PRODUCER.close();
        }
    }
}

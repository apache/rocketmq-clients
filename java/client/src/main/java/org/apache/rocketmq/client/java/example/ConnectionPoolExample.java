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

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.ProducerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example demonstrating how to use connection pooling feature.
 * 
 * <p>Connection pooling allows multiple client instances to share RPC connections
 * to the same endpoints, reducing resource usage and improving efficiency when
 * you have multiple producers/consumers connecting to the same RocketMQ cluster.
 */
public class ConnectionPoolExample {
    private static final Logger log = LoggerFactory.getLogger(ConnectionPoolExample.class);
    
    private static final String ACCESS_KEY = "yourAccessKey";
    private static final String SECRET_KEY = "yourSecretKey";
    private static final String ENDPOINTS = "foobar.com:8080";
    private static final String TOPIC = "yourTopic";

    public static void main(String[] args) throws ClientException {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        
        // Example 1: Create producer WITHOUT connection pooling (default behavior)
        log.info("Creating producer without connection pooling...");
        Producer producerWithoutPool = createProducer(provider, false);
        
        // Example 2: Create producer WITH connection pooling enabled
        log.info("Creating producer with connection pooling enabled...");
        Producer producerWithPool = createProducer(provider, true);
        
        // Example 3: Create multiple producers with connection pooling
        // These will share the same underlying RPC connections
        log.info("Creating multiple producers with connection pooling...");
        Producer producer1 = createProducer(provider, true);
        Producer producer2 = createProducer(provider, true);
        Producer producer3 = createProducer(provider, true);
        
        log.info("All producers created successfully!");
        log.info("Producers with connection pooling will share RPC connections to the same endpoints.");
        
        // Clean up resources
        try {
            producerWithoutPool.close();
            producerWithPool.close();
            producer1.close();
            producer2.close();
            producer3.close();
            log.info("All producers closed successfully!");
        } catch (Exception e) {
            log.error("Error closing producers", e);
        }
    }
    
    private static Producer createProducer(ClientServiceProvider provider, boolean enableConnectionPool) 
            throws ClientException {
        
        SessionCredentialsProvider sessionCredentialsProvider =
            new StaticSessionCredentialsProvider(ACCESS_KEY, SECRET_KEY);
            
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(ENDPOINTS)
            .setCredentialProvider(sessionCredentialsProvider)
            .enableConnectionPool(enableConnectionPool)  // Enable or disable connection pooling
            .build();
            
        ProducerBuilder builder = provider.newProducerBuilder()
            .setClientConfiguration(clientConfiguration)
            .setTopics(TOPIC);
            
        return builder.build();
    }
}

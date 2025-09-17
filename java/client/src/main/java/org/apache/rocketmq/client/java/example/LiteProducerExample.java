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

import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LiteProducerExample {
    static final Logger log = LoggerFactory.getLogger(LiteProducerExample.class);
    static final String TOPIC = "topic_quan_0_0";
    static final String LITE_TOPIC_PREFIX = "liteTopic_";
    static final int LITE_TOPIC_NUM = 5;

    private LiteProducerExample() {

    }

    public static void main(String[] args) throws ClientException {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();

        final Producer producer = ProducerSingleton.getInstance(TOPIC);
        // Define your message body.
        byte[] body = "This is a LITE message for Apache RocketMQ".getBytes(StandardCharsets.UTF_8);
        String tag = "yourMessageTagA";

        for (int i = 0; i < 1; i++) {
            final Message message = provider.newMessageBuilder()
                // Set topic for the current message.
                .setTopic(TOPIC)
                //                .setTopic("quan-broker-a")
                // Message secondary classifier of message besides topic.
                .setTag(tag)
                // Key(s) of the message, another way to mark message besides message id.
                .setKeys("yourMessageKey-1ff69ada8e0e")
                // lite topic
                .setLiteTopic(LITE_TOPIC_PREFIX + (i % LITE_TOPIC_NUM))
                .setBody(body)
                .build();
            try {
                final SendReceipt sendReceipt = producer.send(message);
                log.info("Send message successfully, messageId={}", sendReceipt.getMessageId());
            } catch (Throwable t) {
                log.error("Failed to send message", t);
            }
        }

        // Close the producer when you don't need it anymore.
        // You could close it manually or add this into the JVM shutdown hook.
        // producer.close();
    }
}

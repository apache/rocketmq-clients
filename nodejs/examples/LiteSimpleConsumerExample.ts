/**
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

/**
 * LiteSimpleConsumer Example
 *
 * This example demonstrates how to use LiteSimpleConsumer to consume messages
 * from lite topics with reduced overhead.
 *
 * Key features:
 * - Lightweight subscription management
 * - Dynamic topic subscription
 * - Offset control for consumption starting point
 * - Optimized routing (only uses first readable master queue)
 */

import { LiteSimpleConsumerBuilder, BaseClientOptions } from '../src';

async function main() {
  console.log('========== LiteSimpleConsumer Example ==========');

  // Create client configuration options
  const clientConfig: BaseClientOptions = {
    endpoints: '127.0.0.1:8081',
    namespace: '',
  };

  try {
    // Create LiteSimpleConsumer using builder
    const consumer = await new LiteSimpleConsumerBuilder()
      .setClientConfiguration(clientConfig)
      .setConsumerGroup('lite-simple-consumer-group') // Note: No GID- prefix!
      .bindTopic('parent-topic')
      .setAwaitDuration(30000)
      .build();

    console.log('✓ LiteSimpleConsumer created successfully');
    console.log('  Consumer Group:', consumer.getConsumerGroup());

    // Subscribe to lite topics
    console.log('\n1. Subscribing to lite topics...');
    await consumer.subscribeLite('lite-topic-1');
    console.log('   ✓ Subscribed to lite-topic-1');

    await consumer.subscribeLite('lite-topic-2');
    console.log('   ✓ Subscribed to lite-topic-2');

    // Check subscriptions
    const topics = consumer.getLiteTopicSet();
    console.log('\n2. Current lite topic subscriptions:', Array.from(topics).join(', '));

    // Receive messages
    console.log('\n3. Receiving messages...');
    try {
      const messages = await consumer.receive(10, 60000); // max 10 messages, 60s invisible
      console.log(`   Received ${messages.length} messages`);

      for (const message of messages) {
        console.log('   - Message ID:', message.messageId);
        console.log('     Topic:', message.topic);
        console.log('     Body:', message.body.toString('utf-8'));

        // Acknowledge the message
        await consumer.ack(message);
        console.log('     ✓ Acknowledged');
      }
    } catch (error) {
      console.log('   No messages available or error occurred:', error);
    }

    // Unsubscribe from a topic
    console.log('\n4. Unsubscribing from lite-topic-1...');
    await consumer.unsubscribeLite('lite-topic-1');
    console.log('   ✓ Unsubscribed');

    // Check remaining subscriptions
    const remainingTopics = consumer.getLiteTopicSet();
    console.log('   Remaining topics:', Array.from(remainingTopics).join(', '));

    // Close consumer
    console.log('\n5. Closing consumer...');
    await consumer.close();
    console.log('   ✓ Consumer closed');

  } catch (error) {
    console.error('✗ Error:', error);
  }

  console.log('\n✓ Example completed!');
}

// Run example
main().catch(console.error);

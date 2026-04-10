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
 * LitePushConsumer Example
 *
 * This example demonstrates how to use LitePushConsumer to consume messages
 * from lite topics with reduced overhead.
 *
 * Prerequisites:
 * - RocketMQ server with lite topic support enabled
 * - A parent topic created on the server
 * - Consumer group registered on the server
 */

import {
  LitePushConsumerBuilder,
  ConsumeResult,
  OffsetOption,
  type MessageView,
} from '../src';
import { endpoints, namespace, consumerGroup } from './ProducerSingleton';

async function main() {
  console.log('========== LitePushConsumer Example ==========\n');

  // Create and start LitePushConsumer using builder pattern
  const consumer = await new LitePushConsumerBuilder()
    .setClientConfiguration({
      endpoints,
      namespace,
    })
    .setConsumerGroup(consumerGroup)
    .bindTopic('your-parent-topic') // Replace with your actual parent topic
    .setMessageListener({
      async consume(messageView: MessageView) {
        console.log('Received message:', {
          messageId: messageView.messageId,
          topic: messageView.topic,
          liteTopic: messageView.liteTopic,
          tag: messageView.tag,
          keys: messageView.keys,
          body: messageView.body.toString('utf-8'),
        });
        return ConsumeResult.SUCCESS;
      },
    })
    .setMaxCacheMessageCount(1024)
    .setMaxCacheMessageSizeInBytes(64 * 1024 * 1024) // 64MB
    .setConsumptionThreadCount(20)
    .startup();

  console.log('✓ Consumer started successfully\n');

  try {
    // Subscribe to lite topics with different offset strategies
    console.log('Subscribing to lite topics...\n');

    await consumer.subscribeLite('lite-topic-1');
    console.log('✓ Subscribed to lite-topic-1 (from last offset)');

    await consumer.subscribeLite('lite-topic-2', OffsetOption.MIN_OFFSET);
    console.log('✓ Subscribed to lite-topic-2 (from minimum offset)');

    await consumer.subscribeLite('lite-topic-3', OffsetOption.ofTailN(100));
    console.log('✓ Subscribed to lite-topic-3 (last 100 messages)\n');

    // Check current subscriptions
    const topics = consumer.getLiteTopicSet();
    console.log(`Current subscriptions (${topics.size} topics):`);
    topics.forEach(topic => console.log(`  - ${topic}`));
    console.log();

    // Keep running to receive messages
    console.log('Consumer is running. Press Ctrl+C to exit...\n');
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    await new Promise(() => {});
  } catch (error) {
    console.error('Error during consumption:', error);
    throw error;
  } finally {
    // Close consumer to release resources
    console.log('\nShutting down consumer...');
    await consumer.close();
    console.log('✓ Consumer closed successfully');
  }
}

// Run example
main().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});

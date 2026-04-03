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
 * Key features:
 * - Lightweight subscription management
 * - Dynamic topic subscription
 * - Offset control for consumption starting point
 */

import { OffsetOption } from '../src';

async function main() {
  console.log('========== LitePushConsumer Example ==========');

  // Note: This is a conceptual example showing the API usage.
  // Actual implementation requires server-side support for lite topics.

  console.log('\n1. Creating OffsetOptions for different consumption strategies...');

  // Consume from the last offset (most recent messages)
  const lastOffset = OffsetOption.LAST_OFFSET;
  console.log('   LAST_OFFSET:', lastOffset.toString());

  // Consume from the minimum offset (oldest messages)
  const minOffset = OffsetOption.MIN_OFFSET;
  console.log('   MIN_OFFSET:', minOffset.toString());

  // Consume from the maximum offset (newest messages after subscription)
  const maxOffset = OffsetOption.MAX_OFFSET;
  console.log('   MAX_OFFSET:', maxOffset.toString());

  // Consume from a specific offset
  const customOffset = OffsetOption.ofOffset(1000);
  console.log('   Custom offset (1000):', customOffset.toString());

  // Consume from the last N messages
  const tailN = OffsetOption.ofTailN(100);
  console.log('   Last 100 messages:', tailN.toString());

  // Consume from a specific timestamp
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const timestampOffset = OffsetOption.ofTimestamp(yesterday.getTime());
  console.log('   From yesterday:', timestampOffset.toString());

  console.log('\n2. LitePushConsumer API overview:');
  console.log('   - subscribeLite(liteTopic: string): Subscribe to a lite topic');
  console.log('   - subscribeLite(liteTopic, offsetOption): Subscribe with offset control');
  console.log('   - unsubscribeLite(liteTopic): Unsubscribe from a lite topic');
  console.log('   - getLiteTopicSet(): Get all subscribed lite topics');
  console.log('   - getConsumerGroup(): Get consumer group name');
  console.log('   - close(): Close the consumer');

  console.log('\n3. Example usage pattern:');
  console.log(`
  // Create LitePushConsumer using builder
  try {
    const consumer = await new LitePushConsumerBuilder()
      .setClientConfiguration(clientConfig)
      .setConsumerGroup('your-consumer-group')
      .bindTopic('your-parent-topic')
      .setMessageListener(async (messages) => {
        for (const message of messages) {
          console.log('Received:', message);
          // Process message...
        }
      })
      .setMaxCacheMessageCount(1024)
      .setMaxCacheMessageSizeInBytes(64 * 1024 * 1024)
      .setConsumptionThreadCount(20)
      .build();
    
    // Subscribe to lite topics
    await consumer.subscribeLite('lite-topic-1');
    await consumer.subscribeLite('lite-topic-2', OffsetOption.MIN_OFFSET);
    
    // Check subscriptions
    const topics = consumer.getLiteTopicSet();
    console.log('Subscribed topics:', topics);
    
    // Unsubscribe when done
    await consumer.unsubscribeLite('lite-topic-1');
    
    // Close consumer
    await consumer.close();
  } catch (error) {
    console.error('Failed to create or use LitePushConsumer:', error);
  }
  `);

  console.log('\n✓ Example completed!');
  console.log('\nNote: Full LitePushConsumer implementation requires:');
  console.log('  - Server-side lite topic support');
  console.log('  - LiteSubscriptionManager integration');
  console.log('  - RPC methods for syncLiteSubscription');
  console.log('  - Quota management and error handling\n');
}

// Run example
main().catch(console.error);

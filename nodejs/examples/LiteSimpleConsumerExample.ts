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
 * This example demonstrates how to use LiteSimpleConsumer to pull messages from
 * lite topics with explicit receive / ack control.
 *
 * Prerequisites:
 * - RocketMQ proxy plus a broker with lite support enabled
 *   (broker.conf: enableLmq=true, enableMultiDispatch=true)
 * - A parent topic created with message.type=LITE
 * - A consumer group created with the attribute +lite.bind.topic=<parentTopic>
 * - Messages published to the parent topic with the liteTopic property set,
 *   see LiteProducerExample.ts
 */

import { LiteSimpleConsumerBuilder, OffsetOption, type MessageView } from '../src';
import { endpoints, namespace, consumerGroup, sessionCredentials, liteTopicConfig } from './ProducerSingleton';

const BIND_TOPIC = liteTopicConfig.parentTopic;
const INVISIBLE_DURATION = 15000; // 15s, the visibility timeout of received messages

async function main() {
  console.log('========== LiteSimpleConsumer Example ==========\n');

  // Build and start the lite simple consumer, it binds to one parent topic only.
  const consumer = await new LiteSimpleConsumerBuilder()
    .setClientConfiguration({
      endpoints,
      namespace,
      sessionCredentials,
    })
    .setConsumerGroup(consumerGroup)
    .bindTopic(BIND_TOPIC) // Replace with your actual parent topic
    .setAwaitDuration(5000) // Long-polling timeout of receive()
    .build();

  console.log(`✓ Consumer started, bindTopic=${BIND_TOPIC}, group=${consumerGroup}\n`);

  let running = true;
  process.on('SIGINT', () => {
    console.log('\nReceived SIGINT, stopping...');
    running = false;
  });

  try {
    // Subscribe to lite topics. All of them must belong to the bound parent topic.
    console.log('Subscribing to lite topics...\n');

    await consumer.subscribeLite('lite-topic-1', OffsetOption.MIN_OFFSET);
    console.log('✓ Subscribed to lite-topic-1 (from the minimum offset)');

    await consumer.subscribeLite('lite-topic-2');
    console.log('✓ Subscribed to lite-topic-2 (from the latest offset)');

    await consumer.subscribeLite('lite-topic-3', OffsetOption.ofTailN(100));
    console.log('✓ Subscribed to lite-topic-3 (the last 100 messages)\n');

    console.log('Current lite topic set:', [ ...consumer.getLiteTopicSet() ], '\n');
    console.log('Receiving messages. Press Ctrl+C to exit...\n');

    // Pull messages by batch, then ack them one by one.
    while (running) {
      const messages: MessageView[] = await consumer.receive(16, INVISIBLE_DURATION);
      if (messages.length === 0) {
        continue;
      }

      for (const message of messages) {
        console.log('Received message:', {
          messageId: message.messageId,
          topic: message.topic,
          liteTopic: message.liteTopic,
          tag: message.tag,
          keys: message.keys,
          body: message.body.toString('utf-8'),
        });

        try {
          // Ack as soon as the business logic succeeds, otherwise the message
          // becomes visible again after the invisible duration expires.
          await consumer.ack(message);
          console.log(`✓ Acked message ${message.messageId}\n`);
        } catch (error) {
          console.error(`✗ Failed to ack message ${message.messageId}:`, error);

          // Not ready to process it yet? Extend the invisible duration instead
          // of dropping the message.
          await consumer.changeInvisibleDuration(message, INVISIBLE_DURATION);
        }
      }
    }

    // Release the subscriptions that are no longer needed, this frees the lite
    // topic quota of the consumer group.
    console.log('\nUnsubscribing from lite-topic-3...');
    await consumer.unsubscribeLite('lite-topic-3');
    console.log('✓ Unsubscribed, remaining lite topic set:', [ ...consumer.getLiteTopicSet() ], '\n');
  } catch (error) {
    console.error('Error during consumption:', error);
    throw error;
  } finally {
    console.log('Shutting down consumer...');
    await consumer.close();
    console.log('✓ Consumer closed successfully');
  }
}

main().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});

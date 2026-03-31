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
 * This example demonstrates how to send priority messages using Apache RocketMQ.
 *
 * Priority messages allow you to assign different priority levels to messages,
 * ensuring that higher priority messages are processed before lower priority ones.
 *
 * Key points:
 * - Priority is a non-negative number (0, 1, 2, ...)
 * - Higher priority messages are delivered first
 * - Priority cannot be set together with deliveryTimestamp or messageGroup
 * - Messages without priority are treated as normal priority
 */

import { Producer } from '../src/producer/Producer';
import { Message } from '../src/message/Message';

// Replace with your actual configuration
const ENDPOINT = process.env.ROCKETMQ_ENDPOINT || 'yourEndpoint';
const ACCESS_KEY = process.env.ROCKETMQ_ACCESS_KEY || 'yourAccessKey';
const SECRET_KEY = process.env.ROCKETMQ_SECRET_KEY || 'yourSecretKey';

async function main() {
  // Create a producer instance
  const producer = new Producer({
    namespace: process.env.ROCKETMQ_NAMESPACE || 'yourNamespace',
    endpoints: ENDPOINT,
    sessionCredentials: {
      accessKey: ACCESS_KEY,
      accessSecret: SECRET_KEY,
    },
    topics: [ 'yourPriorityTopic' ],
    requestTimeout: 3000,
  });

  try {
    // Start the producer
    await producer.startup();
    console.log('Producer started successfully');

    // Define message body
    const body = Buffer.from('This is a priority message for Apache RocketMQ', 'utf-8');
    const tag = 'yourMessageTagA';

    // Send high priority message (priority = 1, highest)
    const highPriorityMessage = new Message({
      topic: 'yourPriorityTopic',
      body,
      tag,
      keys: [ 'high-priority-key' ],
      priority: 1, // High priority
    });

    const highPriorityReceipt = await producer.send(highPriorityMessage);
    console.log('[High Priority] Message sent successfully, messageId=%s', highPriorityReceipt.messageId);

    // Send medium priority message (priority = 5)
    const mediumPriorityMessage = new Message({
      topic: 'yourPriorityTopic',
      body: Buffer.from('Medium priority message', 'utf-8'),
      tag,
      keys: [ 'medium-priority-key' ],
      priority: 5, // Medium priority
    });

    const mediumPriorityReceipt = await producer.send(mediumPriorityMessage);
    console.log('[Medium Priority] Message sent successfully, messageId=%s', mediumPriorityReceipt.messageId);

    // Send low priority message (priority = 10)
    const lowPriorityMessage = new Message({
      topic: 'yourPriorityTopic',
      body: Buffer.from('Low priority message', 'utf-8'),
      tag,
      keys: [ 'low-priority-key' ],
      priority: 10, // Low priority
    });

    const lowPriorityReceipt = await producer.send(lowPriorityMessage);
    console.log('[Low Priority] Message sent successfully, messageId=%s', lowPriorityReceipt.messageId);

    // Send normal message (no priority specified)
    const normalMessage = new Message({
      topic: 'yourPriorityTopic',
      body: Buffer.from('Normal priority message', 'utf-8'),
      tag,
      keys: [ 'normal-key' ],
    });

    const normalReceipt = await producer.send(normalMessage);
    console.log('[Normal Priority] Message sent successfully, messageId=%s', normalReceipt.messageId);

  } catch (error) {
    console.error('Failed to send priority messages:', error);
  } finally {
    // Close the producer when done
    await producer.shutdown();
    console.log('Producer closed');
  }
}

// Run the example
main().catch(console.error);

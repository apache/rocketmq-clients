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
 * Producer with Lite Message Example
 *
 * This example demonstrates how to send lite messages that are optimized
 * for lightweight scenarios with reduced metadata and storage overhead.
 *
 * Key points for lite messages:
 * - Lite topic reduces storage and network overhead
 * - Cannot be used with transactional messages
 * - Suitable for high-throughput, low-value data
 */
import { Producer } from '../src';
import { endpoints, sessionCredentials, namespace } from './ProducerSingleton';

async function main() {
  console.log('========== Producer Lite Message Example ==========');

  // Create producer instance
  const producer = new Producer({
    namespace,
    endpoints,
    sessionCredentials,
    maxAttempts: 3,
  });

  try {
    await producer.startup();
    console.log('Producer started successfully!\n');

    // Send lite message
    console.log('Sending lite message...');
    const liteMessageReceipt = await producer.send({
      topic: 'yourLiteTopic',
      tag: 'lite-event',
      body: Buffer.from(JSON.stringify({
        type: 'LITE_MESSAGE',
        content: 'This is a lite message with reduced overhead',
        timestamp: Date.now(),
      })),
      liteTopic: 'lite-topic-name', // Specify lite topic for reduced overhead
    });
    console.log('✓ Lite message sent:', liteMessageReceipt.messageId);

    console.log('\n✓ Lite message sent successfully!');
    console.log('\nNote: Lite messages are optimized for:');
    console.log('- High-throughput scenarios');
    console.log('- Reduced storage overhead');
    console.log('- Lower network bandwidth consumption\n');

  } catch (error) {
    console.error('Failed to send lite message:', error);
    throw error;
  } finally {
    await producer.shutdown();
  }
}

// Run example
main().catch(console.error);

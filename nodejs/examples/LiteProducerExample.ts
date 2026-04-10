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

import { Producer, LiteTopicQuotaExceededException } from '../src';
import { endpoints, namespace } from './ProducerSingleton';

// Configuration
const config = {
  topic: 'yourParentTopic', // Parent topic (bind topic)
};

async function main() {
  console.log('=== Lite Producer Example ===\n');

  // Create producer
  const producer = new Producer({
    endpoints,
    namespace,
  });

  try {
    // Start producer
    await producer.startup();
    console.log('Producer started successfully\n');

    // Define message body
    const body = Buffer.from('This is a lite message for Apache RocketMQ');

    // Send Lite Topic messages
    for (let i = 1; i <= 5; i++) {
      const liteTopicName = `lite-topic-${i}`;
      const message = {
        topic: config.topic,
        keys: [ `key-${i}` ],
        body,
        liteTopic: liteTopicName, // Set lite topic
      };

      try {
        const sendReceipt = await producer.send(message);
        console.log(`✓ Message ${i} sent successfully`);
        console.log(`  - Topic: ${config.topic}`);
        console.log(`  - Lite Topic: ${liteTopicName}`);
        console.log(`  - Message ID: ${sendReceipt.messageId}\n`);
      } catch (error) {
        if (error instanceof LiteTopicQuotaExceededException) {
          // Lite topic quota exceeded.
          // Evaluate and increase the lite topic resource limit.
          console.error(`✗ Lite topic quota exceeded for ${liteTopicName}:`, error.message);
        } else {
          console.error(`✗ Failed to send message ${i}:`, error.message);
        }
      }

      // Wait 1 second between sends
      await new Promise(resolve => setTimeout(resolve, 1000));
    }

    console.log('All messages sent!\n');
  } catch (error) {
    console.error('Error:', error);
  } finally {
    // Shutdown producer
    if (producer) {
      await producer.shutdown();
      console.log('Producer shutdown successfully');
    }
  }
}

main().catch(err => {
  console.error('Program error:', err);
  process.exit(1);
});

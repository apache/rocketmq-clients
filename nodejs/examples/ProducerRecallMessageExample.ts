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
 * This example demonstrates how to send delay messages and recall them before delivery.
 *
 * Key points:
 * 1. Send a delay message with a future delivery timestamp
 * 2. Get the recallHandle from SendReceipt
 * 3. Call producer.recallMessage() with topic and recallHandle before the message is delivered
 * 4. The message will be canceled and won't be delivered to consumers
 */

import { Producer } from '../src';
import { topics, endpoints, sessionCredentials, namespace } from './ProducerSingleton';

(async () => {
  const producer = new Producer({
    endpoints,
    namespace,
    sessionCredentials,
    maxAttempts: 2,
  });
  try {
    await producer.startup();
    console.log('Producer started successfully');

    // Send delay messages
    const sendReceipts: any[] = [];
    for (let i = 0; i < 5; i++) {
      const deliveryTimestamp = new Date(Date.now() + 10000); // 10秒后投递，有足够时间撤回
      const receipt = await producer.send({
        topic: topics.delay,
        tag: 'recall-test',
        deliveryTimestamp,
        body: Buffer.from(JSON.stringify({
          id: i,
          message: `Delay message ${i} - will be recalled`,
          timestamp: Date.now(),
        })),
      });
      console.log(`Sent delay message ${i}:`, {
        messageId: receipt.messageId,
        recallHandle: receipt.recallHandle,
      });
      sendReceipts.push(receipt);
    }

    console.log('\nAll delay messages sent, now attempting to recall them...\n');

    // Wait a bit before recalling (optional)
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Recall all messages before they are delivered
    for (let i = 0; i < sendReceipts.length; i++) {
      const receipt = sendReceipts[i];
      try {
        const recallReceipt = await producer.recallMessage(topics.delay, receipt.recallHandle);
        console.log(`✓ Message ${i} recalled successfully:`, {
          originalMessageId: receipt.messageId,
          recalledMessageId: recallReceipt.messageId,
        });
      } catch (error) {
        console.error(`✗ Failed to recall message ${i}:`, (error as Error).message);
      }
    }

    console.log('\nRecall operation completed');
    // Keep the producer running for a while to see if any messages get delivered
    console.log('\nWaiting 15 seconds to verify no messages were delivered...');
    await new Promise(resolve => setTimeout(resolve, 15000));
  } catch (err) {
    console.error('Error occurred:', err);
  } finally {
    await producer.shutdown();
    console.log('Producer shutdown completed');
  }
})();

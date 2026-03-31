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
 * Producer with FIFO Message Example
 * 
 * This example demonstrates how to send FIFO (First-In-First-Out) messages
 * that will be consumed in order by PushConsumer with FIFO support.
 * 
 * Key points for FIFO messages:
 * - Messages with the same messageGroup are stored and delivered in order
 * - Different messageGroups can be consumed concurrently
 * - Message group is required for FIFO ordering
 */
import { Producer } from '../src';
import { topics, endpoints, sessionCredentials, namespace } from './ProducerSingleton';

async function main() {
  console.log('========== Producer FIFO Message Example ==========');

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

    // Send FIFO messages with different message groups
    // Messages within the same group will be consumed in strict order
    
    // Group 1: Order processing sequence
    console.log('Sending FIFO messages for Group 1 (Order-001)...');
    await sendFifoMessages(producer, 'Order-001', [
      { action: 'CREATE', orderId: '001', amount: 100 },
      { action: 'PAY', orderId: '001', paymentId: 'P001' },
      { action: 'SHIP', orderId: '001', trackingNo: 'T001' },
      { action: 'COMPLETE', orderId: '001' },
    ]);

    // Group 2: Another order (can be processed in parallel with Group 1)
    console.log('\nSending FIFO messages for Group 2 (Order-002)...');
    await sendFifoMessages(producer, 'Order-002', [
      { action: 'CREATE', orderId: '002', amount: 200 },
      { action: 'PAY', orderId: '002', paymentId: 'P002' },
      { action: 'CANCEL', orderId: '002', reason: 'Customer request' },
    ]);

    // Group 3: Account updates (must maintain sequence)
    console.log('\nSending FIFO messages for Group 3 (Account-Alice)...');
    await sendFifoMessages(producer, 'Account-Alice', [
      { type: 'DEPOSIT', accountId: 'Alice', amount: 1000, balance: 1000 },
      { type: 'WITHDRAW', accountId: 'Alice', amount: 200, balance: 800 },
      { type: 'TRANSFER', accountId: 'Alice', amount: 100, balance: 700 },
    ]);

    // Send some messages without group (will be consumed concurrently)
    console.log('\nSending non-FIFO messages (no messageGroup)...');
    for (let i = 0; i < 3; i++) {
      const receipt = await producer.send({
        topic: topics.fifo,
        tag: 'non-fifo',
        body: Buffer.from(JSON.stringify({
          type: 'NON_FIFO',
          index: i,
          timestamp: Date.now(),
          note: 'This message has no messageGroup, will be consumed concurrently',
        })),
      });
      console.log(`Non-FIFO message ${i} sent:`, receipt.messageId);
    }

    console.log('\n✓ All FIFO messages sent successfully!');
    console.log('\nNote: Start PushConsumerFifoMessageExample to consume these messages.');
    console.log('Messages with the same messageGroup will be consumed in strict order.\n');

  } catch (error) {
    console.error('Failed to send FIFO messages:', error);
    throw error;
  } finally {
    await producer.shutdown();
  }
}

// Helper function to send a sequence of FIFO messages
async function sendFifoMessages(
  producer: Producer,
  messageGroup: string,
  messages: Array<Record<string, any>>
) {
  console.log(`\n  Sending ${messages.length} messages to group "${messageGroup}"...`);
  
  for (let i = 0; i < messages.length; i++) {
    const messageData = messages[i];
    
    const receipt = await producer.send({
      topic: topics.fifo,
      tag: 'fifo-message',
      body: Buffer.from(JSON.stringify({
        sequence: i + 1,
        messageGroup,
        timestamp: Date.now(),
        ...messageData,
      })),
      messageGroup, // Required for FIFO ordering
    });
    
    console.log(`    [${i + 1}/${messages.length}] Sent:`, {
      messageId: receipt.messageId,
      sequence: i + 1,
      action: messageData.action || messageData.type,
    });

    // Small delay between messages to ensure clear sequencing
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  
  console.log(`  ✓ Completed sending ${messages.length} messages for group "${messageGroup}"`);
}

// Run example
main().catch(console.error);
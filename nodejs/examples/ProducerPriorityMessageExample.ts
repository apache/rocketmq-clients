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
 * Producer with Priority Message Example
 * 
 * This example demonstrates how to send priority messages that will be
 * consumed based on their priority level. Higher priority messages are
 * delivered before lower priority ones.
 * 
 * Key points for priority messages:
 * - Priority must be >= 0
 * - Higher priority messages are delivered first
 * - Cannot be used with messageGroup, deliveryTimestamp, or liteTopic
 * - Transactional messages cannot have priority set
 */
import { Producer } from '../src';
import { endpoints, sessionCredentials, namespace } from './ProducerSingleton';

async function main() {
  console.log('========== Producer Priority Message Example ==========');

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

    // Send messages with different priority levels
    console.log('Sending priority messages...');
    
    // Low priority message (priority = 1)
    console.log('\n1. Sending LOW priority message (priority=1)...');
    const lowPriorityReceipt = await producer.send({
      topic: 'yourPriorityTopic',
      tag: 'low-priority',
      body: Buffer.from(JSON.stringify({
        type: 'LOW_PRIORITY',
        content: 'This is a low priority notification',
        timestamp: Date.now(),
      })),
      priority: 1, // Lower number = lower priority
    });
    console.log('   ✓ Low priority message sent:', lowPriorityReceipt.messageId);

    // Medium priority message (priority = 5)
    console.log('\n2. Sending MEDIUM priority message (priority=5)...');
    const mediumPriorityReceipt = await producer.send({
      topic: 'yourPriorityTopic',
      tag: 'medium-priority',
      body: Buffer.from(JSON.stringify({
        type: 'MEDIUM_PRIORITY',
        content: 'This is a medium priority alert',
        timestamp: Date.now(),
      })),
      priority: 5, // Medium priority
    });
    console.log('   ✓ Medium priority message sent:', mediumPriorityReceipt.messageId);

    // High priority message (priority = 10)
    console.log('\n3. Sending HIGH priority message (priority=10)...');
    const highPriorityReceipt = await producer.send({
      topic: 'yourPriorityTopic',
      tag: 'high-priority',
      body: Buffer.from(JSON.stringify({
        type: 'HIGH_PRIORITY',
        content: 'This is a high priority urgent alert',
        timestamp: Date.now(),
      })),
      priority: 10, // Higher number = higher priority
    });
    console.log('   ✓ High priority message sent:', highPriorityReceipt.messageId);

    // Critical priority message (priority = 100)
    console.log('\n4. Sending CRITICAL priority message (priority=100)...');
    const criticalReceipt = await producer.send({
      topic: 'yourPriorityTopic',
      tag: 'critical-priority',
      body: Buffer.from(JSON.stringify({
        type: 'CRITICAL_PRIORITY',
        content: 'This is a critical emergency notification',
        timestamp: Date.now(),
      })),
      priority: 100, // Very high priority
    });
    console.log('   ✓ Critical priority message sent:', criticalReceipt.messageId);

    console.log('\n✓ All priority messages sent successfully!');
    console.log('\nNote: Messages will be delivered to consumers based on priority.');
    console.log('Higher priority messages (e.g., priority=100) will be delivered');
    console.log('before lower priority ones (e.g., priority=1).\n');

    // Demonstrate mutual exclusivity constraints
    console.log('Demonstrating mutual exclusivity constraints...\n');
    
    try {
      // This will fail: priority + messageGroup
      await producer.send({
        topic: 'yourPriorityTopic',
        body: Buffer.from('test'),
        priority: 5,
        messageGroup: 'group-1',
      });
      console.log('✗ ERROR: Should have failed when setting both priority and messageGroup');
    } catch (error: any) {
      console.log('✓ Correctly rejected: priority + messageGroup');
      console.log(`  Error: ${error.message}\n`);
    }

    try {
      // This will fail: priority + deliveryTimestamp
      await producer.send({
        topic: 'yourPriorityTopic',
        body: Buffer.from('test'),
        priority: 5,
        deliveryTimestamp: new Date(Date.now() + 60000),
      });
      console.log('✗ ERROR: Should have failed when setting both priority and deliveryTimestamp');
    } catch (error: any) {
      console.log('✓ Correctly rejected: priority + deliveryTimestamp');
      console.log(`  Error: ${error.message}\n`);
    }

    try {
      // This will fail: negative priority
      await producer.send({
        topic: 'yourPriorityTopic',
        body: Buffer.from('test'),
        priority: -1,
      });
      console.log('✗ ERROR: Should have failed with negative priority');
    } catch (error: any) {
      console.log('✓ Correctly rejected: negative priority');
      console.log(`  Error: ${error.message}\n`);
    }

    console.log('All validation checks passed!\n');

  } catch (error) {
    console.error('Failed to send priority messages:', error);
    throw error;
  } finally {
    await producer.shutdown();
  }
}

// Run example
main().catch(console.error);

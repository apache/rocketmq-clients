/**
 * PushConsumer with FIFO Message Example
 *
 * This example demonstrates how to use PushConsumer to consume FIFO (First-In-First-Out) messages.
 * FIFO messages are consumed in order within the same message group.
 *
 * Key features of FIFO consumption:
 * - Messages with the same messageGroup are consumed in strict order
 * - Messages without messageGroup are consumed concurrently
 * - Failed messages are retried before proceeding to next message
 */

import { PushConsumer, ConsumeResult } from '../src';
import type { MessageView } from '../src';

// Get configuration from environment variables
const ACCESS_KEY = process.env.ROCKETMQ_ACCESS_KEY || 'yourAccessKey';
const SECRET_KEY = process.env.ROCKETMQ_SECRET_KEY || 'yourSecretKey';
const ENDPOINT = process.env.ROCKETMQ_ENDPOINT || 'localhost:8080';

async function main() {
  console.log('========== PushConsumer with FIFO Message Example ==========');

  // Track processing statistics per message group
  const groupStats = new Map<string, { processed: number; lastMessageId: string }>();

  // 1. Define FIFO message listener
  const messageListener = {
    async consume(messageView: MessageView): Promise<ConsumeResult> {
      const messageGroup = messageView.messageGroup || 'NO_GROUP';

      console.log('\n[FIFO] Received message:', {
        messageId: messageView.messageId,
        topic: messageView.topic,
        tag: messageView.tag,
        keys: messageView.keys,
        messageGroup,
        body: messageView.body.toString('utf-8'),
        deliveryAttempt: messageView.deliveryAttempt,
        bornTimestamp: messageView.bornTimestamp,
      });

      // Simulate business processing
      try {
        // Update statistics
        const stats = groupStats.get(messageGroup) || { processed: 0, lastMessageId: '' };
        stats.processed++;
        stats.lastMessageId = messageView.messageId;
        groupStats.set(messageGroup, stats);

        // Process message based on content
        await doFifoBusinessLogic(messageView);

        console.log(`[FIFO] ✓ Message processed successfully, group=${messageGroup}, total=${stats.processed}`);

        // Return success to indicate message has been consumed successfully
        return ConsumeResult.SUCCESS;
      } catch (error) {
        console.error(`[FIFO] ✗ Failed to process message, group=${messageGroup}:`, error);
        // Return failure, message will be retried (up to maxAttempts)
        // Next message in the group won't be consumed until this one succeeds
        return ConsumeResult.FAILURE;
      }
    },
  };

  // 2. Configure PushConsumer for FIFO consumption
  const pushConsumer = new PushConsumer({
    // Basic configuration
    namespace: process.env.ROCKETMQ_NAMESPACE || 'yourNamespace',
    endpoints: ENDPOINT,

    // Authentication credentials (optional)
    sessionCredentials: {
      accessKey: ACCESS_KEY,
      accessSecret: SECRET_KEY,
    },

    // Consumer group configuration
    // IMPORTANT: Use a dedicated consumer group for FIFO consumption
    consumerGroup: 'yourFifoConsumerGroup',

    // Subscription configuration
    // FIFO consumption is enabled when topic receives FIFO messages
    subscriptions: new Map([
      [ 'yourFifoTopic', '*' ], // Subscribe to FIFO topic
    ]),

    // Message listener
    messageListener,

    // Cache configuration
    // For FIFO, smaller cache can help maintain order better
    maxCacheMessageCount: 512, // Reduced from default 1024
    maxCacheMessageSizeInBytes: 33554432, // 32MB (reduced from 64MB)

    // Long polling timeout configuration
    longPollingTimeout: 30000,

    // Request timeout configuration
    requestTimeout: 3000,
  });

  try {
    // 3. Start consumer
    console.log('\nStarting FIFO PushConsumer...');
    await pushConsumer.startup();
    console.log('FIFO PushConsumer started successfully!');
    console.log('Client ID:', pushConsumer.getClientId());
    console.log('Consumer Group:', pushConsumer.getConsumerGroup());
    console.log('\nWaiting for FIFO messages...');
    console.log('Messages with the same messageGroup will be consumed in strict order.\n');
    // 4. Monitor statistics (optional)
    const statsInterval = setInterval(() => {
      if (groupStats.size > 0) {
        console.log('\n--- FIFO Consumption Statistics ---');
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        for (const [ group, stats ] of groupStats.entries()) {
          console.log(`Group "${group}": ${stats.processed} messages, last=${stats.lastMessageId}`);
        }
        console.log('-----------------------------------\n');
      }
    }, 10000); // Print stats every 10 seconds

    // Keep program running, waiting for messages
    console.log('Press Ctrl+C to exit...\n');

    // Graceful shutdown handling
    process.on('SIGINT', async () => {
      console.log('\nShutting down FIFO PushConsumer...');
      clearInterval(statsInterval);
      await shutdown(pushConsumer);
      process.exit(0);
    });

    // Keep program running
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    await new Promise(() => {});
  } catch (error) {
    console.error('Failed to start FIFO PushConsumer:', error);
    await shutdown(pushConsumer);
    process.exit(1);
  }
}

// Business logic processing for FIFO messages
async function doFifoBusinessLogic(messageView: MessageView): Promise<void> {
  // Simulate processing time
  await new Promise(resolve => setTimeout(resolve, 50));

  // Parse message content
  const content = messageView.body.toString('utf-8');

  // Implement your FIFO-specific business logic here
  // Examples:
  // - Order processing (must process in order)
  // - Account balance updates (sequential consistency required)
  // - State machine transitions (order matters)
  // - Log replay (must maintain sequence)

  console.log(`  Processing FIFO message: ${content.substring(0, 50)}...`);

  // Example: Validate message sequence
  // You can add your own sequence validation logic here
  const messageGroup = messageView.messageGroup || 'NO_GROUP';
  const sequenceNumber = extractSequenceNumber(content);
  if (sequenceNumber !== null) {
    console.log(`  Sequence number: ${sequenceNumber} (group: ${messageGroup})`);
  }
}

// Helper function to extract sequence number from message content
function extractSequenceNumber(content: string): number | null {
  try {
    const data = JSON.parse(content);
    return typeof data.sequence === 'number' ? data.sequence : null;
  } catch {
    return null;
  }
}

// Gracefully shutdown consumer
async function shutdown(pushConsumer: PushConsumer) {
  try {
    await pushConsumer.shutdown();
    console.log('FIFO PushConsumer has been closed');
  } catch (error) {
    console.error('Error occurred while closing FIFO PushConsumer:', error);
  }
}

// Run example
main().catch(console.error);

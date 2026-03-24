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
 * PushConsumer Example
 *
 * This example demonstrates how to use PushConsumer to consume messages.
 * PushConsumer is a push-mode consumer that actively pulls messages from the server
 * and pushes them to the listener for processing.
 */

import { PushConsumer, ConsumeResult } from '../src';
import type { MessageView } from '../src';

// Get configuration from environment variables
const ACCESS_KEY = process.env.ROCKETMQ_ACCESS_KEY || 'yourAccessKey';
const SECRET_KEY = process.env.ROCKETMQ_SECRET_KEY || 'yourSecretKey';
const ENDPOINT = process.env.ROCKETMQ_ENDPOINT || 'localhost:8080';

async function main() {
  console.log('========== PushConsumer Example ==========');

  // 1. Define message listener
  const messageListener = {
    async consume(messageView: MessageView): Promise<ConsumeResult> {
      // Process received messages here
      console.log('Received message:', {
        messageId: messageView.messageId,
        topic: messageView.topic,
        tag: messageView.tag,
        keys: messageView.keys,
        body: messageView.body.toString('utf-8'),
        deliveryAttempt: messageView.deliveryAttempt,
      });

      // Simulate business processing
      try {
        // TODO: Add your business logic here
        await doBusinessLogic(messageView);
        // Return success to indicate message has been consumed successfully
        return ConsumeResult.SUCCESS;
      } catch (error) {
        console.error('Failed to process message:', error);
        // Return failure, message will be retried
        return ConsumeResult.FAILURE;
      }
    },
  };

  // 2. Configure PushConsumer
  const pushConsumer = new PushConsumer({
    // Basic configuration
    namespace: process.env.ROCKETMQ_NAMESPACE || 'yourNamespace', // Namespace
    endpoints: ENDPOINT,
    // Authentication credentials (optional)
    sessionCredentials: {
      accessKey: ACCESS_KEY,
      accessSecret: SECRET_KEY,
      // securityToken: 'yourSecurityToken', // SecurityToken, optional
    },
    // Consumer group configuration
    consumerGroup: 'yourConsumerGroup',
    // Subscription configuration: Map<topic, filterExpression>
    // filterExpression can be a string (TAG expression) or FilterExpression object
    subscriptions: new Map([
      [ 'yourTopic1', '*' ],
      // ['yourTopic3', new FilterExpression('yourSqlExpression', FilterType.SQL92)],
    ]),
    // Message listener
    messageListener,
    // Cache configuration (optional)
    maxCacheMessageCount: 1024,
    maxCacheMessageSizeInBytes: 67108864, // Max cached bytes per queue (64MB), default 64MB
    // Long polling timeout configuration (optional)
    longPollingTimeout: 30000,
    // Request timeout configuration (optional)
    requestTimeout: 3000,
    // Logger configuration (optional)
    // logger: yourCustomLogger,        // Custom logger
  });

  try {
    // 3. Start consumer
    console.log('Starting PushConsumer...');
    await pushConsumer.startup();
    console.log('PushConsumer started successfully!');
    console.log('Client ID:', pushConsumer.getClientId());
    console.log('Consumer Group:', pushConsumer.getConsumerGroup());

    // 4. Dynamic subscription (optional)
    // Can add new subscriptions at runtime
    // import { FilterExpression } from '../src';
    // await pushConsumer.subscribe('newTopic', new FilterExpression('TagC'));

    // 5. Unsubscribe (optional)
    // pushConsumer.unsubscribe('yourTopic1');

    // Keep program running, waiting for messages
    console.log('\nPress Ctrl+C to exit...');
    // Graceful shutdown handling
    process.on('SIGINT', async () => {
      console.log('\nShutting down PushConsumer...');
      await shutdown(pushConsumer);
      process.exit(0);
    });

    // Keep program running
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    await new Promise(() => {});
  } catch (error) {
    console.error('Failed to start PushConsumer:', error);
    await shutdown(pushConsumer);
    process.exit(1);
  }
}

// Business logic processing function example
// eslint-disable-next-line @typescript-eslint/no-unused-vars
async function doBusinessLogic(_messageView: MessageView): Promise<void> {
  // Simulate asynchronous business processing
  await new Promise(resolve => setTimeout(resolve, 100));
  // Implement your business logic here
  // For example:
  // - Parse message content
  // - Call database
  // - Call external API
  // - Send notifications, etc.
}

// Gracefully shutdown consumer
async function shutdown(pushConsumer: PushConsumer) {
  try {
    await pushConsumer.shutdown();
    console.log('PushConsumer has been closed');
  } catch (error) {
    console.error('Error occurred while closing PushConsumer:', error);
  }
}

// Run example
main().catch(console.error);

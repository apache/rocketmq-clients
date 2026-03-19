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
 * PushConsumer Quick Start Example
 * 
 * The simplest way to use PushConsumer
 */

import { PushConsumer, ConsumeResult, type MessageView } from '../src';

async function quickStart() {
  // Create PushConsumer instance
  const pushConsumer = new PushConsumer({
    namespace: process.env.ROCKETMQ_NAMESPACE || '', // Namespace, can be empty string
    endpoints: process.env.ROCKETMQ_ENDPOINT || 'localhost:8080',
    consumerGroup: 'yourConsumerGroup',
    
    // Subscribe to topic and TAG
    subscriptions: new Map([
      ['yourTopic', '*'],  // Subscribe to yourTopic, receive all TAGs
    ]),
    
    // Message listener - this is the core processing logic
    messageListener: {
      async consume(messageView: MessageView): Promise<ConsumeResult> {
        console.log('Received message:', messageView.body.toString('utf-8'));
        
        // TODO: Process your business logic here
        
        return ConsumeResult.SUCCESS;
      },
    },
  });

  try {
    // Start consumer
    await pushConsumer.startup();
    console.log('PushConsumer started, waiting for messages...');
    
    // Keep running
    await new Promise(() => {});
  } catch (error) {
    console.error('Error:', error);
    await pushConsumer.shutdown();
    throw error;
  }
}

// Run example
quickStart().catch(console.error);

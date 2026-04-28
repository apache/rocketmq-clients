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

import { Producer } from '../src';
import { topics, endpoints, sessionCredentials, namespace } from './ProducerSingleton';

(async () => {
  const producer = new Producer({
    endpoints,
    namespace,
    sessionCredentials,
    maxAttempts: 3,
  });
  await producer.startup();

  try {
    // Send delay message
    const deliveryTimestamp = new Date(Date.now() + 5000); // Deliver after 5 seconds
    const receipt = await producer.send({
      topic: topics.delay,
      tag: 'rocketmq-delay',
      deliveryTimestamp,
      body: Buffer.from(JSON.stringify({
        hello: 'rocketmq-client-nodejs world ',
        now: Date(),
      })),
    });

    console.log('✅ Message sent successfully');
    console.log('   - Message ID:', receipt.messageId);
    console.log('   - Delivery Timestamp:', deliveryTimestamp.toISOString());
    console.log('   - Offset:', receipt.offset);
  } catch (error) {
    console.error('❌ Error sending message:', error);
  } finally {
    await producer.shutdown();
  }
})();

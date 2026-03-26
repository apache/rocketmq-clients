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
    maxAttempts: 2,
  });
  await producer.startup();

  try {
    // 发送延迟消息
    const receipt = await producer.send({
      topic: topics.delay,
      tag: 'rocketmq-delay',
      delay: 5000,
      body: Buffer.from(JSON.stringify({
        hello: 'rocketmq-client-nodejs world ',
        now: Date(),
      })),
    });

    console.log('✅ Message sent successfully');
    console.log('   - Message ID:', receipt.messageId);
    console.log('   - Recall Handle:', receipt.recallHandle);
    console.log('   - Offset:', receipt.offset);

    // 检查 recallHandle 是否存在
    if (!receipt.recallHandle || receipt.recallHandle.trim() === '') {
      console.warn('\n⚠️  Warning: Recall handle is empty');
      console.log('This might be because:');
      console.log('1. The topic is not configured as DELAY type');
      console.log('2. Broker does not support mixed message type');
      console.log('3. The message was not recognized as a delay message');

      // 尝试检查 Topic 配置
      console.log('\n💡 Suggestion: Check if the topic "time-topic" has message.type=DELAY attribute');
    } else {
      console.log('\n🔄 Attempting to recall message...');

      try {
        const recallReceipt = await producer.recallMessage(topics.delay, receipt.recallHandle);
        console.log('✅ Message recalled successfully!');
        console.log('   - Recalled Message ID:', recallReceipt.messageId);
      } catch (recallError) {
        console.error('❌ Failed to recall message:');
        console.error('   Error:', (recallError as Error).message);
        console.error('   Status Code:', (recallError as any).code);

        // 提供更多调试信息
        if ((recallError as Error).message.includes('recall handle is invalid')) {
          console.log('\n📝 Possible reasons:');
          console.log('   1. The recall handle format is incorrect');
          console.log('   2. The message has already been delivered/consumed');
          console.log('   3. The recall time window has expired');
          console.log('   4. Broker configuration issue (enableMixedMessageType=false)');
        }
      }
    }
  } catch (error) {
    console.error('❌ Error sending message:', error);
  } finally {
    await producer.shutdown();
  }
})();

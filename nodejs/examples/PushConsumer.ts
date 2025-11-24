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

import { PushConsumer, ConsumeResult, FilterExpression, MessageView } from '../src';

const accessPoint = 'localhost:8081';
const group = 'test-group';
const topic = 'test-topic';
const tag = 'test-tag';

async function main() {
  const consumer = new PushConsumer({
    endpoints: accessPoint,
    consumerGroup: group,
    namespace: '',
    topics: [ topic ],
  }, async (message: MessageView) => {
    console.log(`Received message: ${message.body.toString()}`);
    return ConsumeResult.SUCCESS;
  });

  consumer.subscribe(topic, new FilterExpression(tag));

  await consumer.start();
  console.log('Consumer started.');
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});

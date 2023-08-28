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

import { randomUUID } from 'node:crypto';
import { strict as assert } from 'node:assert';
import {
  SimpleConsumer, FilterExpression,
  Producer,
} from '../../src';

describe('test/consumer/SimpleConsumer.test.ts', () => {
  let producer: Producer | null = null;
  let simpleConsumer: SimpleConsumer | null = null;
  afterEach(async () => {
    if (producer) {
      await producer.shutdown();
      producer = null;
    }
    if (simpleConsumer) {
      await simpleConsumer.shutdown();
      simpleConsumer = null;
    }
  });

  describe('receive() and ack()', () => {
    it('should receive success', async () => {
      const topic = 'TopicTest';
      const tag = `nodejs-unittest-tag-${randomUUID()}`;
      producer = new Producer({
        endpoints: '127.0.0.1:8081',
      });
      await producer.startup();
      const max = 101;
      for (let i = 0; i < max; i++) {
        const receipt = await producer.send({
          topic,
          tag,
          body: Buffer.from(JSON.stringify({ hello: 'world' })),
        });
        assert(receipt.messageId);
      }

      simpleConsumer = new SimpleConsumer({
        consumerGroup: 'nodejs-unittest-group',
        endpoints: '127.0.0.1:8081',
        subscriptions: new Map().set(topic, new FilterExpression(tag)),
      });
      await simpleConsumer.startup();
      let count = 0;
      let messages = await simpleConsumer.receive(20, 10000);
      while (messages.length > 0) {
        for (const message of messages) {
          assert.equal(message.topic, topic);
          // console.log('#%s: %o, %o', count, message, message.body.toString());
          const msg = JSON.parse(message.body.toString());
          assert.deepEqual(msg, { hello: 'world' });
          assert.equal(message.tag, tag);
          count++;
          await simpleConsumer.ack(message);
        }
        if (count === max) break;
        messages = await simpleConsumer.receive(20, 10000);
      }
      assert.equal(count, max);
    });
  });
});

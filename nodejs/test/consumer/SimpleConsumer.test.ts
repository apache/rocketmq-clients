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
import { topics, endpoints, sessionCredentials } from '../helper';

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

  describe('start with sessionCredentials', () => {
    it('should work', async () => {
      if (!sessionCredentials) return;
      simpleConsumer = new SimpleConsumer({
        endpoints,
        sessionCredentials,
        consumerGroup: 'nodejs-unittest-group',
        subscriptions: new Map().set(topics.delay, FilterExpression.SUB_ALL),
      });
      await simpleConsumer.startup();
    });

    it('should fail when accessKey invalid', async () => {
      if (!sessionCredentials) return;
      simpleConsumer = new SimpleConsumer({
        endpoints,
        sessionCredentials: {
          ...sessionCredentials,
          accessKey: 'wrong',
        },
        consumerGroup: 'nodejs-unittest-group',
        subscriptions: new Map().set(topics.delay, FilterExpression.SUB_ALL),
      });
      await assert.rejects(async () => {
        await simpleConsumer!.startup();
      }, /Startup the rocketmq client failed, .+? error=ForbiddenException: .+? Username is not matched/);
    });

    it('should fail when accessSecret invalid', async () => {
      if (!sessionCredentials) return;
      simpleConsumer = new SimpleConsumer({
        endpoints,
        sessionCredentials: {
          ...sessionCredentials,
          accessSecret: 'wrong',
        },
        consumerGroup: 'nodejs-unittest-group',
        subscriptions: new Map().set(topics.delay, FilterExpression.SUB_ALL),
      });
      await assert.rejects(async () => {
        await simpleConsumer!.startup();
      }, /Startup the rocketmq client failed, .+? error=ForbiddenException: .+? Check signature failed for accessKey/);
    });
  });

  describe('receive() and ack()', () => {
    it('should receive success', async () => {
      const topic = topics.normal;
      const tag = `nodejs-unittest-tag-${randomUUID()}`;
      producer = new Producer({
        endpoints,
        sessionCredentials,
      });
      await producer.startup();
      simpleConsumer = new SimpleConsumer({
        endpoints,
        sessionCredentials,
        consumerGroup: `nodejs-unittest-group-${randomUUID()}`,
        subscriptions: new Map().set(topic, new FilterExpression(tag)),
      });
      await simpleConsumer.startup();
      const receipt = await producer.send({
        topic,
        tag,
        body: Buffer.from(JSON.stringify({ hello: 'world' })),
      });
      assert(receipt.messageId);
      const messages = await simpleConsumer.receive(20, 10000);
      assert.equal(messages.length, 1);
      assert.equal(messages[0].messageId, receipt.messageId);

      const max = 102;
      for (let i = 0; i < max; i++) {
        const receipt = await producer.send({
          topic,
          tag,
          body: Buffer.from(JSON.stringify({ hello: 'world' })),
        });
        assert(receipt.messageId);
      }

      let count = 0;
      while (count < max) {
        const messages = await simpleConsumer.receive(20, 10000);
        console.log('#%s: receive %d new messages', count, messages.length);
        for (const message of messages) {
          assert.equal(message.topic, topic);
          // console.log('#%s: %o, %o', count, message, message.body.toString());
          const msg = JSON.parse(message.body.toString());
          assert.deepEqual(msg, { hello: 'world' });
          assert.equal(message.tag, tag);
          count++;
          await simpleConsumer.ack(message);
        }
      }
      assert.equal(count, max);
    });
  });
});

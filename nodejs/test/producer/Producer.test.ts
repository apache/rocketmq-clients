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

import { strict as assert } from 'node:assert';
import { randomUUID } from 'node:crypto';
import { NotFoundException, Producer, SimpleConsumer } from '../../src';
import { TransactionResolution } from '../../proto/apache/rocketmq/v2/definition_pb';

describe('test/producer/Producer.test.ts', () => {
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

  describe('startup()', () => {
    it('should startup success', async () => {
      producer = new Producer({
        endpoints: '127.0.0.1:8081',
        maxAttempts: 2,
      });
      await producer.startup();
      const sendReceipt = await producer.send({
        topic: 'TopicTest',
        tag: 'nodejs-unittest',
        keys: [
          `foo-key-${Date.now()}`,
          `bar-key-${Date.now()}`,
        ],
        body: Buffer.from(JSON.stringify({
          hello: 'rocketmq-client-nodejs world 😄',
          now: Date(),
        })),
      });
      // console.log('sendReceipt: %o', sendReceipt);
      assert(sendReceipt.offset >= 0);
      assert.equal(typeof sendReceipt.messageId, 'string');
      assert.equal(sendReceipt.messageId, sendReceipt.transactionId);
    });

    it('should startup fail when topic not exists', async () => {
      await assert.rejects(async () => {
        producer = new Producer({
          topic: 'TopicTest-not-exists',
          endpoints: '127.0.0.1:8081',
          maxAttempts: 2,
        });
        await producer.startup();
      }, (err: any) => {
        assert.match(err.message, /Startup the rocketmq client failed, clientId=[^,]+, error=NotFoundException/);
        assert.equal(err.cause instanceof NotFoundException, true);
        assert.equal(err.cause.name, 'NotFoundException');
        assert.equal(err.cause.code, 40402);
        assert.match(err.cause.message, /CODE: 17 {2}DESC: No topic route info in name server for the topic: TopicTest-not-exists/);
        return true;
      });
    });
  });

  describe('send()', () => {
    it('should send normal message', async () => {
      const topic = 'TopicTest';
      const tag = `nodejs-unittest-tag-${randomUUID()}`;
      producer = new Producer({
        endpoints: '127.0.0.1:8081',
        maxAttempts: 2,
      });
      await producer.startup();
      const receipt = await producer.send({
        topic,
        tag,
        keys: [
          `foo-key-${Date.now()}`,
          `bar-key-${Date.now()}`,
        ],
        body: Buffer.from(JSON.stringify({
          hello: 'rocketmq-client-nodejs world 😄',
          now: Date(),
        })),
      });
      assert(receipt.messageId);

      simpleConsumer = new SimpleConsumer({
        consumerGroup: 'nodejs-unittest-group',
        endpoints: '127.0.0.1:8081',
        subscriptions: new Map().set(topic, tag),
        awaitDuration: 3000,
      });
      await simpleConsumer.startup();
      const messages = await simpleConsumer.receive(1, 10000);
      assert.equal(messages.length, 1);
      assert.equal(messages[0].messageId, receipt.messageId);
    });

    it.skip('should send delay message', async () => {
      const topic = 'TestDelayTopic';
      const tag = `nodejs-unittest-tag-${randomUUID()}`;
      producer = new Producer({
        endpoints: '127.0.0.1:8081',
        maxAttempts: 2,
      });
      await producer.startup();
      const receipt = await producer.send({
        topic,
        tag,
        delay: 1000,
        keys: [
          `foo-key-${Date.now()}`,
          `bar-key-${Date.now()}`,
        ],
        body: Buffer.from(JSON.stringify({
          hello: 'rocketmq-client-nodejs world 😄',
          now: Date(),
        })),
      });
      assert(receipt.messageId);

      simpleConsumer = new SimpleConsumer({
        consumerGroup: 'nodejs-unittest-group',
        endpoints: '127.0.0.1:8081',
        subscriptions: new Map().set(topic, tag),
        awaitDuration: 3000,
      });
      await simpleConsumer.startup();
      const messages = await simpleConsumer.receive(1, 10000);
      assert.equal(messages.length, 1);
      assert.equal(messages[0].messageId, receipt.messageId);
      console.log(messages);
    });

    it.skip('should send transaction message', async () => {
      const topic = 'TopicTest';
      const tag = `nodejs-unittest-tag-${randomUUID()}`;
      producer = new Producer({
        endpoints: '127.0.0.1:8081',
        maxAttempts: 2,
        checker: {
          async check(messageView) {
            console.log(messageView);
            return TransactionResolution.COMMIT;
          },
        },
      });
      await producer.startup();
      const transaction = producer.beginTransaction();
      const receipt = await producer.send({
        topic,
        tag,
        keys: [
          `foo-key-${Date.now()}`,
          `bar-key-${Date.now()}`,
        ],
        body: Buffer.from(JSON.stringify({
          hello: 'rocketmq-client-nodejs world 😄',
          now: Date(),
        })),
      }, transaction);
      await transaction.commit();

      simpleConsumer = new SimpleConsumer({
        consumerGroup: 'nodejs-unittest-group',
        endpoints: '127.0.0.1:8081',
        subscriptions: new Map().set(topic, tag),
        awaitDuration: 3000,
      });
      await simpleConsumer.startup();
      const messages = await simpleConsumer.receive(2, 10000);
      assert.equal(messages.length, 1);
      assert.equal(messages[0].messageId, receipt.messageId);
      console.log(messages);
    });
  });
});

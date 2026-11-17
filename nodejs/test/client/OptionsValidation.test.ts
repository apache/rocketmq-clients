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
import { Producer, PushConsumer, SimpleConsumer, ConsumeResult, Message } from '../../src';
import type { MessageOptions } from '../../src/message/Message';

const configuration = { endpoints: '127.0.0.1:8081', namespace: '' };
const listener = { async consume() { return ConsumeResult.SUCCESS; } };
const message = { topic: 'TestTopic', body: Buffer.from('body') };

// All assertions happen in constructors; no client is started.
describe('Client options validation', () => {
  for (const value of [ null, '', '   ', 42 ]) {
    it(`Producer should reject topic ${JSON.stringify(value)}`, () => {
      assert.throws(() => new Producer({ ...configuration, topic: [ value as unknown as string ] }),
        { name: 'TypeError', message: 'topic must be a non-empty string' });
    });
    for (const Client of [ PushConsumer, SimpleConsumer ]) {
      it(`${Client.name} should reject subscription topic ${JSON.stringify(value)}`, () => {
        assert.throws(() => new Client({
          ...configuration, consumerGroup: 'TestGroup', messageListener: listener,
          subscriptions: new Map([[ value as unknown as string, '*' ]]),
        }), { name: 'TypeError', message: 'topic must be a non-empty string' });
      });
    }
  }

  for (const value of [ 0, -1, 1.5, NaN, Infinity ]) {
    it(`Producer should reject maxAttempts ${String(value)}`, () => {
      assert.throws(() => new Producer({ ...configuration, maxAttempts: value }),
        { name: 'TypeError', message: 'maxAttempts should be a positive integer' });
    });
  }

  for (const value of [ 0, -1000, NaN, Infinity ]) {
    it(`SimpleConsumer should reject awaitDuration ${String(value)}`, () => {
      assert.throws(() => new SimpleConsumer({
        ...configuration, consumerGroup: 'TestGroup', awaitDuration: value,
        subscriptions: new Map([[ 'TestTopic', '*' ]]),
      }), { name: 'TypeError', message: 'awaitDuration should be positive' });
    });
  }

  for (const Client of [ PushConsumer, SimpleConsumer ]) {
    it(`${Client.name} should reject empty subscriptions`, () => {
      assert.throws(() => new Client({
        ...configuration, consumerGroup: 'TestGroup', messageListener: listener, subscriptions: new Map(),
      }), { name: 'TypeError', message: 'subscriptions must be a non-empty Map' });
    });
  }

  it('PushConsumer should reject a listener without consume()', () => {
    assert.throws(() => new PushConsumer({
      ...configuration, consumerGroup: 'TestGroup', subscriptions: new Map([[ 'TestTopic', '*' ]]),
      messageListener: {} as typeof listener,
    }), { name: 'TypeError', message: 'messageListener must provide a consume function' });
  });

  it('should accept valid options, duplicated topics and an omitted topic', async () => {
    const clients = [
      new Producer(configuration),
      new Producer({ ...configuration, topic: [ 'TestTopic', 'TestTopic' ], maxAttempts: 1 }),
      new PushConsumer({
        ...configuration, consumerGroup: 'TestGroup', messageListener: listener,
        subscriptions: new Map([[ 'TestTopic', '*' ]]),
      }),
      new SimpleConsumer({
        ...configuration, consumerGroup: 'TestGroup', awaitDuration: 1000,
        subscriptions: new Map([[ 'TestTopic', '*' ]]),
      }),
    ];
    for (const client of clients) {
      await client.shutdown();
    }
  });
});

describe('Message options validation', () => {
  for (const value of [ '', '   ', null, 42 ]) {
    it(`should reject key ${JSON.stringify(value)}`, () => {
      assert.throws(() => new Message({ ...message, keys: [ value as unknown as string ] }),
        { name: 'TypeError', message: 'key should not be blank' });
    });
    it(`should reject property key ${JSON.stringify(value)}`, () => {
      assert.throws(() => new Message({
        ...message, properties: new Map([[ value as unknown as string, 'value' ]]),
      }), { name: 'TypeError', message: 'property key should not be blank' });
    });
    it(`should reject property value ${JSON.stringify(value)}`, () => {
      assert.throws(() => new Message({
        ...message, properties: new Map([[ 'key', value as unknown as string ]]),
      }), { name: 'TypeError', message: 'property value should not be blank' });
    });
  }

  it('should reject non-array keys and non-Map properties', () => {
    assert.throws(() => new Message({ ...message, keys: 'key' } as unknown as MessageOptions),
      { name: 'TypeError', message: 'keys must be an array' });
    assert.throws(() => new Message({ ...message, properties: {} } as MessageOptions),
      { name: 'TypeError', message: 'properties must be a Map' });
  });

  it('should keep valid keys and properties untouched', () => {
    const keys = [ ' key with spaces ', 'key:with:colon', 'k'.repeat(512) ];
    const properties = new Map(Array.from({ length: 256 }, (_, i) => [ `key-${i}`, `value-${i}` ]));
    properties.set('unicode-\u5c5e\u6027', ' \u503c ');
    const actual = new Message({ ...message, keys, properties });
    assert.deepStrictEqual(actual.keys, keys);
    assert.deepStrictEqual(actual.properties, properties);
  });
});

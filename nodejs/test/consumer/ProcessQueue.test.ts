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

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { ProcessQueue } from '../../src/consumer/ProcessQueue';
import { FilterExpression } from '../../src/consumer/FilterExpression';
import { Code } from '../../proto/apache/rocketmq/v2/definition_pb';

function makeConsumer(consumeConcurrentlyMax: number): any {
  return {
    getPushConsumerSettings: () => ({ getConsumeConcurrentlyMax: () => consumeConcurrentlyMax }),
    getRetryPolicy: () => ({ getNextAttemptDelay: () => 0 }),
    requestTimeoutValue: 3000,
    changeInvisibleDurationViaRpc: () => Promise.resolve({
      getStatus: () => ({ toObject: () => ({ code: Code.OK }) }),
    }),
    wrapChangeInvisibleDurationRequest: () => ({}),
    getConsumerGroup: () => 'cg',
    clientId: 'client',
    logger: { info: () => {}, debug: () => {}, warn: () => {}, error: () => {} },
    getConsumeService: () => ({ consume: () => {} }),
    cacheMessageCountThresholdPerQueue: () => 1024,
    cacheMessageBytesThresholdPerQueue: () => 1024 * 1024,
  };
}

function makeMessage(id: string): any {
  return {
    messageId: id,
    body: '',
    endpoints: {},
    deliveryAttempt: 1,
    topic: { name: 't' },
    liteTopic: '',
  };
}

const mq: any = { topic: { name: 't' }, broker: { name: 'b' }, queueId: 0 };

describe('ProcessQueue consumption permit (backpressure)', () => {
  it('caps cached messages by consumeConcurrentlyMax and reports availablePermits', () => {
    const pq = new ProcessQueue(makeConsumer(2), mq, new FilterExpression('*'));
    pq.cacheMessages([makeMessage('a'), makeMessage('b'), makeMessage('c')]);
    // Only two permits exist, so only two messages are cached.
    assert.strictEqual(pq.cachedMessagesCount(), 2);
    assert.strictEqual(pq.availablePermits(), 0);
  });

  it('releases a permit when a message is settled (discarded/nacked)', async () => {
    const pq = new ProcessQueue(makeConsumer(2), mq, new FilterExpression('*'));
    const a = makeMessage('a');
    pq.cacheMessages([a, makeMessage('b')]);
    assert.strictEqual(pq.availablePermits(), 0);
    // discardMessage -> nack -> (async) evict -> release permit
    pq.discardMessage(a);
    await new Promise(r => setTimeout(r, 30));
    assert.strictEqual(pq.availablePermits(), 1);
  });

  it('releases nothing for an unknown message (no double release)', async () => {
    const pq = new ProcessQueue(makeConsumer(2), mq, new FilterExpression('*'));
    const a = makeMessage('a');
    pq.cacheMessages([a]);
    assert.strictEqual(pq.availablePermits(), 1);
    pq.discardMessage(makeMessage('z')); // not in cache
    await new Promise(r => setTimeout(r, 30));
    // Only the originally cached message holds a permit; the unknown one is a no-op.
    assert.strictEqual(pq.availablePermits(), 1);
    pq.discardMessage(a);
    await new Promise(r => setTimeout(r, 30));
    assert.strictEqual(pq.availablePermits(), 2);
  });
});

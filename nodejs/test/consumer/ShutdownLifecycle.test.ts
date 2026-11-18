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
import { Producer, ConsumeResult } from '../../src';
import type { MessageView } from '../../src/message/MessageView';
import { getDefaultLogger } from '../../src/client/Logger';
import { ConsumeService } from '../../src/consumer/ConsumeService';
import { ProcessQueue } from '../../src/consumer/ProcessQueue';
import { FilterExpression } from '../../src/consumer/FilterExpression';
import { Code } from '../../proto/apache/rocketmq/v2/definition_pb';

describe('Shutdown lifecycle', () => {
  it('should share the default logger across clients', () => {
    assert.strictEqual(getDefaultLogger(), getDefaultLogger());
  });

  it('should not close the logger injected by the caller', async () => {
    let closes = 0;
    const logger = {
      info() { return undefined; }, warn() { return undefined; },
      error() { return undefined; }, debug() { return undefined; },
      close() { closes++; },
    };
    const producer = new Producer({ endpoints: '127.0.0.1:8081', namespace: '', logger });
    await producer.shutdown();
    await new Promise(resolve => setImmediate(resolve));
    assert.strictEqual(closes, 0);
  });

  it('should settle a delayed consumption on abort without calling the listener', async () => {
    class TestService extends ConsumeService {
      consume() { return undefined; }
    }
    let calls = 0;
    const service = new TestService('client', {
      async consume() { calls++; return ConsumeResult.SUCCESS; },
    });
    const pending = service.consumeMessage({} as MessageView, 60000);
    service.abort();
    const result = await Promise.race([
      pending,
      new Promise(resolve => setTimeout(() => resolve('pending'), 100)),
    ]);
    assert.strictEqual(result, ConsumeResult.FAILURE);
    assert.strictEqual(calls, 0);
  });

  function queueFixture() {
    let acks = 0;
    let retries = 0;
    const consumer = {
      getPushConsumerSettings: () => ({ getConsumeConcurrentlyMax: () => 1 }),
      getRetryPolicy: () => ({ getMaxAttempts: () => 16, getNextAttemptDelay: () => 0 }),
      getConsumeService: () => ({
        async consumeMessage() { retries++; return ConsumeResult.FAILURE; },
      }),
      incrementConsumptionOkQuantity() { return undefined; },
      incrementConsumptionErrorQuantity() { return undefined; },
      wrapAckMessageRequest: () => ({}), requestTimeoutValue: 3000,
      wrapForwardMessageToDeadLetterQueueRequest: () => ({}),
      async forwardMessageToDeadLetterQueueViaRpc() {
        return { getStatus: () => ({ toObject: () => ({ code: Code.OK }) }) };
      },
      async ackMessageViaRpc() {
        acks++;
        return { getStatus: () => ({ toObject: () => ({ code: Code.OK }) }) };
      },
    };
    const queue = new ProcessQueue(consumer as any, {} as any, new FilterExpression('*'));
    const view = {
      messageId: 'id', deliveryAttempt: 1, body: Buffer.from('body'),
      incrementAndGetDeliveryAttempt() {
        this.deliveryAttempt = (this.deliveryAttempt ?? 0) + 1;
        return this.deliveryAttempt;
      },
    } as MessageView;
    queue.cacheMessages([ view ]);
    return { queue, view, acks: () => acks, retries: () => retries };
  }

  it('should not retry FIFO messages locally after abort and release the cache permit', async () => {
    const fixture = queueFixture();
    fixture.queue.abort();
    await fixture.queue.eraseFifoMessage(fixture.view, ConsumeResult.FAILURE);
    assert.strictEqual(fixture.retries(), 0);
    assert.strictEqual(fixture.queue.cachedMessagesCount(), 0);
    assert.strictEqual(fixture.queue.availablePermits(), 1);
  });

  it('should still ack in-flight FIFO messages when only dropped', async () => {
    const fixture = queueFixture();
    fixture.queue.drop();
    await fixture.queue.eraseFifoMessage(fixture.view, ConsumeResult.SUCCESS);
    assert.strictEqual(fixture.acks(), 1);
    assert.strictEqual(fixture.queue.cachedMessagesCount(), 0);
  });
});

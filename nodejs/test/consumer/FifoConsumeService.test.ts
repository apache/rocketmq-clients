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
 * Test for FIFO Consume Accelerator
 */

import * as assert from 'node:assert';
import * as sinon from 'sinon';
import { FifoConsumeService } from '../../src/consumer/FifoConsumeService';
import { MessageListener } from '../../src/consumer/MessageListener';
import { ConsumeResult } from '../../src/consumer/ConsumeResult';
import type { MessageView } from '../../src/message';
import { deferred } from '../support/fakes';
import { buildMessageView, createProcessQueueHarness, flushPromises } from './processQueueHarness';

describe('FifoConsumeService', () => {
  afterEach(() => sinon.restore());

  it('should support enableFifoConsumeAccelerator option', () => {
    const messageListener: MessageListener = {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      async consume(_messageView: MessageView): Promise<ConsumeResult> {
        return ConsumeResult.SUCCESS;
      },
    };

    // Create with accelerator enabled
    const serviceWithAccelerator = new FifoConsumeService('test-client', messageListener, true);
    assert.ok(serviceWithAccelerator, 'Should create service with accelerator enabled');

    // Create with accelerator disabled (default)
    const serviceWithoutAccelerator = new FifoConsumeService('test-client', messageListener, false);
    assert.ok(serviceWithoutAccelerator, 'Should create service with accelerator disabled');

    // Create without specifying (default to false)
    const serviceDefault = new FifoConsumeService('test-client', messageListener);
    assert.ok(serviceDefault, 'Should create service with default accelerator setting');
  });

  it('redelivers a failed message locally until maxAttempts and then forwards it to the DLQ', async () => {
    const h = createProcessQueueHarness({ maxAttempts: 3 });
    const consume = sinon.stub().resolves(ConsumeResult.FAILURE);
    const service = new FifoConsumeService('client-a', { consume });
    h.setConsumeService(service);
    const message = buildMessageView({ deliveryAttempt: 1 });
    h.pq.cacheMessages([ message ]);

    service.consume(h.pq, [ message ]);
    await flushPromises();

    sinon.assert.calledThrice(consume);
    assert.strictEqual(message.deliveryAttempt, 3);
    sinon.assert.calledOnce(h.consumer.forwardMessageToDeadLetterQueueViaRpc);
    sinon.assert.notCalled(h.consumer.ackMessageViaRpc);
    sinon.assert.notCalled(h.consumer.changeInvisibleDurationViaRpc);
    assert.strictEqual(h.pq.cachedMessagesCount(), 0);
  });

  for (const accelerator of [ true, false ]) {
    it(`consumes message groups ${accelerator ? 'in parallel' : 'serially'} when the accelerator is ${accelerator ? 'on' : 'off'}`, async () => {
      const h = createProcessQueueHarness();
      const started: string[] = [];
      const results = new Map<string, ReturnType<typeof deferred<ConsumeResult>>>();
      const service = new FifoConsumeService('client-a', {
        consume(messageView) {
          started.push(messageView.messageId);
          const result = deferred<ConsumeResult>();
          results.set(messageView.messageId, result);
          return result.promise;
        },
      }, accelerator);
      h.setConsumeService(service);
      const messages = [
        buildMessageView({ messageId: 'g1-a', messageGroup: 'g1' }),
        buildMessageView({ messageId: 'g1-b', messageGroup: 'g1' }),
        buildMessageView({ messageId: 'g2-a', messageGroup: 'g2' }),
      ];
      h.pq.cacheMessages(messages);

      service.consume(h.pq, messages);
      await flushPromises();
      assert.deepStrictEqual(started, accelerator ? [ 'g1-a', 'g2-a' ] : [ 'g1-a' ]);

      results.get('g1-a')!.resolve(ConsumeResult.SUCCESS);
      await flushPromises();
      assert.deepStrictEqual(started, accelerator ? [ 'g1-a', 'g2-a', 'g1-b' ] : [ 'g1-a', 'g1-b' ]);
    });
  }

  it('discards a corrupted message to the DLQ without blocking the next one', async () => {
    const h = createProcessQueueHarness();
    const consume = sinon.stub().resolves(ConsumeResult.SUCCESS);
    const service = new FifoConsumeService('client-a', { consume });
    h.setConsumeService(service);
    const corrupted = buildMessageView({ messageId: 'corrupted', checksum: 'invalid' });
    const valid = buildMessageView({ messageId: 'valid' });
    h.pq.cacheMessages([ corrupted, valid ]);

    service.consume(h.pq, [ corrupted, valid ]);
    await flushPromises();

    sinon.assert.calledOnceWithExactly(consume, valid);
    sinon.assert.calledOnce(h.consumer.forwardMessageToDeadLetterQueueViaRpc);
    sinon.assert.calledOnce(h.consumer.ackMessageViaRpc);
    assert.strictEqual(h.pq.cachedMessagesCount(), 0);
  });
});

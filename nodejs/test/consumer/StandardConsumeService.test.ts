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

import * as assert from 'node:assert';
import * as sinon from 'sinon';
import { StandardConsumeService } from '../../src/consumer/StandardConsumeService';
import { ConsumeResult } from '../../src/consumer/ConsumeResult';
import { buildMessageView, createProcessQueueHarness, flushPromises } from './processQueueHarness';

describe('StandardConsumeService', () => {
  afterEach(() => sinon.restore());

  it('acks every successfully consumed message', async () => {
    const h = createProcessQueueHarness();
    const consume = sinon.stub().resolves(ConsumeResult.SUCCESS);
    const service = new StandardConsumeService('client-a', { consume });
    const messages = [ buildMessageView({ messageId: 'a' }), buildMessageView({ messageId: 'b' }) ];
    h.pq.cacheMessages(messages);

    service.consume(h.pq, messages);
    await flushPromises();

    sinon.assert.calledTwice(consume);
    sinon.assert.calledTwice(h.consumer.ackMessageViaRpc);
    assert.strictEqual(h.pq.cachedMessagesCount(), 0);
  });

  it('discards a corrupted message without calling the listener', async () => {
    const h = createProcessQueueHarness();
    const consume = sinon.stub().resolves(ConsumeResult.SUCCESS);
    const service = new StandardConsumeService('client-a', { consume });
    const corrupted = buildMessageView({ checksum: 'invalid' });
    h.pq.cacheMessages([ corrupted ]);

    service.consume(h.pq, [ corrupted ]);
    await flushPromises();

    sinon.assert.notCalled(consume);
    sinon.assert.calledOnce(h.consumer.changeInvisibleDurationViaRpc);
    sinon.assert.notCalled(h.consumer.ackMessageViaRpc);
    assert.strictEqual(h.pq.cachedMessagesCount(), 0);
  });

  it('nacks a message whose listener throws', async () => {
    const h = createProcessQueueHarness();
    const service = new StandardConsumeService('client-a', { consume: sinon.stub().rejects(new Error('listener')) });
    const message = buildMessageView();
    h.pq.cacheMessages([ message ]);

    service.consume(h.pq, [ message ]);
    await flushPromises();

    sinon.assert.calledOnce(h.consumer.incrementConsumptionErrorQuantity);
    sinon.assert.calledOnce(h.consumer.changeInvisibleDurationViaRpc);
    sinon.assert.notCalled(h.consumer.ackMessageViaRpc);
    assert.strictEqual(h.pq.cachedMessagesCount(), 0);
  });

  it('nacks and evicts a message whose settlement throws', async () => {
    const h = createProcessQueueHarness({ consumeConcurrentlyMax: 1 });
    h.consumer.incrementConsumptionOkQuantity.throws(new Error('stats failed'));
    const service = new StandardConsumeService('client-a', { consume: sinon.stub().resolves(ConsumeResult.SUCCESS) });
    const message = buildMessageView();
    h.pq.cacheMessages([ message ]);

    service.consume(h.pq, [ message ]);
    await flushPromises();

    sinon.assert.notCalled(h.consumer.ackMessageViaRpc);
    sinon.assert.calledOnce(h.consumer.changeInvisibleDurationViaRpc);
    assert.strictEqual(h.pq.cachedMessagesCount(), 0);
    assert.strictEqual(h.pq.availablePermits(), 1);
  });
});

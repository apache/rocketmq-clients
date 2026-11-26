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
import { FifoConsumeService } from '../../src/consumer/FifoConsumeService';
import { LiteStandardConsumeService } from '../../src/consumer/LiteStandardConsumeService';
import { LiteFifoConsumeService } from '../../src/consumer/LiteFifoConsumeService';
import { PushConsumer } from '../../src/consumer/PushConsumer';
import { LitePushConsumerImpl } from '../../src/consumer/LitePushConsumerImpl';
import { Subscription } from '../../proto/apache/rocketmq/v2/definition_pb';
import { getDefaultLogger } from '../../src/client/Logger';
import { createSilentLogger } from '../support/fakes';
import { buildSettings } from '../support/protobuf';
import { ConsumeResult } from '../../src/consumer/ConsumeResult';
import { buildMessageView, createProcessQueueHarness, flushPromises } from './processQueueHarness';

class LoggingPushConsumer extends PushConsumer {
  makeService() { return this.createConsumeService(); }
}

class LoggingLitePushConsumer extends LitePushConsumerImpl {
  makeService() { return this.createConsumeService(); }
}

// Cover logger injection by the actual factories, including parent discard and subclass error logs for Lite FIFO.
describe('消费服务 logger 注入', () => {
  afterEach(() => sinon.restore());

  [ false, true ].forEach(lite => {
    [ false, true ].forEach(fifo => {
      const name = `${lite ? 'Lite ' : ''}${fifo ? 'FIFO' : 'standard'}`;
      function makeClient(logger = createSilentLogger()) {
        const options = {
          endpoints: '127.0.0.1:8081', namespace: '', consumerGroup: 'group', logger,
          subscriptions: new Map([[ 'parent', '*' ]]),
          messageListener: { consume: () => ConsumeResult.SUCCESS },
        };
        const client = lite ? new LoggingLitePushConsumer({ ...options, bindTopic: 'parent' })
          : new LoggingPushConsumer(options);
        client.getPushConsumerSettings().sync(buildSettings({ subscription: new Subscription().setFifo(fifo) }));
        return client;
      }

      it(`${name} 工厂将丢弃日志写入客户端 logger`, async () => {
        const logger = createSilentLogger();
        const fallback = getDefaultLogger();
        const defaultError = sinon.stub(fallback, 'error');
        const defaultWarn = sinon.stub(fallback, 'warn');
        const client = makeClient(logger);
        const service = client.makeService();
        const h = createProcessQueueHarness();
        const consume = sinon.spy(service, 'consumeMessage');
        const corrupted = buildMessageView({ checksum: 'invalid', liteTopic: lite ? 'a' : undefined });
        h.pq.cacheMessages([ corrupted ]);
        try {
          service.consume(h.pq, [ corrupted ]);
          await flushPromises();
          if (!lite && !fifo) sinon.assert.calledOnce(logger.warn);
          else sinon.assert.calledOnce(logger.error);
          sinon.assert.notCalled(defaultError);
          sinon.assert.notCalled(defaultWarn);
          sinon.assert.notCalled(consume);
          assert.strictEqual(h.pq.cachedMessagesCount(), 0);
        } finally {
          h.pq.abort();
          service.abort();
          await client.shutdown();
        }
        sinon.assert.notCalled(logger.close);
      });

      it(`${name} 旧构造调用仍使用默认 logger`, async () => {
        const logger = getDefaultLogger();
        const error = sinon.stub(logger, 'error');
        const warn = sinon.stub(logger, 'warn');
        const listener = { consume: () => ConsumeResult.SUCCESS };
        const service = lite
          ? (fifo ? new LiteFifoConsumeService('client', listener) : new LiteStandardConsumeService('client', listener))
          : (fifo ? new FifoConsumeService('client', listener) : new StandardConsumeService('client', listener));
        const h = createProcessQueueHarness();
        service.consume(h.pq, [ buildMessageView({ checksum: 'invalid' }) ]);
        await flushPromises();
        if (!lite && !fifo) sinon.assert.calledOnce(warn);
        else sinon.assert.calledOnce(error);
        service.abort();
      });

      if (lite && fifo) {
        it('Lite FIFO 子类的结算异常同样写入客户端 logger', async () => {
          const logger = createSilentLogger();
          const defaultError = sinon.stub(getDefaultLogger(), 'error');
          const client = makeClient(logger);
          const service = client.makeService();
          const h = createProcessQueueHarness();
          const failure = new Error('结算失败');
          sinon.stub(h.pq, 'eraseFifoMessage').rejects(failure);
          try {
            service.consume(h.pq, [ buildMessageView({ liteTopic: 'a' }) ]);
            await flushPromises();
            sinon.assert.calledOnce(logger.error);
            assert.ok(logger.error.firstCall.args.includes(failure));
            sinon.assert.notCalled(defaultError);
          } finally {
            service.abort();
            await client.shutdown();
          }
        });
      }
    });
  });
});

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

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
import { ProcessQueue } from '../../src/consumer/ProcessQueue';
import { FilterExpression } from '../../src/consumer/FilterExpression';
import { ConsumeResult, ConsumeResultSuspend } from '../../src/consumer/ConsumeResult';
import type { ConsumeService } from '../../src/consumer/ConsumeService';
import { TooManyRequestsException } from '../../src/exception';
import { Code } from '../../proto/apache/rocketmq/v2/definition_pb';
import { setRequestId } from '../../src/util/rpcContext';
import { ForwardMessageToDeadLetterQueueResponse } from '../../proto/apache/rocketmq/v2/service_pb';
import { deferred } from '../support/fakes';
import { buildRoute } from '../support/protobuf';
import {
  ackResponse,
  buildMessageView,
  changeInvisibleDurationResponse,
  createProcessQueueHarness,
  flushPromises,
  useFakeTimers,
} from './processQueueHarness';

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
    logger: { info: () => undefined, debug: () => undefined, warn: () => undefined, error: () => undefined },
    getConsumeService: () => ({ consume: () => undefined }),
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
    pq.cacheMessages([ makeMessage('a'), makeMessage('b'), makeMessage('c') ]);
    // Only two permits exist, so only two messages are cached.
    assert.strictEqual(pq.cachedMessagesCount(), 2);
    assert.strictEqual(pq.availablePermits(), 0);
  });

  it('releases a permit when a message is settled (discarded/nacked)', async () => {
    const pq = new ProcessQueue(makeConsumer(2), mq, new FilterExpression('*'));
    const a = makeMessage('a');
    pq.cacheMessages([ a, makeMessage('b') ]);
    assert.strictEqual(pq.availablePermits(), 0);
    // discardMessage -> nack -> (async) evict -> release permit
    pq.discardMessage(a);
    await new Promise(r => setTimeout(r, 30));
    assert.strictEqual(pq.availablePermits(), 1);
  });

  it('releases nothing for an unknown message (no double release)', async () => {
    const pq = new ProcessQueue(makeConsumer(2), mq, new FilterExpression('*'));
    const a = makeMessage('a');
    pq.cacheMessages([ a ]);
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

describe('ProcessQueue receive loop', () => {
  let clock: sinon.SinonFakeTimers;
  const never = () => new Promise<never>(() => undefined);

  beforeEach(() => {
    clock = useFakeTimers();
  });

  afterEach(() => {
    clock.restore();
    sinon.restore();
  });

  it('caches received messages, hands them to the consume service and receives again', async () => {
    const h = createProcessQueueHarness();
    const consume = sinon.stub();
    h.setConsumeService({ consume } as unknown as ConsumeService);
    const message = buildMessageView();
    h.consumer.receiveMessage.onFirstCall().resolves([ message ]);
    h.consumer.receiveMessage.returns(never());

    h.pq.fetchMessageImmediately();
    await flushPromises();

    assert.strictEqual(h.pq.cachedMessagesCount(), 1);
    assert.strictEqual(h.pq.cachedMessageBytes(), message.body.length);
    sinon.assert.calledOnceWithExactly(consume, h.pq, [ message ]);
    sinon.assert.calledTwice(h.consumer.receiveMessage);
  });

  it('关闭边界回归：abort 后的接收成功不再缓存或消费', async () => {
    const h = createProcessQueueHarness();
    const consume = sinon.stub();
    h.setConsumeService({ consume } as unknown as ConsumeService);
    const held = deferred<ReturnType<typeof buildMessageView>[]>();
    h.consumer.receiveMessage.returns(held.promise);
    h.pq.fetchMessageImmediately();
    h.pq.abort();
    held.resolve([ buildMessageView() ]);
    await flushPromises();

    sinon.assert.notCalled(consume);
    assert.strictEqual(h.pq.cachedMessagesCount(), 0);
    assert.strictEqual(h.pq.isDrained(), true);
    assert.strictEqual(clock.countTimers(), 0);
  });

  it('drop 时的在途接收和缓存全部结算后才标记为 drained', async () => {
    const h = createProcessQueueHarness();
    const consume = sinon.stub();
    h.setConsumeService({ consume } as unknown as ConsumeService);
    const held = deferred<ReturnType<typeof buildMessageView>[]>();
    h.consumer.receiveMessage.returns(held.promise);
    h.pq.fetchMessageImmediately();
    h.pq.drop();
    assert.strictEqual(h.pq.isDrained(), false);
    const message = buildMessageView();
    held.resolve([ message ]);
    await flushPromises();
    assert.strictEqual(h.pq.isDrained(), false);
    sinon.assert.calledOnce(consume);
    h.pq.eraseMessage(message, ConsumeResult.SUCCESS);
    await flushPromises();
    assert.strictEqual(h.pq.isDrained(), true);
  });

  it('bounds the batch size by the remaining permits', async () => {
    const h = createProcessQueueHarness({ consumeConcurrentlyMax: 4 });
    h.consumer.receiveMessage.returns(never());
    h.pq.cacheMessages([ buildMessageView() ]);

    h.pq.receiveMessage();

    assert.strictEqual(h.consumer.wrapPushReceiveMessageRequest.firstCall.args[0], 3);
  });

  [
    { name: 'message count', options: { cacheMessageCount: 1 } },
    { name: 'message bytes', options: { cacheMessageBytes: 1 } },
  ].forEach(({ name, options }) => {
    it(`backs off while the cache is full by ${name}`, async () => {
      const h = createProcessQueueHarness(options);
      h.consumer.receiveMessage.returns(never());
      const message = buildMessageView();
      h.pq.cacheMessages([ message ]);

      h.pq.receiveMessage();
      await clock.tickAsync(2000);
      sinon.assert.notCalled(h.consumer.receiveMessage);

      h.pq.discardMessage(message);
      await flushPromises();
      await clock.tickAsync(1000);
      sinon.assert.calledOnce(h.consumer.receiveMessage);
    });
  });

  it('backs off while every permit is in use', async () => {
    const h = createProcessQueueHarness({ consumeConcurrentlyMax: 1 });
    h.consumer.receiveMessage.returns(never());
    const message = buildMessageView();
    h.pq.cacheMessages([ message ]);

    h.pq.receiveMessage();
    await clock.tickAsync(100);
    sinon.assert.notCalled(h.consumer.receiveMessage);

    h.pq.discardMessage(message);
    await flushPromises();
    await clock.tickAsync(20);
    sinon.assert.calledOnce(h.consumer.receiveMessage);
  });

  [
    { name: 'flow control', error: new TooManyRequestsException(Code.TOO_MANY_REQUESTS, 'throttled'), delay: 20 },
    { name: 'failure', error: new Error('network'), delay: 1000 },
  ].forEach(({ name, error, delay }) => {
    it(`retries a failed receive after the ${name} backoff`, async () => {
      const h = createProcessQueueHarness();
      h.consumer.receiveMessage.onFirstCall().rejects(error);
      h.consumer.receiveMessage.returns(never());

      h.pq.fetchMessageImmediately();
      await flushPromises();
      await clock.tickAsync(delay - 1);
      sinon.assert.calledOnce(h.consumer.receiveMessage);
      await clock.tickAsync(1);
      sinon.assert.calledTwice(h.consumer.receiveMessage);
    });
  });

  it('reuses the attemptId after a failure and generates a new one after a success', async () => {
    const h = createProcessQueueHarness();
    h.consumer.receiveMessage.onFirstCall().rejects(new Error('network'));
    h.consumer.receiveMessage.onSecondCall().resolves([]);
    h.consumer.receiveMessage.returns(never());

    h.pq.fetchMessageImmediately();
    await flushPromises();
    await clock.tickAsync(1000);
    await flushPromises();

    const attemptIds = h.consumer.wrapPushReceiveMessageRequest.getCalls().map(call => call.args[4]);
    assert.strictEqual(attemptIds.length, 3);
    assert.ok(attemptIds[0]);
    assert.strictEqual(attemptIds[1], attemptIds[0]);
    assert.notStrictEqual(attemptIds[2], attemptIds[1]);
  });

  ([ 'drop', 'abort' ] as const).forEach(stop => {
    it(`schedules no retry for a receive failing after ${stop}`, async () => {
      const h = createProcessQueueHarness();
      const inflight = deferred<never>();
      h.consumer.receiveMessage.onFirstCall().returns(inflight.promise);

      h.pq.fetchMessageImmediately();
      h.pq[stop]();
      inflight.reject(new Error('network'));
      await flushPromises();

      assert.strictEqual(clock.countTimers(), 0);
      h.pq.fetchMessageImmediately();
      h.pq.receiveMessage();
      sinon.assert.calledOnce(h.consumer.receiveMessage);
    });

    it(`ignores a retry timer that fires after ${stop}`, async () => {
      const h = createProcessQueueHarness();
      h.consumer.receiveMessage.onFirstCall().rejects(new Error('network'));

      h.pq.fetchMessageImmediately();
      await flushPromises();
      h.pq[stop]();
      await clock.tickAsync(1000);

      sinon.assert.calledOnce(h.consumer.receiveMessage);
    });
  });
});

describe('ProcessQueue settlement', () => {
  let clock: sinon.SinonFakeTimers;

  beforeEach(() => {
    clock = useFakeTimers();
  });

  afterEach(() => {
    clock.restore();
    sinon.restore();
  });

  it('acks a successfully consumed message and restores the cache and permit', async () => {
    const h = createProcessQueueHarness({ consumeConcurrentlyMax: 2 });
    const message = buildMessageView();
    h.pq.cacheMessages([ message ]);

    h.pq.eraseMessage(message, ConsumeResult.SUCCESS);
    await flushPromises();

    sinon.assert.calledOnce(h.consumer.ackMessageViaRpc);
    sinon.assert.notCalled(h.consumer.changeInvisibleDurationViaRpc);
    sinon.assert.calledOnce(h.consumer.incrementConsumptionOkQuantity);
    assert.strictEqual(h.pq.cachedMessagesCount(), 0);
    assert.strictEqual(h.pq.cachedMessageBytes(), 0);
    assert.strictEqual(h.pq.availablePermits(), 2);
  });

  it('nacks a failed message with the retry policy delay', async () => {
    const h = createProcessQueueHarness({ nextAttemptDelay: 5000 });
    const message = buildMessageView({ deliveryAttempt: 2 });
    h.pq.cacheMessages([ message ]);

    h.pq.eraseMessage(message, ConsumeResult.FAILURE);
    await flushPromises();

    sinon.assert.calledOnceWithExactly(h.retryPolicy.getNextAttemptDelay, 2);
    sinon.assert.calledOnceWithExactly(h.consumer.wrapChangeInvisibleDurationRequest, message, 5000);
    sinon.assert.calledOnce(h.consumer.incrementConsumptionErrorQuantity);
    sinon.assert.notCalled(h.consumer.ackMessageViaRpc);
    assert.strictEqual(h.pq.cachedMessagesCount(), 0);
  });

  ([ 'ack', 'nack', 'DLQ' ] as const).forEach(operation => {
    it(`结算日志回归：${operation} 非 OK 状态记录 code、requestId 和 message`, async () => {
      const h = createProcessQueueHarness();
      const response = setRequestId(ackResponse(Code.INTERNAL_SERVER_ERROR), 'settle-request');
      response.getStatus()!.setMessage('remote failure');
      const rpc = operation === 'ack' ? h.consumer.ackMessageViaRpc
        : operation === 'nack' ? h.consumer.changeInvisibleDurationViaRpc : h.consumer.forwardMessageToDeadLetterQueueViaRpc;
      rpc.onFirstCall().resolves(response);
      const message = buildMessageView();
      h.pq.cacheMessages([ message ]);
      if (operation === 'DLQ') h.pq.discardFifoMessage(message);
      else h.pq.eraseMessage(message, operation === 'ack' ? ConsumeResult.SUCCESS : ConsumeResult.FAILURE);
      await flushPromises();
      const logged = h.logger.error.getCalls().map(call => call.args);
      await clock.tickAsync(1000);

      assert.strictEqual(logged.length, 1);
      assert.strictEqual(logged[0][4], Code.INTERNAL_SERVER_ERROR);
      assert.strictEqual(logged[0][5], 'settle-request');
      assert.strictEqual(logged[0][6], 'remote failure');
      sinon.assert.calledTwice(rpc);
      assert.strictEqual(h.pq.cachedMessagesCount(), 0);
    });

    it(`关闭边界回归：abort 后 ${operation} 不发首轮 RPC 并释放缓存`, async () => {
      const h = createProcessQueueHarness();
      const message = buildMessageView();
      h.pq.cacheMessages([ message ]);
      h.pq.abort();
      if (operation === 'DLQ') h.pq.discardFifoMessage(message);
      else h.pq.eraseMessage(message, operation === 'ack' ? ConsumeResult.SUCCESS : ConsumeResult.FAILURE);
      await flushPromises();

      sinon.assert.notCalled(h.consumer.ackMessageViaRpc);
      sinon.assert.notCalled(h.consumer.changeInvisibleDurationViaRpc);
      sinon.assert.notCalled(h.consumer.forwardMessageToDeadLetterQueueViaRpc);
      assert.strictEqual(h.pq.cachedMessagesCount(), 0);
      assert.strictEqual(clock.countTimers(), 0);
    });
  });

  it('retries a failed ack at a fixed interval until it succeeds', async () => {
    const h = createProcessQueueHarness();
    h.consumer.ackMessageViaRpc.onFirstCall().rejects(new Error('network'));
    h.consumer.ackMessageViaRpc.onSecondCall().resolves(ackResponse(Code.INTERNAL_SERVER_ERROR));
    const message = buildMessageView();
    h.pq.cacheMessages([ message ]);

    h.pq.eraseMessage(message, ConsumeResult.SUCCESS);
    await flushPromises();
    sinon.assert.calledOnce(h.consumer.ackMessageViaRpc);
    await clock.tickAsync(1000);
    sinon.assert.calledTwice(h.consumer.ackMessageViaRpc);
    assert.strictEqual(h.pq.cachedMessagesCount(), 1);
    await clock.tickAsync(1000);
    await flushPromises();

    sinon.assert.calledThrice(h.consumer.ackMessageViaRpc);
    assert.strictEqual(h.pq.cachedMessagesCount(), 0);
  });

  it('retries a failed nack until it succeeds', async () => {
    const h = createProcessQueueHarness();
    h.consumer.changeInvisibleDurationViaRpc.onFirstCall()
      .resolves(changeInvisibleDurationResponse(Code.INTERNAL_SERVER_ERROR));
    const message = buildMessageView();
    h.pq.cacheMessages([ message ]);

    h.pq.eraseMessage(message, ConsumeResult.FAILURE);
    await flushPromises();
    assert.strictEqual(h.pq.cachedMessagesCount(), 1);
    await clock.tickAsync(1000);
    await flushPromises();

    sinon.assert.calledTwice(h.consumer.changeInvisibleDurationViaRpc);
    assert.strictEqual(h.pq.cachedMessagesCount(), 0);
  });

  it('gives up the ack on INVALID_RECEIPT_HANDLE', async () => {
    const h = createProcessQueueHarness();
    h.consumer.ackMessageViaRpc.resolves(ackResponse(Code.INVALID_RECEIPT_HANDLE));
    const message = buildMessageView();
    h.pq.cacheMessages([ message ]);

    h.pq.eraseMessage(message, ConsumeResult.SUCCESS);
    await flushPromises();
    await clock.tickAsync(5000);

    sinon.assert.calledOnce(h.consumer.ackMessageViaRpc);
    assert.strictEqual(h.pq.cachedMessagesCount(), 0);
  });

  it('retries forwarding to DLQ when the response has no status, as Java does', async () => {
    const h = createProcessQueueHarness({ consumeConcurrentlyMax: 1 });
    h.consumer.forwardMessageToDeadLetterQueueViaRpc.onFirstCall().resolves(new ForwardMessageToDeadLetterQueueResponse());
    const message = buildMessageView();
    h.pq.cacheMessages([ message ]);

    h.pq.discardFifoMessage(message);
    await flushPromises();
    sinon.assert.calledOnce(h.consumer.forwardMessageToDeadLetterQueueViaRpc);
    assert.strictEqual(h.pq.cachedMessagesCount(), 1);
    await clock.tickAsync(1000);

    sinon.assert.calledTwice(h.consumer.forwardMessageToDeadLetterQueueViaRpc);
    assert.strictEqual(h.pq.cachedMessagesCount(), 0);
    assert.strictEqual(h.pq.availablePermits(), 1);
  });

  it('releases the permit only once when a message is settled twice', async () => {
    const h = createProcessQueueHarness({ consumeConcurrentlyMax: 2 });
    const message = buildMessageView();
    h.pq.cacheMessages([ message ]);

    h.pq.eraseMessage(message, ConsumeResult.SUCCESS);
    h.pq.discardMessage(message);
    h.pq.discardFifoMessage(message);
    await flushPromises();

    assert.strictEqual(h.pq.availablePermits(), 2);
    assert.strictEqual(h.pq.cachedMessagesCount(), 0);
  });
});

describe('ProcessQueue consumer context', () => {
  afterEach(() => sinon.restore());

  it('converts a suspend result to FAILURE for a non-lite consumer', async () => {
    const h = createProcessQueueHarness({ nextAttemptDelay: 5000 });
    const message = buildMessageView();
    h.pq.cacheMessages([ message ]);

    h.pq.eraseMessage(message, ConsumeResultSuspend.of(1000));
    await flushPromises();

    sinon.assert.calledWithMatch(h.logger.warn, /Only LitePushConsumer support ConsumeResultSuspend/);
    sinon.assert.calledOnceWithExactly(h.consumer.wrapChangeInvisibleDurationRequest, message, 5000);
    sinon.assert.calledOnce(h.consumer.incrementConsumptionErrorQuantity);
  });

  it('suspends a lite message for the requested time', async () => {
    const h = createProcessQueueHarness({ liteConsumer: true, nextAttemptDelay: 5000 });
    const message = buildMessageView({ liteTopic: 'lite-a' });
    h.pq.cacheMessages([ message ]);

    await h.pq.eraseFifoMessage(message, ConsumeResultSuspend.of(1000));

    sinon.assert.notCalled(h.logger.warn);
    sinon.assert.calledOnceWithExactly(h.consumer.wrapChangeInvisibleDurationRequest, message, 1000);
    assert.strictEqual(h.pq.cachedMessagesCount(), 0);
  });

  it('keeps reading the logger and lite flag from the consumer for three-argument construction', async () => {
    const h = createProcessQueueHarness();
    const legacyConsumer = { ...h.consumer, logger: h.logger, clientId: 'client-legacy', isLiteConsumer: () => true };
    const pq = new ProcessQueue(legacyConsumer as any, buildRoute().messageQueues[0], new FilterExpression('*'));
    const message = buildMessageView({ liteTopic: 'lite-a' });
    pq.cacheMessages([ message ]);

    await pq.eraseFifoMessage(message, ConsumeResultSuspend.of(1000));

    sinon.assert.calledWithMatch(h.logger.info, /Suspend consumption/);
    sinon.assert.calledOnceWithExactly(h.consumer.wrapChangeInvisibleDurationRequest, message, 1000);
  });

  it('redelivers a FIFO message locally with a logger that has no debug method', async () => {
    const h = createProcessQueueHarness();
    const consumeMessage = sinon.stub().resolves(ConsumeResult.SUCCESS);
    h.setConsumeService({ consumeMessage } as unknown as ConsumeService);
    // debug is optional on ILogger, e.g. a custom logger passed to the consumer options.
    const logger = { info: sinon.stub(), warn: sinon.stub(), error: sinon.stub() };
    const pq = new ProcessQueue({ ...h.consumer, logger } as any, buildRoute().messageQueues[0],
      new FilterExpression('*'), { logger, clientId: 'client-a', liteConsumer: false });
    const message = buildMessageView();
    pq.cacheMessages([ message ]);

    await pq.eraseFifoMessage(message, ConsumeResult.FAILURE);

    sinon.assert.calledOnce(consumeMessage);
    sinon.assert.calledOnce(h.consumer.ackMessageViaRpc);
    assert.strictEqual(pq.cachedMessagesCount(), 0);
  });
});

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
import { Assignment as AssignmentPB, ClientType, Permission, Subscription } from '../../proto/apache/rocketmq/v2/definition_pb';
import {
  HeartbeatResponse,
  NotifyClientTerminationResponse,
  QueryAssignmentResponse,
  QueryRouteResponse,
  ReceiveMessageResponse,
} from '../../proto/apache/rocketmq/v2/service_pb';
import { ConsumeResult } from '../../src/consumer/ConsumeResult';
import type { ConsumeService } from '../../src/consumer/ConsumeService';
import { PushConsumer } from '../../src/consumer/PushConsumer';
import { createSilentLogger, deferred, FakeTelemetryStream, installFakeRpcClientManager } from '../support/fakes';
import { buildMessagePB, buildRoute, buildSettings, buildStatus } from '../support/protobuf';
import { createDuration } from '../../src/util';
import { ackResponse, buildMessageView, flushPromises, forwardToDLQResponse, useFakeTimers } from './processQueueHarness';

const route = buildRoute({ topic: 'topic-a', queuesPerBroker: 3 });

function assignmentResponse(queueIds: number[]) {
  const response = new QueryAssignmentResponse().setStatus(buildStatus());
  for (const id of queueIds) {
    response.addAssignments(new AssignmentPB().setMessageQueue(route.messageQueues[id].toProtobuf()));
  }
  return response;
}

const queueIdsOf = (consumer: PushConsumer) => consumer.processQueues().map(pq => pq.getMessageQueue().queueId);

describe('PushConsumer assignment scanning', () => {
  let clock: sinon.SinonFakeTimers;
  let consumer: PushConsumer;
  let rpc: ReturnType<typeof installFakeRpcClientManager>;
  let logger: ReturnType<typeof createSilentLogger>;
  let receiveGate: ReturnType<typeof deferred<ReceiveMessageResponse[]>>;
  let shutdown: boolean;
  let listener: sinon.SinonStub;

  beforeEach(() => {
    clock = useFakeTimers();
    logger = createSilentLogger();
    listener = sinon.stub().resolves(ConsumeResult.SUCCESS);
    consumer = new PushConsumer({
      endpoints: '127.0.0.1:8081',
      namespace: '',
      consumerGroup: 'group-a',
      subscriptions: new Map([[ 'topic-a', '*' ]]),
      messageListener: { consume: listener },
      logger,
    });
    rpc = installFakeRpcClientManager(consumer);
    rpc.queryRoute.resolves(new QueryRouteResponse().setStatus(buildStatus())
      .setMessageQueuesList(route.messageQueues.map(mq => mq.toProtobuf())));
    rpc.telemetry.callsFake(() => new FakeTelemetryStream().asGrpcStream());
    rpc.notifyClientTermination.resolves(new NotifyClientTerminationResponse().setStatus(buildStatus()));
    rpc.queryAssignment.resolves(assignmentResponse([ 0, 1 ]));
    // Receives stay in flight until the gate opens, so process queues never spin.
    receiveGate = deferred<ReceiveMessageResponse[]>();
    rpc.receiveMessage.callsFake(() => receiveGate.promise);
    sinon.stub(consumer, 'getConsumeService').returns({ consume: sinon.stub() } as unknown as ConsumeService);
    shutdown = false;
  });

  afterEach(async () => {
    if (!shutdown) {
      await shutdownConsumer();
    }
    clock.restore();
    sinon.restore();
  });

  async function shutdownConsumer() {
    shutdown = true;
    const done = consumer.shutdown();
    receiveGate.resolve([ new ReceiveMessageResponse().setStatus(buildStatus()) ]);
    await clock.tickAsync(2000);
    await done;
  }

  async function startScanning() {
    (consumer as any).startAssignmentScanning();
    await clock.tickAsync(1000);
    await flushPromises();
  }

  it('creates a receiving process queue for every assignment on the first scan', async () => {
    await startScanning();

    assert.deepStrictEqual(queueIdsOf(consumer), [ 0, 1 ]);
    sinon.assert.calledTwice(rpc.receiveMessage);
  });

  it('waits for the next scan when the first assignments are empty', async () => {
    rpc.queryAssignment.resolves(assignmentResponse([]));
    await startScanning();
    assert.deepStrictEqual(queueIdsOf(consumer), []);

    rpc.queryAssignment.resolves(assignmentResponse([ 2 ]));
    await clock.tickAsync(5000);
    assert.deepStrictEqual(queueIdsOf(consumer), [ 2 ]);
  });

  it('drops the process queue of a removed assignment and creates one for an added assignment', async () => {
    await startScanning();
    const [ kept ] = consumer.processQueues();

    rpc.queryAssignment.resolves(assignmentResponse([ 0, 2 ]));
    await clock.tickAsync(5000);

    assert.deepStrictEqual(queueIdsOf(consumer), [ 0, 2 ]);
    assert.strictEqual(consumer.processQueues()[0], kept);
  });

  it('drops every process queue when the remote assignments become empty', async () => {
    await startScanning();

    rpc.queryAssignment.resolves(assignmentResponse([]));
    await clock.tickAsync(5000);

    assert.deepStrictEqual(queueIdsOf(consumer), []);
    sinon.assert.calledWithMatch(logger.warn, /acquired empty assignments from remote, but existed assignments/);
  });

  it('recreates an expired process queue on the next scan', async () => {
    await startScanning();
    const before = consumer.processQueues();

    // Expiry needs (longPollingTimeout + requestTimeout) * 3 = 99s without a new receive.
    await clock.tickAsync(100_000);

    const after = consumer.processQueues();
    assert.deepStrictEqual(queueIdsOf(consumer), [ 0, 1 ]);
    assert.ok(after.every(pq => !before.includes(pq)));
    sinon.assert.callCount(rpc.receiveMessage, 4);
  });

  it('keeps the existing process queues when a scan fails', async () => {
    await startScanning();
    const before = consumer.processQueues();

    rpc.queryAssignment.rejects(new Error('query assignment failed'));
    await clock.tickAsync(5000);

    assert.deepStrictEqual(consumer.processQueues(), before);
    sinon.assert.calledWithMatch(logger.error, /Exception raised while scanning the assignments, topic=%s/);
  });

  it('分配查询轮转回归：真实扫描轮转可用 master 端点', async () => {
    const selectionRoute = buildRoute({ topic: 'topic-a', brokers: [
      { name: 'slave', id: 1, endpoints: '127.0.0.1:8082' },
      { name: 'none', endpoints: '127.0.0.1:8083' },
      { name: 'master-a', endpoints: '127.0.0.1:8084' },
      { name: 'master-b', endpoints: '127.0.0.1:8085' },
    ] });
    selectionRoute.messageQueues[1].permission = Permission.NONE;
    rpc.queryRoute.resolves(new QueryRouteResponse().setStatus(buildStatus())
      .setMessageQueuesList(selectionRoute.messageQueues.map(mq => mq.toProtobuf())));
    rpc.queryAssignment.resolves(assignmentResponse([]));
    await startScanning();
    await clock.tickAsync(15000);
    const targets = rpc.queryAssignment.getCalls().map(call => call.args[0].facade);
    assert.deepStrictEqual(new Set(targets), new Set([ '127.0.0.1:8084', '127.0.0.1:8085' ]));
  });

  ([ 'ackMessage', 'changeInvisibleDuration', 'forwardMessageToDeadLetterQueue' ] as const).forEach(operation => {
    it(`移出队列关闭回归：${operation} 重试随 shutdown 终止`, async () => {
      await startScanning();
      const pq = consumer.processQueues()[0];
      rpc[operation].rejects(new Error('connection unavailable'));
      const message = buildMessageView();
      pq.cacheMessages([ message ]);
      if (operation === 'forwardMessageToDeadLetterQueue') pq.discardFifoMessage(message);
      else pq.eraseMessage(message, operation === 'ackMessage' ? ConsumeResult.SUCCESS : ConsumeResult.FAILURE);
      rpc.queryAssignment.resolves(assignmentResponse([]));
      await clock.tickAsync(5000);
      assert.strictEqual(consumer.processQueues().length, 0);
      shutdown = true;
      const closing = consumer.shutdown();
      receiveGate.resolve([ new ReceiveMessageResponse().setStatus(buildStatus()) ]);
      await clock.tickAsync(35000);
      await closing;
      const calls = rpc[operation].callCount;
      await clock.tickAsync(5000);
      try {
        assert.strictEqual(rpc[operation].callCount, calls);
        assert.strictEqual(pq.cachedMessagesCount(), 0);
        assert.strictEqual(clock.countTimers(), 0);
      } finally {
        pq.abort();
        await clock.tickAsync(1000);
      }
    });
  });

  it('移出队列关闭回归：等待 dropped 队列正常结算后再关闭 RPC', async () => {
    await startScanning();
    const pq = consumer.processQueues()[0];
    const message = buildMessageView();
    pq.cacheMessages([ message ]);
    rpc.ackMessage.resolves(ackResponse());
    rpc.queryAssignment.resolves(assignmentResponse([]));
    await clock.tickAsync(5000);
    shutdown = true;
    const closing = consumer.shutdown();
    receiveGate.resolve([ new ReceiveMessageResponse().setStatus(buildStatus()) ]);
    await clock.tickAsync(1500);
    const closedBeforeSettlement = rpc.close.called;
    pq.eraseMessage(message, ConsumeResult.SUCCESS);
    await clock.tickAsync(2000);
    await closing;

    assert.strictEqual(closedBeforeSettlement, false);
    assert.strictEqual(pq.cachedMessagesCount(), 0);
    sinon.assert.calledOnce(rpc.ackMessage);
    sinon.assert.calledOnce(rpc.close);
  });

  it('移出队列关闭回归：drop 时缓存为空的在途 receive 仍由 shutdown 管理', async () => {
    await startScanning();
    const queues = consumer.processQueues();
    const service = consumer.getConsumeService();
    (service.consume as sinon.SinonStub).callsFake((pq, messages) => {
      for (const message of messages) pq.eraseMessage(message, ConsumeResult.SUCCESS);
    });
    rpc.ackMessage.rejects(new Error('connection unavailable'));
    rpc.queryAssignment.resolves(assignmentResponse([]));
    await clock.tickAsync(5000);
    assert.ok(queues.every(pq => pq.cachedMessagesCount() === 0));
    shutdown = true;
    const closing = consumer.shutdown();
    receiveGate.resolve([
      new ReceiveMessageResponse().setMessage(buildMessagePB()),
      new ReceiveMessageResponse().setStatus(buildStatus()),
    ]);
    await clock.tickAsync(35000);
    await closing;
    const calls = rpc.ackMessage.callCount;
    await clock.tickAsync(5000);
    try {
      assert.strictEqual(rpc.ackMessage.callCount, calls);
      assert.ok(queues.every(pq => pq.cachedMessagesCount() === 0));
      assert.strictEqual(clock.countTimers(), 0);
    } finally {
      queues.forEach(pq => pq.abort());
      await clock.tickAsync(1000);
    }
  });

  it('移出队列关闭回归：FIFO 本地重试等待被取消时不误投 DLQ', async () => {
    (consumer.getConsumeService as sinon.SinonStub).restore();
    listener.resolves(ConsumeResult.FAILURE);
    rpc.queryAssignment.resolves(assignmentResponse([ 0 ]));
    rpc.receiveMessage.onFirstCall().resolves([
      new ReceiveMessageResponse().setMessage(buildMessagePB()),
      new ReceiveMessageResponse().setStatus(buildStatus()),
    ]);
    rpc.forwardMessageToDeadLetterQueue.resolves(forwardToDLQResponse());
    rpc.heartbeat.resolves(new HeartbeatResponse().setStatus(buildStatus()));
    const startup = consumer.startup();
    await flushPromises();
    const settings = buildSettings({ clientType: ClientType.PUSH_CONSUMER,
      subscription: new Subscription().setFifo(true).setReceiveBatchSize(32) });
    settings.getBackoffPolicy()!.getExponentialBackoff()!
      .setInitial(createDuration(60000)).setMax(createDuration(60000));
    consumer.onSettingsCommand(consumer.getEndpoints(), settings);
    await startup;
    await clock.tickAsync(1000);
    const pq = consumer.processQueues()[0];
    assert.strictEqual(pq.cachedMessagesCount(), 1);
    rpc.queryAssignment.resolves(assignmentResponse([]));
    await clock.tickAsync(5000);
    shutdown = true;
    const closing = consumer.shutdown();
    receiveGate.resolve([ new ReceiveMessageResponse().setStatus(buildStatus()) ]);
    await clock.tickAsync(35000);
    await closing;
    try {
      sinon.assert.notCalled(rpc.forwardMessageToDeadLetterQueue);
      sinon.assert.calledOnce(listener);
      assert.strictEqual(pq.cachedMessagesCount(), 0);
      assert.strictEqual(clock.countTimers(), 0);
    } finally {
      pq.abort();
      await clock.tickAsync(1000);
    }
  });

  it('stops scanning and creates no process queue after shutdown', async () => {
    await startScanning();
    const scans = rpc.queryAssignment.callCount;

    await shutdownConsumer();
    await clock.tickAsync(10_000);

    assert.strictEqual(rpc.queryAssignment.callCount, scans);
    assert.deepStrictEqual(consumer.processQueues(), []);
    sinon.assert.calledTwice(rpc.receiveMessage);
  });
});

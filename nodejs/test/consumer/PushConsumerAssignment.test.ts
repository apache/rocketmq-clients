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
import { Assignment as AssignmentPB } from '../../proto/apache/rocketmq/v2/definition_pb';
import {
  NotifyClientTerminationResponse,
  QueryAssignmentResponse,
  QueryRouteResponse,
  ReceiveMessageResponse,
} from '../../proto/apache/rocketmq/v2/service_pb';
import { ConsumeResult } from '../../src/consumer/ConsumeResult';
import type { ConsumeService } from '../../src/consumer/ConsumeService';
import { PushConsumer } from '../../src/consumer/PushConsumer';
import { createSilentLogger, deferred, FakeTelemetryStream, installFakeRpcClientManager } from '../support/fakes';
import { buildRoute, buildStatus } from '../support/protobuf';
import { flushPromises, useFakeTimers } from './processQueueHarness';

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

  beforeEach(() => {
    clock = useFakeTimers();
    logger = createSilentLogger();
    consumer = new PushConsumer({
      endpoints: '127.0.0.1:8081',
      namespace: '',
      consumerGroup: 'group-a',
      subscriptions: new Map([[ 'topic-a', '*' ]]),
      messageListener: { consume: async () => ConsumeResult.SUCCESS },
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

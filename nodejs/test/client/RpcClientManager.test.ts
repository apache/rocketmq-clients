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
import { format } from 'node:util';
import * as sinon from 'sinon';
import { Metadata } from '@grpc/grpc-js';
import { HeartbeatRequest, HeartbeatResponse } from '../../proto/apache/rocketmq/v2/service_pb';
import type { BaseClient } from '../../src/client/BaseClient';
import { Producer } from '../../src/producer';
import { RpcClient } from '../../src/client/RpcClient';
import { RpcClientManager } from '../../src/client/RpcClientManager';
import { Endpoints } from '../../src/route';
import { createSilentLogger } from '../support/fakes';

const BROKER_A = new Endpoints('127.0.0.1:8081');
const BROKER_B = new Endpoints('127.0.0.1:8082');

function createManager() {
  const logger = createSilentLogger();
  const baseClient = {
    clientId: 'test-client-id',
    sslEnabled: false,
    getRequestMetadata: () => new Metadata(),
  } as unknown as BaseClient;
  return { manager: new RpcClientManager(baseClient, logger), logger };
}

describe('RpcClientManager', () => {
  let heartbeatTargets: RpcClient[];

  beforeEach(() => {
    heartbeatTargets = [];
    sinon.stub(RpcClient.prototype, 'heartbeat').callsFake(async function(this: RpcClient) {
      heartbeatTargets.push(this);
      return new HeartbeatResponse();
    });
    sinon.stub(RpcClient.prototype, 'close');
  });

  afterEach(() => sinon.restore());

  it('does not keep the process alive with its idle check timer', () => {
    const setIntervalSpy = sinon.spy(global, 'setInterval');
    const { manager } = createManager();
    try {
      sinon.assert.calledOnce(setIntervalSpy);
      const timer = setIntervalSpy.firstCall.returnValue as unknown as NodeJS.Timeout;
      assert.strictEqual(timer.hasRef(), false);
    } finally {
      manager.close();
    }
  });

  it('does not keep the process alive for a client that is constructed but never started', () => {
    const setIntervalSpy = sinon.spy(global, 'setInterval');
    const producer = new Producer({ endpoints: '127.0.0.1:8081', namespace: '', logger: createSilentLogger() });
    try {
      const timers = setIntervalSpy.returnValues as unknown as NodeJS.Timeout[];
      assert.ok(timers.length > 0);
      assert.ok(timers.every(timer => !timer.hasRef()), 'a never-started client must not hold a referenced interval');
    } finally {
      producer.getRpcClientManager().close();
    }
  });

  it('reuses one RpcClient per endpoints facade', async () => {
    const { manager } = createManager();
    await manager.heartbeat(BROKER_A, new HeartbeatRequest(), 3000);
    await manager.heartbeat(new Endpoints('127.0.0.1:8081'), new HeartbeatRequest(), 3000);
    await manager.heartbeat(BROKER_B, new HeartbeatRequest(), 3000);
    assert.strictEqual(heartbeatTargets[0], heartbeatTargets[1]);
    assert.notStrictEqual(heartbeatTargets[0], heartbeatTargets[2]);
    manager.close();
  });

  it('rebuilds the RpcClient after evict', async () => {
    const { manager } = createManager();
    await manager.heartbeat(BROKER_A, new HeartbeatRequest(), 3000);
    manager.evict(BROKER_A);
    sinon.assert.calledOnce(RpcClient.prototype.close as sinon.SinonStub);
    manager.evict(BROKER_A);
    sinon.assert.calledOnce(RpcClient.prototype.close as sinon.SinonStub);
    await manager.heartbeat(BROKER_A, new HeartbeatRequest(), 3000);
    assert.notStrictEqual(heartbeatTargets[0], heartbeatTargets[1]);
    manager.close();
  });

  it('rejects every call after close and tolerates a repeated close', async () => {
    const { manager } = createManager();
    await manager.heartbeat(BROKER_A, new HeartbeatRequest(), 3000);
    manager.close();
    manager.close();
    sinon.assert.calledOnce(RpcClient.prototype.close as sinon.SinonStub);
    await assert.rejects(manager.heartbeat(BROKER_A, new HeartbeatRequest(), 3000), /already closed/);
    assert.throws(() => manager.telemetry(BROKER_A, new Metadata()), /already closed/);
  });

  it('delegates every RPC to the RpcClient with fresh request metadata', async () => {
    const methods = [
      'queryRoute', 'sendMessage', 'queryAssignment', 'receiveMessage', 'ackMessage',
      'forwardMessageToDeadLetterQueue', 'pullMessage', 'updateOffset', 'getOffset', 'queryOffset',
      'endTransaction', 'changeInvisibleDuration', 'notifyClientTermination', 'recallMessage',
      'syncLiteSubscription',
    ] as const;
    const { manager } = createManager();
    for (const method of methods) {
      const response = { method };
      const stub = sinon.stub(RpcClient.prototype, method).resolves(response as never);
      const request = {};
      assert.strictEqual(await (manager as any)[method](BROKER_A, request, 3000), response);
      sinon.assert.calledOnce(stub);
      assert.strictEqual(stub.firstCall.args[0], request);
      assert.ok(stub.firstCall.args[1] instanceof Metadata);
      assert.strictEqual(stub.firstCall.args[2], 3000);
    }
    const stream = {};
    const telemetry = sinon.stub(RpcClient.prototype, 'telemetry').returns(stream as never);
    const metadata = new Metadata();
    assert.strictEqual(manager.telemetry(BROKER_A, metadata), stream);
    sinon.assert.calledOnceWithExactly(telemetry, metadata);
    manager.close();
  });

  it('closes clients idle beyond the threshold and logs the real idle duration', async () => {
    const clock = sinon.useFakeTimers();
    try {
      const { manager, logger } = createManager();
      await manager.heartbeat(BROKER_A, new HeartbeatRequest(), 3000);
      clock.tick(30 * 60000);
      sinon.assert.notCalled(RpcClient.prototype.close as sinon.SinonStub);
      clock.tick(60000);
      sinon.assert.calledOnce(RpcClient.prototype.close as sinon.SinonStub);
      const line = format(...logger.info.lastCall.args as [ string, ...unknown[] ]);
      assert.match(line, /endpoints=127\.0\.0\.1:8081, idleDuration=\d+ms, maxIdleDuration=1800000ms, clientId=test-client-id$/);
      await manager.heartbeat(BROKER_A, new HeartbeatRequest(), 3000);
      assert.notStrictEqual(heartbeatTargets[0], heartbeatTargets[1], 'a reclaimed client is rebuilt on next use');
      manager.close();
    } finally {
      clock.restore();
    }
  });
});

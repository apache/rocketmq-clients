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

import { Duplex } from 'node:stream';
import type { ClientDuplexStream } from '@grpc/grpc-js';
import * as sinon from 'sinon';
import type { BaseClient } from '../../src/client/BaseClient';
import type { ILogger } from '../../src/client/Logger';
import { RpcClientManager } from '../../src/client/RpcClientManager';
import { TelemetryCommand } from '../../proto/apache/rocketmq/v2/service_pb';

export function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

export function createSilentLogger() {
  return {
    info: sinon.stub<unknown[], void>(),
    warn: sinon.stub<unknown[], void>(),
    error: sinon.stub<unknown[], void>(),
    debug: sinon.stub<unknown[], void>(),
    close: sinon.stub<unknown[], void>(),
  } satisfies ILogger;
}

// Skip construction to avoid real channels and idle timers; configure responses with resolves/rejects/callsFake.
export function createFakeRpcClientManager(): sinon.SinonStubbedInstance<RpcClientManager> {
  const fake = sinon.createStubInstance(RpcClientManager);
  const asyncMethods = [
    'queryRoute', 'heartbeat', 'sendMessage', 'queryAssignment', 'receiveMessage',
    'ackMessage', 'forwardMessageToDeadLetterQueue', 'pullMessage', 'updateOffset',
    'getOffset', 'queryOffset', 'endTransaction', 'changeInvisibleDuration',
    'notifyClientTermination', 'recallMessage', 'syncLiteSubscription',
  ] as const;
  for (const method of asyncMethods) {
    fake[method].rejects(new Error(`未配置 FakeRpcClientManager.${method}`));
  }
  fake.telemetry.throws(new Error('未配置 FakeRpcClientManager.telemetry'));
  return fake;
}

// Inject only before startup; close the original manager first to avoid retaining real timers.
export function installFakeRpcClientManager(client: BaseClient, fake = createFakeRpcClientManager()) {
  if (client.isRunning()) {
    throw new Error('只能向尚未启动的客户端注入 RPC 替身');
  }
  client.getRpcClientManager().close();
  Object.defineProperty(client, 'rpcClientManager', { value: fake, configurable: true, writable: true });
  return fake;
}

export class FakeTelemetryStream extends Duplex {
  readonly writes: TelemetryCommand[] = [];

  constructor() {
    super({ objectMode: true });
  }

  _read(): void {
    // Tests explicitly send server responses.
  }

  _write(command: TelemetryCommand, _encoding: BufferEncoding, callback: (error?: Error | null) => void): void {
    this.writes.push(command);
    callback();
  }

  receive(command: TelemetryCommand): void {
    this.emit('data', command);
  }

  fail(error: Error): void {
    this.emit('error', error);
  }

  finishFromServer(): void {
    this.emit('end');
  }

  cancel(): void {
    this.destroy();
  }

  // Simulate only the telemetry stream interface, without real gRPC metadata/status.
  asGrpcStream(): ClientDuplexStream<TelemetryCommand, TelemetryCommand> {
    return this as unknown as ClientDuplexStream<TelemetryCommand, TelemetryCommand>;
  }
}
